
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import functools
import pyjson5
from pandas.io.json import json_normalize
from ..lib.cmonth import date_to_cmonth

from .lib import fancy_apply

def JSON_GET(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)

  # For some reason Verb's JSON has capitalized constants.
  # TODO: fix this at earlier stage.
  df[name] = df[name].str.replace(': True', ': true')
  df[name] = df[name].str.replace(': None', ': null')
  df[name] = df[name].str.replace(': False', ': false')

  
  def parse_json(row):
    if not row[name] or row[name] == '[]':
      return None
    return pyjson5.loads(row[name])
  
  # HACK it's wrong that we even have to do this!
  cmonths = None
  if 'CMONTH(date)' in df.columns:
    cmonths = df['CMONTH(date)'].unique()
    df.drop('CMONTH(date)', axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

  df['__parsed__'] = fancy_apply(df, parse_json, axis=1)
  
  def get_field(row):
    if not row['__parsed__']:
      return None
    try:
      # FIXME stranger danger!?
      return eval("row['__parsed__']%s" % field)
    except:
      return None
  df[name] = fancy_apply(df, get_field, axis=1)

  # HACK add back CMONTH(date) that we dropped because otherwise error will throw!
  if cmonths is not None:
    cmonths = pd.DataFrame({'CMONTH(date)': cmonths })
    cmonths['__mergekey__'] = 1
    df['__mergekey__'] = 1
    df = pd.merge(df, cmonths, on=['__mergekey__'])
 
  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result


def JSON_GET_FLEXPLAN(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)

  # For some reason Verb's JSON has capitalized constants.
  # TODO: fix this at earlier stage.
  df[name] = df[name].str.replace(': True', ': true')
  df[name] = df[name].str.replace(': None', ': null')
  df[name] = df[name].str.replace(': False', ': false')
  
  # HACK it's wrong that we even have to do this!
  cmonths = None
  if not 'CMONTH(date)' in df.columns:
    raise Exception()

  # Contract
  cmonths = df['CMONTH(date)'].unique()
  df.drop('CMONTH(date)', axis=1, inplace=True)
  df.drop_duplicates(inplace=True)

  def parse_json(row):
    if not row[name] or row[name] == '[]':
      return None
    return pyjson5.loads(row[name])
  df['__parsed__'] = fancy_apply(df, parse_json, axis=1)
  
  # Expand back
  cmonths = pd.DataFrame({'CMONTH(date)': cmonths })
  cmonths['__mergekey__'] = 1
  df['__mergekey__'] = 1
  df = pd.merge(df, cmonths, on=['__mergekey__'])
  cmonths = None

  def get_field(row):
    if not row['__parsed__']:
      return None
    plan = row['__parsed__'][0]
    started = datetime.strptime(plan['started'], '%Y-%m-%dT%H:%M:%S.%fZ')
    started_cmonth = date_to_cmonth(started)

    resumed_cmonth = paused_cmonth = None
    if 'pausedOn' in plan:
      paused = datetime.strptime(plan['pausedOn'], '%Y-%m-%dT%H:%M:%S.%fZ')
      paused_cmonth = date_to_cmonth(paused)
    
    if 'resumedOn' in plan:
      resumed = datetime.strptime(plan['resumedOn'], '%Y-%m-%dT%H:%M:%S.%fZ')
      resumed_cmonth = date_to_cmonth(resumed)
    
    # print(row['CMONTH(date)'], started_cmonth)
    if row['CMONTH(date)'] < started_cmonth:
      return None
    
    current_value = eval("row['__parsed__']%s" % field)
    # return current_value
    
    if paused_cmonth:
      if not resumed_cmonth:
        if row['CMONTH(date)'] >= paused_cmonth:
          return None
        else:
          return current_value
      else:
        if paused_cmonth < resumed_cmonth:
          if row['CMONTH(date)'] < paused_cmonth:
            return current_value
          elif row['CMONTH(date)'] <= resumed_cmonth:
            return current_value
          else:
            return None
        else:
          # QUESTION it's paused now then?
          return False
      
      #   if resumed_cmonth:
      #     print("resumed", started_cmonth, paused_cmonth, resumed_cmonth)
      #     if row['CMONTH(date)'] >= resumed_cmonth:
      #       return current_value
    else:
      return current_value
    # try:
    #   # FIXME stranger danger!?
    # except:
    #   return None
  df[name] = fancy_apply(df, get_field, axis=1)
 
  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result

