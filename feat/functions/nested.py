
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import functools
import pyjson5
from pandas.io.json import json_normalize

from ..lib.workarounds import drop_hashable_duplicates
from ..lib.tblock import date_to_cmonth
from .lib.lib import fancy_apply, can_collapse_date, uncollapse_date
from .lib.per_value import per_value

# Add tests to make sure sizes are changed?


def DICT_GET(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  
  df.rename(columns={ child.name: name }, inplace=True)
  collapsable = can_collapse_date(child, 'CMONTH(date)')
  if collapsable:
    df = drop_hashable_duplicates(df.drop('CMONTH(date)', axis=1))
  
  def get_field(row):
    if not row[name] or pd.isnull(row[name]):
      return np.nan
    try:
      # FIXME stranger danger!?
      return eval("row[name]%s" % field)
    except Exception as e:
      # import traceback; traceback.print_exc()
      print("Failed to get from row", row[name], field, e)
      return np.nan
  df[name] = fancy_apply(df, get_field, axis=1)
 
  if collapsable:
    df = uncollapse_date(name, df, child, 'CMONTH(date)')

  result = ctx.table.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result


def JSON_GET(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  
  df.rename(columns={ child.name: name }, inplace=True)
  collapsable = 'CMONTH(date)' in df.columns and can_collapse_date(child, 'CMONTH(date)')
  if collapsable:
    df = drop_hashable_duplicates(df.drop('CMONTH(date)', axis=1))

  def parse_json(row):
    if not row[name] or pd.isnull(row[name]) or row[name] == '[]':
      return None
    val = row[name].replace(': True', ': true').replace(': False', ': false').replace(': None', ': null')
    return pyjson5.loads(val)

  df['__parsed__'] = fancy_apply(df, parse_json, axis=1)
  
  failed_count = 0
  def get_field(row):
    nonlocal failed_count
    if not row['__parsed__']:
      return None
    try:
      # FIXME stranger danger!?
      result = eval("row['__parsed__']%s" % field)
      return result
    except Exception as e:
      failed_count += 1
      # import traceback; traceback.print_exc()
      # print("Failed to get from row", row['__parsed__'], field)
      return None
  df[name] = fancy_apply(df, get_field, axis=1)

  print("JSON_GET failed %d times (%d%%)" % (failed_count, failed_count/df[name].shape[0]*100))
 
  if collapsable:
    df = uncollapse_date(name, df, child, 'CMONTH(date)')

  result = ctx.table.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result

def get_json(value, args):
  pass

functions = {
  'GET_JSON': per_value(get_json, fillna=-1, dtype=int, num_args=2),
  'JSON_GET': dict(call=JSON_GET, num_args=2),
  'DICT_GET': dict(call=DICT_GET, num_args=2),
}