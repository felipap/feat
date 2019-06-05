
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import pyjson5
from pandas.io.json import json_normalize

from .lib import fancy_apply

def EMAIL_DOMAIN(ctx, name, args):
  child = args[0]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)
  
  cmonths = df['CMONTH(date)'].unique()
  df.drop('CMONTH(date)', axis=1, inplace=True)
  df.drop_duplicates(inplace=True)
  
  def get_domain(row):
    if row[name] and '@' in row[name]:
      return row[name].split('@')[1]
    return None
  df[name] = fancy_apply(df, get_domain, axis=1)
 
  if cmonths is not None:
    cmonths = pd.DataFrame({'CMONTH(date)': cmonths })
    cmonths['__mergekey__'] = 1
    df['__mergekey__'] = 1
    df = pd.merge(df, cmonths, on=['__mergekey__'])

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result


def DOMAIN_EXT(ctx, name, args):
  child = args[0]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)
  
  cmonths = None
  if 'CMONTH(date)' in df.columns:
    cmonths = df['CMONTH(date)'].unique()
    df.drop('CMONTH(date)', axis=1, inplace=True)
    df.drop_duplicates(inplace=True)
  
  def get_domain_ext(row):
    if row[name] and '.' in row[name]:
      return '.'.join(row[name].split('.')[1:])
    return None
  df[name] = fancy_apply(df, get_domain_ext, axis=1)
 
  if cmonths is not None:
    cmonths = pd.DataFrame({'CMONTH(date)': cmonths })
    cmonths['__mergekey__'] = 1
    df['__mergekey__'] = 1
    df = pd.merge(df, cmonths, on=['__mergekey__'])

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result
  
