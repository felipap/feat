
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import pyjson5
from pandas.io.json import json_normalize

from .lib.lib import fancy_apply, can_collapse_date, uncollapse_date, assert_constant_nrows

@assert_constant_nrows
def EMAIL_DOMAIN(ctx, name, args):
  child = args[0]

  df = child.get_stripped()

  collapse = can_collapse_date(child, 'CMONTH(date)')
  if collapse:
    df = df.drop('CMONTH(date)', axis=1).drop_duplicates()

  df.rename(columns={ child.name: name }, inplace=True)

  def get_domain(row):
    if row[name] and pd.notna(row[name]) and '@' in row[name]:
      return row[name].split('@')[1].lower()
    return None
  df[name] = fancy_apply(df, get_domain, axis=1)

  if collapse:
    df = uncollapse_date(name, df, child, 'CMONTH(date)')

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result

@assert_constant_nrows
def DOMAIN_EXT(ctx, name, args):
  child = args[0]

  df = child.get_stripped()

  collapsed = False
  if can_collapse_date(child, 'CMONTH(date)'):
    collapsed = True
    df = df.drop('CMONTH(date)', axis=1).drop_duplicates()
  df.rename(columns={ child.name: name }, inplace=True)

  def get_domain_ext(row):
    if row[name] and pd.notnull(row[name]) and '.' in row[name]:
      return '.'.join(row[name].split('.')[1:])
    return None
  df[name] = fancy_apply(df, get_domain_ext, axis=1)
 
  if collapsed:
    df = uncollapse_date(name, df, child, 'CMONTH(date)')

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result
  
