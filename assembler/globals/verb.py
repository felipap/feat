
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import functools
import pyjson5
from pandas.io.json import json_normalize

def fancy_apply(df, function, **kwargs):
  count = 0
  @functools.wraps(function)
  def wrapped(row):
    nonlocal count
    count += 1
    if count % 2000 == 0:
      print("\r<%s> count: " % function.__name__, count, end="")
    return function(row)
  start = timer()
  result = df.apply(wrapped, **kwargs)
  print("\nFunction took: %s seconds" % round(timer() - start, 2))
  return result

def JSON_GET(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)
  
  def parse_json(row):
    if row[name] == '[]':
      return None
    return pyjson5.loads(row[name])
  
  # HACK it's wrong that we even have to do this!
  cmonths = df['CMONTH(date)'].unique()
  df.drop('CMONTH(date)', axis=1, inplace=True)
  df.drop_duplicates(inplace=True)

  # For some reason Verb's JSON has capitalized constants.
  # TODO: fix this at earlier stage.
  df[name] = df[name].str.replace(': True', ': true')
  df[name] = df[name].str.replace(': None', ': null')
  df[name] = df[name].str.replace(': False', ': false')

  df['__parsed__'] = fancy_apply(df, parse_json, axis=1)
  
  def foo(row):
    if not row['__parsed__']:
      return ''
    # FIXME stranger danger!
    return eval("row['__parsed__']%s" % field)
  
  try:
    df[name] = fancy_apply(df, foo, axis=1)
  except Exception as e:
    # import traceback; traceback.print_exc()
    print("Error using .apply()", e)
    raise Exception()

  # HACK add back CMONTH(date) that we dropped because otherwise error will throw!
  cmonths = pd.DataFrame({'CMONTH(date)': cmonths })
  cmonths['__mergekey__'] = 1
  df['__mergekey__'] = 1
  df = pd.merge(df, cmonths, on=['__mergekey__'])
 
  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result

