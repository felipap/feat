
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
from .lib import fancy_apply, can_collapse_date, uncollapse_date

# Add tests to make sure sizes are changed?

def JSON_GET(ctx, name, args):
  child = args[0]
  field = args[1][1:-1]

  df = child.get_stripped()
  
  df.rename(columns={ child.name: name }, inplace=True)
  collapse = can_collapse_date(child, 'CMONTH(date)')
  if collapse:
    df = df.drop('CMONTH(date)', axis=1).drop_duplicates()

  def parse_json(row):
    if not row[name] or pd.isnull(row[name]) or row[name] == '[]':
      return None
    return pyjson5.loads(row[name])

  df['__parsed__'] = fancy_apply(df, parse_json, axis=1)
  
  def get_field(row):
    if not row['__parsed__']:
      return None
    try:
      # FIXME stranger danger!?
      return eval("row['__parsed__']%s" % field)
    except Exception as e:
      # import traceback; traceback.print_exc()
      print("Failed to get from row", row['__parsed__'], field)
      return None
  df[name] = fancy_apply(df, get_field, axis=1)
 
  if collapse:
    df = uncollapse_date(name, df, child, 'CMONTH(date)')

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df, fillnan=0)
  return result


def JSON_GET_FLEXPLAN(ctx, name, args):
  raise Exception()


# if paused_cmonth:
#   if not resumed_cmonth:
#     if row['CMONTH(date)'] >= paused_cmonth:
#       return None
#     else:
#       return current_value
#   else:
#     if paused_cmonth < resumed_cmonth:
#       if row['CMONTH(date)'] < paused_cmonth:
#         return current_value
#       elif row['CMONTH(date)'] <= resumed_cmonth:
#         return current_value
#       else:
#         return None
#     else:
#       # QUESTION it's paused now then?

#       return False
# else:
#   return current_value
