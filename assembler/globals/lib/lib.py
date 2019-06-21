
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
from functools import wraps
import pyjson5
from pandas.io.json import json_normalize

from ...lib.workarounds import stringify_unhashables

def fancy_apply(df, function, **kwargs):
  total = df.shape[0]
  
  count = 0
  @wraps(function)
  def wrapped(row):
    nonlocal count
    count += 1
    if count % 6000 == 0:
      print("\r<%s> done: %d%%" % (function.__name__, 100*count/total), end="")
    return function(row)

  start = timer()
  try:
    result = df.apply(wrapped, **kwargs)
  except Exception as e:
    import traceback; traceback.print_exc()
    print("\nError using .apply(). row=%s" % count, e)
    raise Exception(e)
  
  print("\r<%s> done: %d%%" % (function.__name__, 100))
  print("\nFunction took: %s seconds" % round(timer() - start, 2))
  return result

# How to test this:
# df.loc[(df.customer=='5b777d1e50c3c0094d2b1d5c')&(df['CMONTH(date)']==592),'customer.email'] = '1234'
# df = df[df.customer=='5b777d1e50c3c0094d2b1d5c']
# df = counts[counts.customer=='5b777d1e50c3c0094d2b1d5c']
# df.loc[16,'customer.email'] = 'hahah' # 5b777d1e50c3c0094d2b1d5c
# df.loc[(df.customer=='5b777d1e50c3c0094d2b1d5c')&(df[datefield]==592)] = 'anotheremail@gmail.com'

# REVIEW add as method?
def can_collapse_date(child, datefield):
  if not datefield in child.pivots:
    raise Exception()
  minusdate = list(set([*child.pivots]) - set([datefield]))

  df = child.df.drop(datefield, axis=1)
  # dropna=False so that if field is X for certain cmonths and None for others,
  # collapse will not be allowed.
  # counts = df.groupby(minusdate).nunique(dropna=False)
  
  counts = stringify_unhashables(df).groupby(minusdate).nunique(dropna=True)
  counts = counts[[child.name]]
  counts.reset_index(inplace=True)

  offending = counts[(counts[child.name] > 1)]
  if offending.empty:
    return True
  print("Can't collapse %s." % child.name)

  return False

def uncollapse_date(name, df, child, datefield, expand_to_original=True):
  if not datefield in child.pivots:
    raise Exception()
  minusdate = list(set([*child.pivots]) - set([datefield]))
  df = df[~df[name].isna()]
  child_pivot_cols = child.df[~child.df[child.name].isna()][child.pivots]
  # child_pivot_cols = child.df[child.pivots]
  merged = pd.merge(child_pivot_cols, df, on=minusdate)

  if expand_to_original:
    return pd.merge(child.df[list(child.pivots)], merged, on=list(child.pivots), how='left')
  return merged

def ensure_same_nrow(fn):
  @wraps(fn)
  def wrapped(ctx, name, arguments, *args, **kwargs):
    child = arguments[0]
    result = fn(ctx, name, arguments, *args, **kwargs)
    assert child.df.shape[0] == result.df.shape[0]
    return result
  return wrapped