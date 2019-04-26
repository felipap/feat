
from datetime import datetime
import time

import numpy as np
import pandas as pd

fns = {}
def register_function(name, keyword, call, **kwargs):
  fns[keyword] = dict(
    name=name,
    keyword=keyword,
    call=call,
    takes_pivots=kwargs.get('takes_pivots', False),
    num_args=kwargs.get('num_args', 1),
  )


def getFunction(name):
  return fns.get(name)

#

def call_fwd(ctx, name, args, pivots):
  lag = args[1]
  child = args[0]

  to_shift = child.get_stripped()
  to_shift.rename(columns={ child.name: name }, inplace=True)
  to_shift[ctx.timeCol] += lag

  # If pivots isn't supplied, use child's pivots instead.
  if pivots is None:
    pivots = child.pivots
  result = ctx.create_subframe(name, pivots)
  result.fillData(to_shift)
  return result

register_function('Forward', 'FWD', call_fwd, num_args=2, takes_pivots=True)

#

def call_get(ctx, name, args, pivots):
  child = args[0]
  assert name == 'GET(%s)' % child.name, '%s != %s' % (name, child.name)
  if pivots is not None: # If pivot isn't supplied, use child's pivots instead.
    child.pivots = pivots
  child.df.rename(columns={child.name: name}, inplace=True)
  child.name = name
  return child

register_function('Get', 'GET', call_get, num_args=1, takes_pivots=True)

#

def call_meandiff(ctx, name, args, pivot):
  child = args[0]

  # print("groupby is", groupby)
  groupby = child.getPivots()
  groupbyMinusTime = list(groupby-{ctx.timeCol})

  df = child.get_stripped()
  childMean = df.groupby(groupbyMinusTime).agg({ child.name: ['mean'] })

  # Dynamically execute Mean() here to do this work
  # ctx.df = pd.merge(ctx.df, childMean, on=groupbyMinusTime, how='left')
  childMean.columns = ['averaged']
  childMean.reset_index(inplace=True)

  grouped = pd.merge(ctx.df, \
    childMean, \
    on=groupbyMinusTime, \
    how='left', \
    suffixes=(False, False))

  # grouped.drop_duplicates(child['groupby'], inplace=True)

  assert child.name in grouped.columns

  grouped[name] = (grouped[child.name] - grouped['averaged']) / grouped['averaged']
  # display(grouped)
  # display('mean', grouped)

  result = ctx.create_subframe(name, groupby)
  result.fillData(grouped)

  # agg.columns = [name]
  # agg[name] = agg[name].astype(np.float64)
  # grouped = grouped[list(set([name, ctx.timeCol])|set(groupby))]
  #
  # agg.reset_index(inplace=True)
  return result

register_function('MeanDiff', 'MEAN_DIFF', call_meandiff, num_args=1, takes_pivots=True)

#

def call_mean(ctx, name, args, pivots):
  child = args[0]
  result = ctx.create_subframe(name, pivots)
  agg = ctx.df.groupby(pivots).agg({ child.name: ['mean'] })
  agg.columns = [name]
  agg[name] = agg[name].astype(np.float64)
  agg.reset_index(inplace=True)
  result.fillData(agg)
  return result

register_function('Mean', 'MEAN', call_mean, num_args=1, takes_pivots=True)

#

def call_cmonth(ctx, name, args):
  child = args[0]

  assert 'date' in child.get_stripped().columns
  assert child.get_stripped().date.dtype == np.dtype('datetime64[ns]')

  def apply(row):
    parsed = row[child.name] # datetime.strptime(row['date'], '%Y-%m-%d')
    return int((parsed.year - 2000)*12+parsed.month)
  df = child.get_stripped().copy()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.getPivots())
  result.fillData(df)
  return result

register_function('CMonth', 'CMONTH', call_cmonth, num_args=1)

#

def call_sum(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['sum'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  agg[name] = agg[name].astype(np.float64) # REVIEW type cast
  result = ctx.create_subframe(name, pivots)
  result.fillData(agg)
  return result

register_function('Sum', 'SUM', call_sum, num_args=1, takes_pivots=True)
