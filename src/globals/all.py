
from datetime import datetime
from dateutil import relativedelta
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
  time_col = args[2].name
  child = args[0]

  to_shift = child.get_stripped()
  to_shift.rename(columns={ child.name: name }, inplace=True)

  to_shift[time_col] += lag

  # to_shift[time_col] = to_shift[time_col].astype(np.int64)

  # If pivots isn't supplied, use child's pivots instead.
  if pivots is None:
    pivots = child.pivots
  result = ctx.create_subframe(name, pivots)
  result.fill_data(to_shift)
  return result

register_function('Forward', 'FWD', call_fwd, num_args=3, takes_pivots=True)

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

def call_meandiff(ctx, name, args, pivots):
  child = args[0]
  time_col = args[1].name

  # print("groupby is", groupby)
  if pivots:
    print("pivots is", pivots)
    groupby = pivots
    for pivot in pivots:
      assert pivot in child.get_pivots()
  else:
    groupby = child.get_pivots()

  groupbyMinusTime = list(set(groupby)-{time_col})

  df = child.get_stripped().copy()
  if groupbyMinusTime:
    childMean = df.groupby(groupbyMinusTime).agg({ child.name: ['mean'] })
    # IDEA dynamically execute Mean() here to do this work?
    childMean.columns = ['__averaged__']
    childMean.reset_index(inplace=True)

    grouped = pd.merge(ctx.df, \
      childMean, \
      on=groupbyMinusTime, \
      how='left', \
      suffixes=(False, False))
  else:
    # When groupbyMinusTime is empty, that means the average we want is over the
    # entire dataframe. (df.groupby just doesn't support an empty list).
    grouped = ctx.df.copy()
    grouped['__averaged__'] = grouped[child.name].mean()

  # grouped.drop_duplicates(child['groupby'], inplace=True)

  assert child.name in grouped.columns

  grouped[name] = (grouped[child.name] - grouped['__averaged__']) / grouped['__averaged__']

  result = ctx.create_subframe(name, groupby)
  result.fill_data(grouped)

  # agg.columns = [name]
  # agg[name] = agg[name].astype(np.float64)
  # grouped = grouped[list(set([name, ctx.timeCol])|set(groupby))]
  #
  # agg.reset_index(inplace=True)
  return result

register_function('MeanDiff', 'MEAN_DIFF', call_meandiff, num_args=2, takes_pivots=True)

#

def call_mean(ctx, name, args, pivots):
  child = args[0]
  result = ctx.create_subframe(name, pivots)
  agg = ctx.df.groupby(pivots).agg({ child.name: ['mean'] })
  agg.columns = [name]
  agg[name] = agg[name].astype(np.float64)
  agg.reset_index(inplace=True)
  result.fill_data(agg)
  return result

register_function('Mean', 'MEAN', call_mean, num_args=1, takes_pivots=True)

#

def call_cmonth(ctx, name, args):
  child = args[0]

  # print('date is', child.get_stripped().columns, name, child.name)

  assert child.name.endswith('date') # in child.get_stripped().columns
  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    # print("child.name", child.name, row[child.name])
    # return datetime.strptime(row[child.name], '%Y-%m-%dT%H:%M:%S.%f')
    value = row[child.name] # datetime.strptime(row['date'], '%Y-%m-%d')
    return int((value.year - 2000)*12+value.month)
  df = child.get_stripped().copy()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CMonth', 'CMONTH', call_cmonth, num_args=1)

#

def call_cmsince(ctx, name, args):
  child = args[0]

  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    if pd.isnull(row[child.name]):
      return 1000
    r = relativedelta.relativedelta(datetime.now(), row[child.name])
    return r.months * (r.years+1)

  df = child.get_stripped().copy()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CMonthSince', 'CMONTHSINCE', call_cmsince, num_args=1)

#

def call_nunique(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['nunique'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  # display(agg)
  agg[name] = agg[name].astype(np.int64) # REVIEW type cast
  result = ctx.create_subframe(name, pivots)
  result.fill_data(agg)
  return result

register_function('Nunique', 'NUNIQUE', call_nunique, num_args=1, takes_pivots=True)

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
  result.fill_data(agg)
  return result

register_function('Sum', 'SUM', call_sum, num_args=1, takes_pivots=True)
