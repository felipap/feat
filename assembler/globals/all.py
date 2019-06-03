
from datetime import datetime, timedelta
from dateutil import relativedelta
import time

from ..lib.gen_cartesian import gen_cartesian

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

def call_greaterthan(ctx, name, args):
  child = args[0]
  arg = args[1]

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)
  # display(df[name])
  df[name] = (df[name]>int(arg)).astype(np.int64)
  # display(df[name])

  result = ctx.create_subframe(name, child.pivots)
  result.fill_data(df)
  return result

register_function('GreaterThan', 'GREATERTHAN', call_greaterthan, num_args=2)

#

def call_fwd(ctx, name, args, pivots):
  lag = args[1]
  time_col = args[2].name
  child = args[0]

  shifted = child.get_stripped()
  shifted.rename(columns={ child.name: name }, inplace=True)

  shifted[time_col] += lag

  # shifted[time_col] = shifted[time_col].astype(np.int64)

  # If pivots isn't supplied, use child's pivots instead.
  if pivots is None:
    pivots = child.pivots
  result = ctx.create_subframe(name, pivots)
  result.fill_data(shifted)
  return result

register_function('Forward', 'FWD', call_fwd, num_args=3, takes_pivots=True)

#f

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

  df = child.get_stripped()
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

  # assert child.name.endswith('date') # in child.get_stripped().columns
  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    # print("child.name", child.name, row[child.name])
    # return datetime.strptime(row[child.name], '%Y-%m-%dT%H:%M:%S.%f')
    value = row[child.name] # datetime.strptime(row['date'], '%Y-%m-%d')
    return int((value.year - 1970)*12+value.month)
  df = child.get_stripped()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CMonth', 'CMONTH', call_cmonth, num_args=1)

#

def call_cdsince(ctx, name, args):
  child = args[0]

  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    if pd.isnull(row[child.name]):
      return 1000000
    return (datetime.now() - row[child.name]).days
    # r = relativedelta.relativedelta(datetime.now(), row[child.name])
    # return r.months  * (12 * r.years)

  df = child.get_stripped()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CDaySince', 'CDAYSINCE', call_cdsince, num_args=1)

#

def call_cmsince(ctx, name, args):
  child = args[0]

  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    if pd.isnull(row[child.name]):
      return 1000
    return round((datetime.now() - row[child.name]).days / 30)

  df = child.get_stripped()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CMonthSince', 'CMONTHSINCE', call_cmsince, num_args=1)

#

def call_exists(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['count'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  # display(agg)
  agg.loc[agg[name]>0,name] = 1
  agg[name] = agg[name].astype(np.int64) # REVIEW type cast
  result = ctx.create_subframe(name, pivots)
  result.fill_data(agg)
  return result

register_function('Exists', 'EXISTS', call_exists, num_args=1, takes_pivots=True)

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

#

def call_accumulate(ctx, name, args):
  """
  ACC might be given a frame "sparse in dates", but it fills it up because
  accumulated rows show still show up for previously unexisting dates.
  Eg. if there were orders in October and December, but none in November, a
  count for November must still show up (with the same value as October).
  """
  time_col = 'CMONTH(date)' # args[2].name
  child = args[0]
  pivots = list(child.pivots)

  original = child.get_stripped()
  original.rename(columns={ child.name: name }, inplace=True)

  # Tip: to better understand this, see how it works for a single customer.
  # Eg: display(acc[acc.customer=='5b69c4280998ba2b42deb32c'])
  # original = original[original.customer=='5b69c4280998ba2b42deb32c']

  acc = original.copy()
  acc.set_index(pivots, inplace=True)
  leaf = original.copy()
  date_counts = sorted(ctx.get_date_range())
  for date in date_counts:
    print("date", date)
    leaf[time_col] += 1
    leaf = leaf[leaf[time_col]<date_counts[-1]]
    acc = leaf.set_index(pivots).add(acc, fill_value=0)
    # Dates may only be those in date_counts.

  # For debugging purposes. Must be done while acc is indexed.
  acc['__original__'] = original.set_index(pivots)[name]
  acc.reset_index(inplace=True)

  acc.drop('__original__', axis=1, inplace=True)

  result = ctx.create_subframe(name, pivots)
  result.fill_data(acc)
  return result

register_function('Accumulate', 'ACC', call_accumulate, num_args=1, takes_pivots=False)

#

def call_time_since(ctx, name, args, pivots):
  child = args[0]
  time_col = 'CMONTH(date)' # args[1].name

  import builtins
  builtins.child = child

  colUniqueVals = {}
  for pivot in child.pivots:
    colUniqueVals[pivot] = ctx.df[pivot].unique()
  colUniqueVals[time_col] = ctx.get_date_range()
  df = gen_cartesian(colUniqueVals)

  df.set_index(list(child.pivots), inplace=True)
  c = child.df.set_index(list(child.pivots))

  df[child.name] = c[child.name]
  df[name] = df[time_col]-df[child.name]

  df.loc[df[name]<0,name] = -100

  builtins.df = df
  builtins.c = c
  df.reset_index(inplace=True)

  result = ctx.create_subframe(name, {time_col,*child.pivots})
  result.fill_data(df)
  return result

register_function('TimeSince', 'TIME_SINCE', call_time_since, num_args=1, takes_pivots=True)

#

def call_tsinceseen(ctx, name, args, pivots):
  child = args[0]
  time_col = args[1].name

  groupbyMinusTime = list(set(pivots)-{time_col})

  colUniqueVals = { pivot: ctx.df[pivot].unique() for pivot in pivots}
  df = gen_cartesian(colUniqueVals)

  df_leaf = df.copy()
  df[name] = np.NaN

  date_counts = sorted(ctx.get_date_range(), reverse=False)
  for date_count in date_counts:
    maxed = ctx.df[ctx.df[time_col]<=date_count].groupby(groupbyMinusTime).agg({ child.name: ['max'] })

    maxed.columns = [name] # Must come before reset_index.
    maxed.reset_index(inplace=True)

    # Use the pd.merge magic to combine the aggregated value into all rows
    # in df for years after date_count.
    df_leaf['__key__'] = 0
    df_leaf.loc[df_leaf[time_col]>=date_count, '__key__'] = 123
    maxed['__key__'] = 123

    other = pd.merge(df_leaf, \
      maxed, \
      on=[*groupbyMinusTime,'__key__'], \
      how='left', \
      suffixes=(False,False),)
    df.update(other)

  df[name] = df[time_col] - df[name]
  df[name].fillna(10000000, inplace=True)

  # df = df[df['order.customer']=='5b69c4240998ba2b42de9708'].sort_values('CMONTH(order.date)')
  # display(df[df['order.customer']=='5b69c4240998ba2b42de9708'].sort_values('CMONTH(order.date)'))
  # display(df[df['order.customer']=='5b69c4280998ba2b42deb32c'].sort_values('CMONTH(order.date)'))

  # display(df)

  result = ctx.create_subframe(name, pivots)
  result.fill_data(df)
  return result

register_function('TimeSinceSeen', 'TSINCESEEN', call_tsinceseen, num_args=2, takes_pivots=True)
