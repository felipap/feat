
from datetime import datetime

from ..lib.gen_cartesian import gen_cartesian
from .lib.lib import can_collapse_date
from ..lib.tblock import date_to_cmonth, cmonth_to_date

import numpy as np
import pandas as pd

functions = {}
def register_function(keyword, call, **kwargs):
  if keyword in functions:
    raise Exception()
  functions[keyword] = dict(
    name=keyword,
    keyword=keyword,
    call=call,
    takes_pivots=kwargs.get('takes_pivots', False),
    num_args=kwargs.get('num_args', 1),
  )

def call_shift(ctx, name, args, pivots):
  lag = args[1]
  child = args[0]

  shifted = child.get_stripped()
  shifted.rename(columns={ child.name: name }, inplace=True)

  shifted[child.get_date_col()] += lag

  # shifted[time_col] = shifted[time_col].astype(np.int64)

  # If pivots isn't supplied, use child's pivots instead.
  if pivots is None:
    pivots = child.pivots
  
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(shifted, child.fillnan)
  return result

register_function('SHIFT', call_shift, num_args=2, takes_pivots=True)

#f

def call_get(ctx, name, args, pivots):
  child = args[0]
  assert name == 'GET(%s)' % child.name, '%s != %s' % (name, child.name)
  if pivots is not None: # If pivot isn't supplied, use child's pivots instead.
    child.pivots = pivots
  child.df.rename(columns={child.name: name}, inplace=True)
  child.name = name
  return child

register_function('GET', call_get, num_args=1, takes_pivots=True)

#

def call_minusprev(ctx, name, args, pivots):
  child = args[0]
  time_col = child.get_date_col()
  
  if pivots:
    raise NotImplementedError()
    # groupby = pivots
    # for pivot in pivots:
    #   assert pivot in child.get_pivots()
  else:
    child_pivots = list(child.get_pivots())

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)

  # df = df[df['customer']=='5b69c4280998ba2b42deb32c']
  subtracted = df.copy()
  subtracted[time_col] += 1

  subtracted.set_index(child_pivots, inplace=True)
  df.set_index(child_pivots, inplace=True)

  df.fillna(value={ name: 0 }, inplace=True)

  subtracted.fillna(value={ name: 0 }, inplace=True)
  
  # Just using .subtract() will get rid of negative values, unless we
  # expand df to have all (consumer,cmonth) combinations in both df and
  # subtracted.

  a = df.drop(name, axis=1)
  a['__key__'] = 1
  b = subtracted.drop(name, axis=1)
  b['__key__'] = 1
  expanded = pd.merge(a.reset_index(),b.reset_index(), how='outer')
  expanded.drop('__key__', axis=1, inplace=True)

  df = pd.merge(expanded, df[name].subtract(subtracted[name], fill_value=0), on=child_pivots)

  pivots = child_pivots

  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(df, 0)
  return result

register_function('MINUSPREV', call_minusprev, num_args=1, takes_pivots=True)

#

def call_mean(ctx, name, args, pivots):
  child = args[0]
  result = ctx.table.create_subframe(name, pivots)
  agg = ctx.df.groupby(pivots).agg({ child.name: ['mean'] })
  agg.columns = [name]
  agg[name] = agg[name].astype(np.float64)
  agg.reset_index(inplace=True)
  result.fill_data(agg)
  return result

register_function('MEAN', call_mean, num_args=1, takes_pivots=True)

#

def call_cmonth(ctx, name, args):
  child = args[0]

  # print('date is', child.get_stripped().columns, name, child.name)

  df = child.get_stripped()

  pivots = child.get_pivots()
  if name in pivots:
    assert can_collapse_date(child, name)
    df = df.drop(name, axis=1).drop_duplicates()
    pivots.remove(name)

  def apply(row):
    # if pd.isna(row[child.name]):
    #   return np.nan
    return date_to_cmonth(datetime.strptime(row[child.name], '%Y-%m-%dT%H:%M:%S.%fZ'))
  df[name] = df.apply(apply, axis=1)

  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(df)
  return result

register_function('CMONTH', call_cmonth, num_args=1)

#

def call_cmsince(ctx, name, args):
  child = args[0]

  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    if pd.isnull(row[child.name]):
      return -99999
    return round((datetime.now() - row[child.name]).days / 30)

  df = child.get_stripped()
  df[name] = df.apply(apply, axis=1)

  result = ctx.table.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CMONTHSINCE', call_cmsince, num_args=1)

#

def call_exists(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['count'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  agg.loc[agg[name]>0,name] = 1
  agg[name] = agg[name].astype(np.int64) # REVIEW type cast
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(agg)
  return result

register_function('EXISTS', call_exists, num_args=1, takes_pivots=True)

#

def call_latest(ctx, name, args, pivots):
  # Should we make sure date is one of the fields?
  
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: (lambda x: x.iloc[0]) })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(agg, fillnan=0)
  return result

register_function('LATEST', call_latest, num_args=1, takes_pivots=True)

def call_sum(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['sum'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  agg[name] = agg[name].astype(np.float64) # REVIEW type cast
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(agg, fillnan=0)
  return result

register_function('SUM', call_sum, num_args=1, takes_pivots=True)

#

def call_count(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: ['count'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  agg[name] = agg[name].astype(np.int64) # REVIEW type cast
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(agg, fillnan=0)
  return result

register_function('COUNT', call_count, num_args=1, takes_pivots=True)

#

def call_count_where(ctx, name, args, pivots):
  child = args[0]
  value = args[1]

  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  value = value.replace('"', '')
  filtered = ctx.df[ctx.df[child.name] == value]
  agg = filtered.groupby(pivots).agg({ child.name: ['count'] })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  agg[name] = agg[name].astype(np.int64) # REVIEW type cast
  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(agg, fillnan=0)
  return result

register_function('COUNT_WHERE', call_count_where, num_args=1, takes_pivots=True)

#

def call_where(ctx, name, args):
  breakpoint()
  
  child = args[0]
  value = args[1]

  value = value.replace('"', '')
  
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  filtered = ctx.df[ctx.df[child.name]==value]
  
  result = ctx.table.create_subframe(name, list(child.pivots))
  result.fill_data(filtered, fillnan=0)
  return result

register_function('WHERE', call_where, num_args=2, takes_pivots=False)

#

def call_accumulate(ctx, name, args):
  """
  ACC might be given a frame "sparse in dates", but it fills it up because
  accumulated rows show still show up for previously unexisting dates.
  Eg. if there were orders in October and December, but none in November, a
  count for November must still show up (with the same value as October).
  """
  child = args[0]
  time_col = child.get_date_col() # 'CMONTH(date)' # args[2].name
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

  result = ctx.table.create_subframe(name, pivots)
  result.fill_data(acc, fillnan=0)
  return result

register_function('ACC', call_accumulate, num_args=1, takes_pivots=False)

#

def call_rank(ctx, name, args):
  child = args[0]

  frame = child.get_stripped()
  
  # FIXME this is not right. we are assuming that negative values should not
  # be considered towards finding the quantiles.
  frame.loc[frame[child.name] < 0, child.name] = np.nan

  if '__date__' in child.get_pivots():
    for date_value in frame['__date__'].unique():
      frame.loc[frame['__date__'] == date_value, name] = \
        frame[frame['__date__'] == date_value][child.name].rank(pct=True, method='average')
  else:
    frame[name] = frame[child.name].rank(pct=True, method='average')
  
  result = ctx.table.create_subframe(name, child.pivots)
  result.fill_data(frame, child.fillnan)
  return result

register_function('RANK', call_rank, num_args=1, takes_pivots=False)

#

# TODO: document
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
    df_leaf.loc[df_leaf[time_col]>=date_count, '__key__'] = 123456
    maxed['__key__'] = 123456

    other = pd.merge(df_leaf, \
      maxed, \
      on=[*groupbyMinusTime,'__key__'], \
      how='left', \
      suffixes=(False,False),)
    df.update(other)

  df[name] = df[time_col] - df[name]
  result = ctx.table.create_subframe(name, pivots)
  # Fill NaN values with big positive number.
  result.fill_data(df, fillnan=-99999)
  return result

register_function('TSINCESEEN', call_tsinceseen, num_args=2, takes_pivots=True)

