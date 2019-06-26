
from datetime import datetime, timedelta
from dateutil import relativedelta
import time

from ..lib.gen_cartesian import gen_cartesian
from .lib.lib import can_collapse_date
from ..lib.cmonth import date_to_cmonth, cmonth_to_date
from .lib.pergroup import make_pergroup

import numpy as np
import pandas as pd

fns = {}
def register_function(keyword, call, **kwargs):
  if keyword in fns:
    raise Exception()
  fns[keyword] = dict(
    name=keyword,
    keyword=keyword,
    call=call,
    takes_pivots=kwargs.get('takes_pivots', False),
    num_args=kwargs.get('num_args', 1),
  )

from .counts import accumulate, csince
register_function('ACCUMULATE', accumulate, num_args=1)
register_function('CSINCE', csince, num_args=1)

from .compare import greaterthan
register_function('GREATERTHAN', greaterthan, num_args=2)

from .stats import strend
register_function('STREND', strend, num_args=1)

from .verb import JSON_GET, DICT_GET
register_function('JSON_GET', JSON_GET, num_args=2)
register_function('DICT_GET', DICT_GET, num_args=2)

from .formatters import EMAIL_DOMAIN, DOMAIN_EXT
register_function('EMAIL_DOMAIN', EMAIL_DOMAIN, num_args=1)
register_function('DOMAIN_EXT', DOMAIN_EXT, num_args=1)

def getFunction(name):
  return fns.get(name)

#

def call_fwd(ctx, name, args, pivots):
  lag = args[1]
  child = args[0]
  time_col = 'CMONTH(date)'

  shifted = child.get_stripped()
  shifted.rename(columns={ child.name: name }, inplace=True)

  shifted[time_col] += lag

  # shifted[time_col] = shifted[time_col].astype(np.int64)

  # If pivots isn't supplied, use child's pivots instead.
  if pivots is None:
    pivots = child.pivots
  
  result = ctx.create_subframe(name, pivots)
  result.fill_data(shifted, child.fillnan)
  return result

register_function('SHIFT', call_fwd, num_args=2, takes_pivots=True)

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
  time_col = 'CMONTH(date)'
  
  if pivots:
    raise NotImplementedError()
    # groupby = pivots
    # for pivot in pivots:
    #   assert pivot in child.get_pivots()
  else:
    child_pivots = list(child.get_pivots())

  df = child.get_stripped()
  df.rename(columns={ child.name: name }, inplace=True)

  df = df[df['customer']=='5b69c4280998ba2b42deb32c']
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

  result = ctx.create_subframe(name, pivots)
  result.fill_data(df, 0)
  return result

register_function('MINUSPREV', call_minusprev, num_args=1, takes_pivots=True)

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
    return date_to_cmonth(datetime.strptime(row[child.name], '%Y-%m-%dT%H:%M:%S.%fZ'))
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, pivots)
  result.fill_data(df)
  return result

register_function('CMONTH', call_cmonth, num_args=1)

#

def call_cdsince(ctx, name, args):
  child = args[0]

  assert child.get_stripped()[child.name].dtype == np.dtype('datetime64[ns]')

  def apply(row):
    if pd.isnull(row[child.name]):
      return -99999
    return (datetime.now() - row[child.name]).days
    # r = relativedelta.relativedelta(datetime.now(), row[child.name])
    # return r.months  * (12 * r.years)

  df = child.get_stripped()
  df[name] = df.apply(apply, axis=1)

  result = ctx.create_subframe(name, child.get_pivots())
  result.fill_data(df)
  return result

register_function('CDAYSINCE', call_cdsince, num_args=1)

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

  result = ctx.create_subframe(name, child.get_pivots())
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
  result = ctx.create_subframe(name, pivots)
  result.fill_data(agg)
  return result

register_function('EXISTS', call_exists, num_args=1, takes_pivots=True)

#

def call_latest(ctx, name, args, pivots):
  child = args[0]
  # FIXME: childResult should be used to generate the thing below, not ctx.df
  # and childName
  agg = ctx.df.groupby(pivots).agg({ child.name: (lambda x: x.iloc[0]) })
  agg.columns = [name]
  agg.reset_index(inplace=True)
  result = ctx.create_subframe(name, pivots)
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
  result = ctx.create_subframe(name, pivots)
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
  result = ctx.create_subframe(name, pivots)
  result.fill_data(agg, fillnan=0)
  return result

register_function('COUNT', call_count, num_args=1, takes_pivots=True)

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
  result.fill_data(acc, fillnan=0)
  return result

register_function('ACC', call_accumulate, num_args=1, takes_pivots=False)

#


def timesince(keys, rows):
  '''Accumulate _value_ across dates.'''

  # if type(rows[0]['_value_']) == int:
  #   raise Exception('Not expected value integer')

  # TODO assert continuity of key (ie. CMONTH(date)) values, otherwise this will break.

  count = None
  result = {}
  for date in sorted(rows.keys()):
    if not count is None:
      count += 1
      result[date] = count
    elif not rows[date] or date < date_to_cmonth(rows[date]['_value_']):
      result[date] = -9999
    else:
      count = 0
      result[date] = 0
  return result

call_timesince = make_pergroup(timesince)

"""
def call_timesince(ctx, name, args, pivots):
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
  df[name] = df[time_col] - df[child.name]

  # Fill rows for which event hasn't happened yet with very negative number.
  df.loc[df[name]<0,name] = -99999

  builtins.df = df
  builtins.c = c
  df.reset_index(inplace=True)

  result = ctx.create_subframe(name, {time_col,*child.pivots})
  result.fill_data(df)
  return result
"""

register_function('TIME_SINCE', call_timesince, num_args=1, takes_pivots=True)

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
    df_leaf.loc[df_leaf[time_col]>=date_count, '__key__'] = 123
    maxed['__key__'] = 123

    other = pd.merge(df_leaf, \
      maxed, \
      on=[*groupbyMinusTime,'__key__'], \
      how='left', \
      suffixes=(False,False),)
    df.update(other)

  df[name] = df[time_col] - df[name]
  result = ctx.create_subframe(name, pivots)
  # Fill NaN values with big positive number.
  result.fill_data(df, fillnan=-99999)
  return result

register_function('TSINCESEEN', call_tsinceseen, num_args=2, takes_pivots=True)
