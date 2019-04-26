
from datetime import datetime
import time

import numpy as np
import pandas as pd

def formFeat(fn, col, pivot):
    return '%s(%s|%s)' % (fn, col, ','.join(pivot))

class Forward(object):
  name = 'Forward'
  keyword = 'FWD'
  num_args = 2
  takes_pivots = True

  def call2(ctx, name, lazyArgs, pivots):
    lag = lazyArgs[1]
    child = lazyArgs[0]()

    to_shift = child.get_stripped()
    to_shift.rename(columns={ child.name: name }, inplace=True)
    to_shift[ctx.timeCol] += lag

    # If pivots isn't supplied, use child's pivots instead.
    if pivots is None:
      pivots = child.pivots
    print("what is", name, pivots, to_shift.columns)
    result = ctx.create_subframe(name, pivots)
    result.fillData(to_shift)
    return result

class Get(object):
  name = 'Get'
  keyword = 'GET'
  num_args = 1
  takes_pivots = True

  def call2(ctx, name, lazyArgs, pivot):
    child = lazyArgs[0]()
    assert name == 'GET(%s)' % child.name, '%s != %s' % (name, child.name)
    if pivot is not None: # If pivot isn't supplied, use child's pivots instead.
      child.pivots = pivot
    child.df.rename(columns={child.name: name}, inplace=True)
    child.name = name
    return child


class MeanDiff(object):
  name = 'MeanDiff'
  keyword = 'MEAN_DIFF'
  num_args = 1
  takes_pivots = True

  def call2(ctx, name, lazyArgs, pivot):
    child = lazyArgs[0]()

    # if pivot is None:
    #   pivot = childResult.pivots
    #
    # result = ctx.create_subframe(name, pivot)
    #
    # shifted = childResult.get_stripped()
    #
    # # felipap: take all the rows, rename the 'item_ctn_month' to col_lag_X, and
    # # merge it with the rows of i blocks in the future.
    # # col_lag_X will mean: X months ago, the value was this.
    # shifted.rename(columns={ childName: name }, inplace=True)
    # shifted[ctx.timeCol] += lag
    #
    # result.fillData(shifted)
    #
    # return result
    #
    # assert ctx.timeCol in childResult.pivot

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

class Mean(object):
  name = 'Mean'
  keyword = 'MEAN'
  num_args = 1
  takes_pivots = True

  def call2(ctx, name, lazyArgs, pivots):
    child = lazyArgs[0]()
    result = ctx.create_subframe(name, pivots)
    agg = ctx.df.groupby(pivots).agg({ child.name: ['mean'] })
    agg.columns = [name]
    agg[name] = agg[name].astype(np.float64)
    agg.reset_index(inplace=True)
    result.fillData(agg)
    return result

class CMonth(object):
  name = 'CMonth'
  keyword = 'CMONTH'
  num_args = 1

  def call2(ctx, name, lazyArgs):
    child = lazyArgs[0]()

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


class Sum(object):
  name = 'Sum'
  keyword = 'SUM'
  num_args = 1
  takes_pivots = True

  def call2(ctx, name, lazyArgs, pivots):
    child = lazyArgs[0]()
    # FIXME: childResult should be used to generate the thing below, not ctx.df
    # and childName
    agg = ctx.df.groupby(pivots).agg({ child.name: ['sum'] })
    agg.columns = [name]
    agg.reset_index(inplace=True)
    agg[name] = agg[name].astype(np.float64) # REVIEW type cast
    result = ctx.create_subframe(name, pivots)
    result.fillData(agg)
    return result

allFunctions = [CMonth, Forward, MeanDiff, Sum, Mean, Get]

def getFunctions():
  fns = {}
  for cls in allFunctions:
    assert hasattr(cls, 'name'), 'Failed for %s' % cls.name
    assert hasattr(cls, 'keyword'), 'Failed for %s' % cls.name
    assert hasattr(cls, 'num_args'), 'Failed for %s' % cls.name
    fns[cls.keyword] = cls
  return fns
