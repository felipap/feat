
from datetime import datetime
import time

from ..common.Frame import Frame

import numpy as np
import pandas as pd

def formFeat(fn, col, pivot):
    return '%s(%s|%s)' % (fn, col, ','.join(pivot))

class Forward(object):
  name = 'Forward'
  keyword = 'FWD'

  args = 2

  def fn(ctx, tree, genColumn, pivot):
    """If pivot isn't supplied, use child's pivots instead."""

    name = tree['name']
    lag = tree['args'][1]

    child = tree['args'][0]
    childName = child['name']

    childResult = genColumn(child)

    if pivot is None:
      pivot = childResult.pivots

    result = Frame(ctx, ctx.current, pivot, name)

    shifted = childResult.getStripped()

    # felipap: take all the rows, rename the 'item_ctn_month' to col_lag_X, and
    # merge it with the rows of i blocks in the future.
    # col_lag_X will mean: X months ago, the value was this.
    shifted.rename(columns={ childName: name }, inplace=True)
    shifted[ctx.timeCol] += lag

    result.fillData(shifted)

    return result


class Get(object):
  name = 'Get'
  keyword = 'GET'

  args = 1

  def fn(ctx, tree, genColumn, pivot):
    """If pivot isn't supplied, use child's pivots instead."""

    name = tree['name']

    child = tree['args'][0]
    childName = child['name']

    childResult = genColumn(child)

    if pivot is not None:
      childResult.pivots = pivot
      # pivot = childResult.pivots

    # result = Frame(ctx, ctx.current, pivot, name)
    #
    # shifted = childResult.getStripped()
    #
    # # felipap: take all the rows, rename the 'item_ctn_month' to col_lag_X, and
    # # merge it with the rows of i blocks in the future.
    # # col_lag_X will mean: X months ago, the value was this.
    # shifted.rename(columns={ childName: name }, inplace=True)
    # shifted[ctx.timeCol] += lag
    #
    # result.fillData(shifted)

    return childResult


class MeanDiff(object):
  name = 'MeanDiff'
  keyword = 'MEAN_DIFF'

  args = 1

  def fn(ctx, tree, genColumn, pivot):

    name = tree['name']

    child = tree['args'][0]
    childName = child['name']

    childResult = genColumn(child)

    # if pivot is None:
    #   pivot = childResult.pivots
    #
    # result = Frame(ctx, ctx.current, pivot, name)
    #
    # shifted = childResult.getStripped()
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
    groupby = childResult.getPivots()
    groupbyMinusTime = list(groupby-{ctx.timeCol})

    df = childResult.getStripped()

    childMean = df.groupby(groupbyMinusTime).agg({ childName: ['mean'] })

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

    assert childName in grouped.columns

    grouped[name] = (grouped[childName] - grouped['averaged']) / grouped['averaged']
    # display(grouped)
    display('mean', grouped)

    result = Frame(ctx, ctx.current, groupby, name)
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

  # TODO implement this
  args = 1

  def fn(ctx, tree, genColumn, keyCols):
    name = tree['name']

    child = tree['args'][0]
    childName = child['name']

    result = Frame(name, ctx, ctx.current, keyCols)

    childResult = genColumn(child)

    agg = ctx.df.groupby(keyCols).agg({ childName: ['mean'] })

    agg.columns = [name]
    agg[name] = agg[name].astype(np.float64)
    agg.reset_index(inplace=True)

    result.fillData(agg)
    return result

class CMonth(object):
  name = 'CMonth'
  keyword = 'CMONTH'

  args = 1

  def fn(ctx, tree, genColumn, keyCols):
    name = tree['name']
    child = tree['args'][0]
    childName = child['name']

    childResult = genColumn(child)

    result = Frame(name, ctx, ctx.current, childResult.getPivots())

    assert 'date' in childResult.getStripped().columns

    assert childResult.getStripped().date.dtype == np.dtype('datetime64[ns]')

    def apply(row):
      parsed = row.date # datetime.strptime(row['date'], '%Y-%m-%d')
      try:
        return int((parsed.year - 2000)*12+parsed.month)
      except:
        print("PQPQPQPQ", row)

    df = childResult.getStripped().copy()
    df[name] = df.apply(apply, axis=1)

    result.fillData(df)
    return result

class Sum(object):
  name = 'Sum'
  keyword = 'SUM'

  def fn(ctx, tree, genColumn, keyCols):
    name = tree['name']

    child = tree['args'][0]
    childName = child['name']

    result = Frame(name, ctx, ctx.current, keyCols)

    childResult = genColumn(child)

    # FIXME: childResult should be used to generate the thing below, not ctx.df and
    # childName
    agg = ctx.df.groupby(keyCols).agg({ childName: ['sum'] })

    agg.columns = [name]
    agg[name] = agg[name].astype(np.float64)
    agg.reset_index(inplace=True)

    result.fillData(agg)
    return result

allFunctions = [CMonth, Forward, MeanDiff, Sum, Mean, Get]

def getFunctions():
  fns = {}
  for cls in allFunctions:
    fns[cls.keyword] = cls.fn
  return fns
