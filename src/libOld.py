
import types
import time

import numpy as np
import pandas as pd

# TODO: rewrite variable names

def formFeat(fn, col, pivot):
    return '%s(%s|%s)' % (fn, col, ','.join(pivot))

def lagFeature(df, lags, lagCol, pivot):
    assert "month_block" in pivot

    # Example args:
    # . [1, 6, 12] mean(item_cnt_month|month_block,item) ['month_block', 'item']

    wantedCols = list(set(pivot)|set([lagCol]))
    # print("df, ", df, lags, lagCol, pivot, wantedCols)

    for i in lags:
        shifted = df[wantedCols].copy()
        # felipap: take all the rows, rename the 'item_ctn_month' to col_lag_X, and
        # merge it with the rows of i blocks in the future.
        # col_lag_X will mean: X months ago, the value was this.
        shifted.rename(columns={ lagCol: '%s_LAG_%s' % (lagCol, i)}, inplace=True)
        shifted['month_block'] += i
        df = pd.merge(df, shifted, on=list(pivot), how='left')
    return df


def createGroupBy(source, pivot, config):
    ts = time.time()


    agg = source.groupby(pivot).agg(config)

    # TODO: change format to eg. fn(item_ctn_month|month_block,type_code)
    agg.columns = [
        formFeat(col[1], col[0], pivot)
        for col in agg.columns.values
    ]

#     for col in agg.columns:
#         agg[col] = agg[col].astype(np.float16)

    agg.reset_index(inplace=True)
    return agg


def predictAggColumnName(aggCol, function, pivots):
    return "%s(%s|%s)" % (function, aggCol, ','.join(pivots))

def execAggregationFeature(feature, source):
    pivot = feature["pivot"]

    newColName = predictAggColumnName(feature["aggregate"], feature["function"], pivot)

    aggArgs = { feature["aggregate"]: [feature["function"]] }
    grouped = createGroupBy(source, pivot, aggArgs)

    assert newColName in list(grouped.columns.values), \
        "newColName %s not in %s" % (newColName, grouped.columns.values)

    if feature.get("lags", False):
        grouped = lagFeature(grouped, feature["lags"], newColName, pivot)

    if feature.get("dropMain", False):
        assert feature.get("lags", False), "Can't dropMain unless we are lagging."
        grouped.drop(newColName, axis=1, inplace=True)

    return grouped

import regex

def computeString():
    column = 'sum(revenue|date_block_num,shop_id)'
    re.match('^\w*\(.*\)', column)

def processAverageTrend(left, args):
    pandaAggArg = { args['column']: ['mean'] }
    pivotPlusBlock = args['pivot'] + [args['per']]
    lags = args['lags']
    fn = args['function']

    # print('Aggregating %s' % (left, args))

    aggCol = formFeat(fn, args['column'], args['pivot'])
    aggColPerBlock = formFeat(fn, args['column'], pivotPlusBlock)

    # print("aggCol", aggCol, aggColPerBlock)

    average = createGroupBy(left, args['pivot'], pandaAggArg)
    # display(average)
    averagePerBlock = createGroupBy(left, pivotPlusBlock, pandaAggArg)
    # display(averagePerBlock)

    grouped = lagFeature(
      df=averagePerBlock,
      lags=lags, # eg: [1,2,3]
      lagCol=aggColPerBlock, # eg. mean(price|item,month_block)
      pivot=pivotPlusBlock, # eg. ['item', 'month_block']
    )
    grouped = pd.merge(grouped, average, on=args['pivot'], how='left')

    for i in lags:
        grouped['delta_lag_%s' % i] = \
            (grouped['%s_LAG_%s' % (aggColPerBlock, i)] - grouped[aggCol]) \
            / grouped[aggCol]

    # There must be some best practice stuff behind this that I ignore...
    def selectTrend(row):
        for i in lags:
            if row['delta_lag_%s' % i]:
                return row['delta_lag_%s' % i]
        return 0

    colName = 'avg_trend(%s|%s$%s)' % (args['column'], ','.join(args['pivot']), args['per'])
    grouped[colName] = grouped.apply(selectTrend, axis=1)
    grouped[colName] = grouped[colName].astype(np.float16)
    grouped[colName].fillna(0, inplace=True)

    display(grouped)

    return grouped[pivotPlusBlock+[colName]]

def processMeanDeviation(left, args):
    pandaAggArg = { args['column']: ['mean'] }
    pivotPlusBlock = args['pivot'] + [args['per']]

    # print('Aggregating %s' % (left, args))

    aggCol = formFeat('mean', args['column'], args['pivot'])
    aggColPerBlock = formFeat('mean', aggCol, pivotPlusBlock)

    average = createGroupBy(left, args['pivot'], pandaAggArg)
    averagePerBlock = createGroupBy(left, pivotPlusBlock, pandaAggArg)

    grouped = lagFeature(averagePerBlock, [1], aggColPerBlock, pivotPlusBlock)
    grouped = pd.merge(grouped, average, on=args['pivot'], how='left')

    grouped['delta_lag_1'] = (grouped['%s_LAG_1' % aggColPerBlock] - grouped[aggCol]) / grouped[aggCol]

    colName = 'mean_dev(%s|%s$%s)' % (args['column'], ','.join(args['pivot']), args['per'])
    grouped[colName] = grouped.apply(selectTrend, axis=1)
    grouped[colName] = grouped[colName].astype(np.float16)
    grouped[colName].fillna(0, inplace=True)
    return grouped[pivotPlusBlock+[colName]]

def process(tree):
    if type(tree) == pd.core.frame.DataFrame:
        return tree
    # https://stackoverflow.com/a/624948/396050
    if isinstance(tree, types.FunctionType):
        return tree()

    if tree['action'] == 'merge':
        left = process(tree['left'])
        for keys, child in tree['right'].items():
            prefixed = process(child).copy().add_prefix('%s.' % keys[0])
            left = pd.merge(left,
                            prefixed,
                            left_on=keys[0],
                            right_on='%s.%s' % (keys[0], keys[1]),
                            how='left')
        return left
    elif tree['action'] == 'mean_deviation':
        # Add mean(x|y) - x with lag 1
        return processMeanDeviation(process(tree['left']), tree['args'])
    elif tree['action'] == 'average_trend':
        return processAverageTrend(process(tree['left']), tree['args'])
    elif tree['action'] == 'aggregate':
        left = process(tree['left'])
        aggs = tree['args']

        for index, feature in enumerate(aggs):
            print("generating %s/%s" % (index, len(aggs)))
            grouped = execAggregationFeature(feature, left)
            left = pd.merge(left, grouped, on=feature['pivot'], how='left')
        return left
    else:
        raise Exception()
