
import builtins
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .Context import Context
from .lib import process
from .parser import parseLineToCommand

def genMonthCount(start, end):
  curr = start
  while curr < end:
    yield curr
    curr += relativedelta(months=1)

def genOutputMatrix(dataframes, shape):
  cols = ['month_block', 'shop', 'item']

  config = shape['config']

  outputPivotVals = {}

  assert '__DATE__' in shape['config']['output_pivots']
  for col in set(shape['config']['output_pivots'])-{'__DATE__'}:
    df = dataframes[col]
    assert col in shape['pivots'], col
    assert len(shape['pivots'][col]) == 1
    dfKey = shape['pivots'][col][0]
    print("asdf", dfKey)
    outputPivotVals[col.lower()] = df[dfKey].unique()

  start = datetime.strptime(config['date_start'], '%Y-%m-%d')
  end = datetime.strptime(config['date_end'], '%Y-%m-%d')

  outputPivotVals['MCOUNT(date)'] = list(genMonthCount(start, end))

  m2 = []
  for date in genMonthCount(start, end):
    _sales = train[train.month_block==date]
    assert len(sales)
    m2.append(np.array(list(product([date], _sales.shop.unique(), _sales.item.unique())), dtype='int16'))

  matrix = pd.DataFrame(np.vstack(m2), columns=cols)
  matrix['month_block'] = matrix['month_block'].astype(np.int16)

  matrix['shop'] = matrix['shop'].astype(np.int16)
  matrix['item'] = matrix['item'].astype(np.int16)

  matrix.sort_values(cols, inplace=True)

  # felipap: using 'train', which lists demand by day ('date' attr), sum
  # up to create 'group', which lists total demand that month

  group = train.groupby(['month_block','shop','item']).agg({
      'item_cnt_day': ['sum'],
  })
  # print(group)
  group.columns = ['item_cnt_month']
  group.reset_index(inplace=True)

  # Aggregate train set by shop/item pairs to calculate target
  # aggreagates, then <b>clip(0,20)</b> target value. This way train
  # target will be similar to the test predictions.

  # felipap: add item_ctn_month column to 'matrix'
  matrix = pd.merge(matrix, group, on=cols, how='left')
  matrix['item_cnt_month'] = (matrix['item_cnt_month']
                                  .fillna(0)
                                  .clip(0,20) # NB clip target here
                                  .astype(np.float16))

  # I use floats instead of ints for item_cnt_month to avoid
  # downcasting it after concatination with the test set later. If
  # It would be int16, after concatination with NaN values it becomes
  # int64, but foat16 becomes float16 even with NaNs.

  return matrix


import builtins

def processShape(shape, dataframes):
  dataframes['Matrix'] = genOutputMatrix(dataframes, shape)

  builtins.context = Context(dataframes, 'Matrix', 'month_block')

  context.rels = set(
    (*val.split('.'),*key.split('.'))
    for (val, key) in shape['pointers'].items()
  )
  print(context.rels)
  context.pivots = shape['pivots']

  for index, feature in enumerate(shape['features']):
      print('Processing {}/{}: {}'.format(index+1, len(shape['features']), feature))
      cmd = parseLineToCommand(feature)
      # pprint.pprint(cmd)
      process(context, cmd)
      display(context.df)
      break
