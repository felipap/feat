
import builtins
import pandas as pd
import numpy as np

from datetime import datetime
from dateutil.relativedelta import relativedelta

from .common.Context import Context
from .assembler import assemble
from .parser import parseLineToCommand

def genMonthCount(start, end):
  curr = start
  while curr < end:
    yield (curr.year - 2000)*12+curr.month
    curr += relativedelta(months=1)


def genUniqueProduct(colUniqueVals, dataframes):
  many = 1
  for values in colUniqueVals.values():
    assert type(values) == list
    many *= len(values)

  # TODO improve check
  print("many is", many)
  assert many < 50*1000*1000

  # TODO one of the Kaggle solutions is smarter than just doing a complete product
  # for date in genMonthCount(start, end):
  #   _sales = train[train.month_block==date]
  #   m2.append(np.array(list(product([date], _sales.shop.unique(), _sales.item.unique())), dtype='int16'))

  df = pd.DataFrame().assign(key=1)
  # Generate cartesian product of the columns in colUniqueVals
  # Uses https://stackoverflow.com/questions/13269890
  for key, values in colUniqueVals.items():
    this = pd.DataFrame({ key: values })
    df = pd.merge(df, this.assign(key=1), on='key', how='outer')
  df.drop('key', axis=1, inplace=True)

  # REVIEW decide what to do with types
  for column in df.columns:
    # Convert all but datetime cols??
    if df[column].dtype == np.dtype('datetime64[ns]'):
      continue
    df[column] = df[column].astype(np.int64)

  return df


def genOutputMatrix(dataframes, shape):
  cols = ['CMONTH(date)', 'shop', 'item']

  config = shape['config']

  colUniqueVals = {}
  assert '__DATE__' in shape['config']['output_pivots']

  print(shape['config']['output_pivots'])

  for column, tableField in shape['config']['output_pivots'].items():
    if column == '__DATE__':
      continue

    tableIn, keyIn = tableField.split('.')

    df = dataframes[tableIn]
    assert tableIn in shape['pivots']
    assert keyIn in shape['pivots'][tableIn]

    if 'restrict_ids_to' in config and column in config['restrict_ids_to']:
      t, f = config['restrict_ids_to'][column].split('.') # eg: Sales.location
      # possible = set(df[keyIn].unique()) & set(dataframes[t][f].unique())
      colUniqueVals[column.lower()] = list(set(dataframes[t][f].unique()))
    else:
      colUniqueVals[column.lower()] = list(df[keyIn].unique())

  # colUniqueVals['product'] = df[keyIn].unique()

  print("colUniqueVals", colUniqueVals.keys())

  start = datetime.strptime(config['date_start'], '%Y-%m')
  end = datetime.strptime(config['date_end'], '%Y-%m')

  builtins.colUniqueVals = colUniqueVals
  colUniqueVals['CMONTH(date)'] = list(genMonthCount(start, end))

  matrix = genUniqueProduct(colUniqueVals, dataframes)

  matrix.sort_values(['CMONTH(date)','location','product'], inplace=True)

  # Aggregate train set by shop/item pairs to calculate target
  # aggreagates, then <b>clip(0,20)</b> target value. This way train
  # target will be similar to the test predictions.

  # felipap: add item_ctn_month column to 'matrix'
  # matrix = pd.merge(matrix, group, on=cols, how='left')
  # matrix['item_cnt_month'] = (matrix['item_cnt_month']
  #                                 .fillna(0)
  #                                 .clip(0,20) # NB clip target here
  #                                 .astype(np.float16))

  # I use floats instead of ints for item_cnt_month to avoid
  # downcasting it after concatination with the test set later. If
  # It would be int16, after concatination with NaN values it becomes
  # int64, but foat16 becomes float16 even with NaNs.

  return matrix

def processShape(shape, dataframes):
  dataframes['Matrix'] = genOutputMatrix(dataframes, shape)

  builtins.context = Context(dataframes, 'Matrix', 'CMONTH(date)')

  context.rels = set(
    (*val.split('.'),*key.split('.'))
    for (val, key) in shape['pointers'].items()
  )
  context.pivots = shape['pivots']
  context.pivots['Matrix'] = {'CMONTH(date)','location','product'}

  cmd = parseLineToCommand("Matrix.GET(%s)" % shape['config']['output_col'])
  # context.globals['Matrix'][shape['config']['output_col']] ==
  assemble(context, cmd['column'])
  context.df[shape['config']['output_col']] = context.df[shape['config']['output_col']].fillna(0)

  builtins.df = context.df

  for index, feature in enumerate(shape['features']):
    print('Processing {}/{}: {}'.format(index+1, len(shape['features']), feature))
    cmd = parseLineToCommand(feature)
    # TODO validate tree
    # pprint.pprint(cmd)
    assemble(context, cmd['column'])
  display(context.df)
      # break
