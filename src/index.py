
import pandas as pd
import numpy as np

from datetime import datetime
from dateutil.relativedelta import relativedelta

from .common.Context import Context
from .assembler import assemble_column
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
  print("Generating output of size", many)
  assert many < 50*1000*1000, "Table too big"

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


def init_output_frame(dataframes, output_config):
  # assert '__DATE__' in output_config['pointers']

  colUniqueVals = {}
  for column, tableField in output_config['pointers'].items():
    if column == '__DATE__':
      continue

    tableIn, keyIn = tableField.split('.')

    df = dataframes[tableIn]
    assert tableIn in shape['pivots']
    assert keyIn in shape['pivots'][tableIn]

    if 'restrict_ids_to' in output_config and column in output_config['restrict_ids_to']:
      t, f = output_config['restrict_ids_to'][column].split('.') # eg: Sales.location
      # possible = set(df[keyIn].unique()) & set(dataframes[t][f].unique())
      colUniqueVals[column.lower()] = list(set(dataframes[t][f].unique()))
    else:
      colUniqueVals[column.lower()] = list(df[keyIn].unique())

  # colUniqueVals['product'] = df[keyIn].unique()

  print("colUniqueVals", colUniqueVals.keys())

  start = datetime.strptime(output_config['date_start'], '%Y-%m')
  end = datetime.strptime(output_config['date_end'], '%Y-%m')

  colUniqueVals['CMONTH(date)'] = list(genMonthCount(start, end))

  Output = genUniqueProduct(colUniqueVals, dataframes)

  # Output.sort_values(['CMONTH(date)','location','product'], inplace=True)

  # Aggregate train set by shop/item pairs to calculate target
  # aggreagates, then <b>clip(0,20)</b> target value. This way train
  # target will be similar to the test predictions.

  # felipap: add item_ctn_month column to 'Output'
  # Output = pd.merge(Output, group, on=cols, how='left')
  # Output['item_cnt_month'] = (Output['item_cnt_month']
  #                                 .fillna(0)
  #                                 .clip(0,20) # NB clip target here
  #                                 .astype(np.float16))

  # I use floats instead of ints for item_cnt_month to avoid
  # downcasting it after concatination with the test set later. If
  # It would be int16, after concatination with NaN values it becomes
  # int64, but foat16 becomes float16 even with NaNs.

  return Output

def assembleColumnAndAdd(ctx, tree, current):
  result = assemble_column(ctx, tree)

  if result.name not in ctx.globals[current].columns:
    ctx.globals[current] = pd.merge(ctx.globals[current], \
      result.get_stripped(), \
      on=list(result.pivots), \
      how='left', \
      suffixes=(False, False))

  return result

def assemble(shape, dataframes):
  dataframes['Output'] = init_output_frame(dataframes, shape['output'])

  context = Context(dataframes, 'Output', 'CMONTH(date)')

  # Set pointers for each table
  context.pointers = set(
    (*val.split('.'),*key.split('.'))
    for (val, key) in shape['pointers'].items()
  )
  for val, key in shape['output']['pointers'].items():
    context.pointers.add((*val.split('.'),*key.split('.')))

  # Set pivots for each table
  context.pivots = shape['pivots']
  context.pivots['Output'] = set(shape['output']['pivots'])

  for index, feature in enumerate(shape['features']):
    print('\nProcessing {}/{}: {}'.format(index+1, len(shape['features']), feature))
    cmd = parseLineToCommand(feature) # REVIEW no need to validate tree?
    assembleColumnAndAdd(context, cmd['column'], 'Output')

  # Expose to notebook console
  import builtins
  builtins.context = context

  return context.df[list(context.pivots['Output']) + shape['features']]
