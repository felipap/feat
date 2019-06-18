from datetime import datetime
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer

import pandas as pd
import numpy as np

from .common.Context import Context
from .common.Graph import Graph
from .lib.gen_cartesian import gen_cartesian
from .assembler import assemble_column
from .parser import parseLineToCommand
from .lib.state import save_state
from .lib.cmonth import date_to_cmonth, cmonth_to_date, date_yearmonth, yearmonth_date


def genMonthCount(start, end):
  curr = start
  while curr < end:
    yield date_to_cmonth(curr)
    curr += relativedelta(months=1)


def gen_unique_product(colUniqueVals, dataframes):
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

  df = gen_cartesian(colUniqueVals)

  # REVIEW decide what to do with types
  for column in df.columns:
    # Convert all but datetime cols??
    if df[column].dtype == np.dtype('datetime64[ns]'):
      continue
    # df[column] = df[column].astype(str) # .astype(np.int64)
  return df


def init_output_frame(dataframes, output_config, date_range):

  colUniqueVals = {}
  # QUESTION should we look over pointers or over pivots?
  # for column, tableField in output_config['pointers'].items():
  #   tableIn, keyIn = tableField.split('.')

  for column in output_config['pivots']:
    if column.startswith('CMONTH'):
      continue

    tableField = output_config['pointers'][column]
    tableIn, keyIn = tableField.split('.')

    df = dataframes[tableIn]

    if 'restrict_ids_to' in output_config and column in output_config['restrict_ids_to']:
      t, f = output_config['restrict_ids_to'][column].split('.') # eg: Sales.location
      # possible = set(df[keyIn].unique()) & set(dataframes[t][f].unique())
      colUniqueVals[column.lower()] = list(set(dataframes[t][f].unique()))
    else:
      colUniqueVals[column.lower()] = list(df[keyIn].unique())
    print("x %s (%s items)" % (column.lower(), len(colUniqueVals[column.lower()])))

  start = datetime.strptime(date_range[0], '%Y-%m')
  end = datetime.strptime(date_range[1], '%Y-%m')
  dates = list(genMonthCount(start, end))
  print("Using date range: %s to %s" % (start, end), dates)

  colUniqueVals[output_config['date_block']] = dates
  print("x %s (%s items)" % (output_config['date_block'], len(dates)))

  Output = gen_unique_product(colUniqueVals, dataframes)

  # Output.sort_values([output_config['date_block'],'location','product'], inplace=True)

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

def caseword(word):
  return word[0].upper()+word[1:]

def assemble(shape, type_config, dataframes):
  dataframes['Output'] = init_output_frame(dataframes, shape['output'], shape['date_range'])

  for (type_name, config) in type_config.items():
    name = caseword(type_name)
    df = dataframes[name]
    
    if 'CMONTH(date)' in config['pivots']:
      # TODO find a better place to make this transformation
      uniques = dataframes[name]['CMONTH(date)'].unique()
      mapping = { unique: date_to_cmonth(yearmonth_date(unique)) for unique in uniques }
      dataframes[name].replace(mapping, inplace=True)
    
    if not set(config['pivots']).issubset(df.columns):
      raise Exception('Expected pivots (%s) for df %s but found columns [%s].' \
        % (config['pivots'], type_name, ', '.join(df.columns)))
    if df.duplicated(config['pivots']).any():
      print("Dropping duplicates in df %s" % type_name, \
        config['pivots'],
        df[config['pivots']].shape, df.duplicated(config['pivots']).shape[0])
      dataframes[name] = df.drop_duplicates(config['pivots'])

  context = Context(dataframes, 'Output', shape['output']['date_block'])

  # Create graph of connections between tables.
  context.graph = Graph()
  for (type_name, config) in type_config.items():
    context.graph.add_node(caseword(type_name), pivots=config['pivots'])

  # Must register all nodes first, and only then register the edges.
  for (type_name, config) in type_config.items():
    if 'pointers' in config:
      for (colOut, compose) in config['pointers'].items():
        tableIn, colIn = compose.split('.')
        print(caseword(type_name), colOut, caseword(tableIn), colIn)
        context.graph.add_edge(caseword(type_name), colOut, caseword(tableIn), colIn)
  
  context.graph.add_node('Output', pivots=shape['output']['pivots'])
  for val, key in shape['output']['pointers'].items():
    colOut, tableIn, colIn = (val,*key.split('.'))
    context.graph.add_edge('Output', colOut, tableIn, colIn)

  print("Context graph is", context.graph)

  ######

  # TODO parse all before starting to assemble one-by-one.

  generated_columns = []
  
  for index, feature in enumerate(shape['features']):
    # REVIEW no need to validate tree?
    cmd = parseLineToCommand(feature)
    print('\nProcessing {}/{}: {}'.format(index+1, len(shape['features']), cmd['name']))

    generated_columns.append(cmd['name'])

    start = timer()
    result = assemble_column(context, cmd)
    end = timer()
    print("elapsed:", end - start)

    # Calling assemble_column with context.current set to 'Output' should take
    # care of merging the assembled col umns with the Output dataframe.
    if result.name not in context.globals['Output'].columns:
      raise Exception('Something went wrong')
  
  import pickle
  pickle.dump(context.df, open('/Users/felipe/dev/assembler_df.pickle', 'wb'))

  to_return = context.df[list(shape['output']['pivots']) + generated_columns]

  # Assert no duplicate features (which might break things later)
  if sorted(list(set(to_return.columns))) != sorted(to_return.columns):
    raise Exception("Duplicate features found.")


  for col in to_return.columns:
    if to_return[col].isna().any():
      print("Column %s has NaN items " % col, to_return[col].unique())
  # print('assembler is done\n\n\n')

  mapping = { c:date_yearmonth(cmonth_to_date(c)) for c in range(570, 650) }
  print("map is", mapping)
  to_return['CMONTH(date)'] = to_return['CMONTH(date)'].replace(mapping)

  return to_return
