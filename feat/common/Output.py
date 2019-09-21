
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import itertools
from functools import reduce
from .Table import Table

from ..lib.tblock import date_to_cmonth, date_to_cweek, make_week_starts

def gen_product(sets):
  """
  Generate the cartesian product of a list of sets (with element sorted by
  the order that their sets were provided, of course).
  """

  # TODO one of the Kaggle solutions is smarter than just doing a complete product
  # for date in get_month_count(start, end):
  #   _sales = train[train.month_block==date]
  #   m2.append(np.array(list(product([date], _sales.shop.unique(), _sales.item.unique())), dtype='int16'))

  # def gen_cartesian(colUniqueVals):
  #   """Generate cartesian product of the columns in colUniqueVals.
  #   Uses https://stackoverflow.com/questions/13269890."""
  #   df = pd.DataFrame().assign(key=1)
  #   for key, values in colUniqueVals.items():
  #     this = pd.DataFrame({ key: values })
  #     df = pd.merge(df, this.assign(key=1), on='key', how='outer')
  #   df.drop('key', axis=1, inplace=True)
  #   return df

  return list(itertools.product(*sets))

# TODO DELETE
def caseword(word):
  return word[0].upper()+word[1:]

def make_date_counts(date_range, block_type):
  """Inclusive range."""

  start = datetime.strptime(date_range[0], '%Y-%m-%d')
  end = datetime.strptime(date_range[1], '%Y-%m-%d')

  if block_type == 'month':
    months = []
    curr = start
    while curr <= end:
      months.append(curr)
      curr += relativedelta(months=1)
    return list(map(date_to_cmonth, months))
  else:
    week_starts = make_week_starts(start, end)
    return list(map(date_to_cweek, week_starts))

RE_POINTER = re.compile(r'^\w+\.\w+$')

class Output(Table):
  """ TODO """

  def __init__(self, tables, output_config, block_type):
    """
    One output row will be generated for each combination of the tables in
    pointers and the dates we want to generate values for.

    For instance, if pointers is { 'user': 'user.id', 'product': 'product.id' },
    and dates = range('jan 2018' to 'jan 2019'), there will be an output row for
    each user and each product and each month between these two months.

    Args:
      tables: dictionary of Table, indexed by their names.
      output_config: {
        'customer': 'customer.id',
        '__date__': ("2017-11-01", "2019-9-14"),
      }
      block_type: 'week' | 'month'
    """
    
    date_field = None
    
    pointers = {}
    for column, value in output_config.items():
      if column.startswith('__date__'):
        date_field = column
        date_range = value
      else:
        pointers[column] = value
    
    print("pointers", pointers, date_field, date_range)
    assert date_field
    
    date_counts = make_date_counts(date_range, block_type)
    
    # Record each unique value of the tables that the output points to (eg.
    # for each product id), so we can scaffold an output dataframe.

    unique_values = {}
    for in_pointer in pointers.values():
      if not RE_POINTER.match(in_pointer):
        raise Exception(f'Unexpected pointer format of {in_pointer}')
      
      table_name, field = in_pointer.split('.')
      if table_name not in tables:
        raise Exception('Unrecognized table {table_name}.')
      table = tables[table_name]
        
      """
      if 'restrict_ids_to' in config and field in config['restrict_ids_to']:
        t, f = config['restrict_ids_to'][field].split('.') # eg: Sales.location
      # possible = set(df[keyIn].unique()) & set(dataframes[t][f].unique())
      uniques[field.lower()] = list(set(dataframes[t][f].unique()))
      """

      uniques = table.get_unique_values(field)
      unique_values[table_name] = uniques

      print(f'Unique values for {table_name}.{field}: ({len(uniques)} items)')

    unique_values[date_field] = date_counts

    # print("Using date range", dates, unique_values[date_field], len(dates))

    sizes = map(len, unique_values.values())
    output_size = reduce(lambda x, y: x*y, sizes)

    # print("Generating output of size", output_size)
    assert output_size < 50*1000*1000, "Output is too big!"

    product = gen_product(unique_values.values())

    dataframe = pd.DataFrame(product)
    dataframe.columns = list(unique_values.keys())

    # Output.sort_values([config['date_block'],'location','product'], inplace=True)

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

    self.date_field = date_field

    Table.__init__(self,
      'output',
      dataframe,
      list(pointers.keys()),
      pointers,
      self.date_field,
    )
  
  def get_date_field(self):
    return self.date_field


