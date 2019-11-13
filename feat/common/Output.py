
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import itertools
from functools import reduce

from .Table import Table
from ..lib.tblock import date_to_cmonth, cmonth_to_date, date_yearmonth,\
  date_to_cweek, cweek_to_date, make_week_starts

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
  """
  Inclusive range.
  """

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

def _validate_final_dataframe(assembled):
  # Alert of NaN values being returned.
  for column in assembled.columns:
    if assembled[column].isna().any():
      print("Column %s has NaN items " % column, assembled[column].unique())

    if assembled[column].isna().any():
      print("Column %s has NaN items " % column, assembled[column].unique())

    # FIXME document this. Well wtf is this
    if assembled[column].dtype == np.dtype('O'):
      assembled[column] = assembled[column].astype(str)

  # Assert no duplicate features (which might break things later)
  if sorted(list(set(assembled.columns))) != sorted(assembled.columns):
    raise Exception("Duplicate features found.")




class Output(Table):
  """
  Output accumulates the assembled features and centralizes the information
  regarding the output of a feat.assemble() call. In its heart, it's a Table
  object, with specific attributes and logic sprinkled on top.

  Output hides many of the complexities of creating multiple time blocks for
  tables with blab la bla.
  """

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
        'date': ("2017-11-01", "2019-9-14"),
      }
      block_type: 'week' | 'month'
    """
    
    date_field = None
    date_range = None
    
    pointers = {}
    for column, value in output_config.items():
      if type(value) in [list, tuple]: # Must be a date field.
        if date_field:
          raise Exception('Found more than one date field.')
        print(f'Taking {column} to be the date field ({value})')
        date_field = column
        date_range = value
      else:
        pointers[column] = value
        
    if not date_range:
      raise Exception("No date field specified.")
    
    print("pointers", pointers, date_field, date_range, block_type)
    assert date_field
        
    """
    The scaffold is the basic frame containing the exact combination of keys for
    which we will be assembling features. If we are simply assembling features
    for customers over time, the scaffold will be a two-column frame with
    every combination of customer id and date block (eg. weeks) for which we
    will generate features (eg. number of purchases that week).

    [Customer Ids] [Date block]
    1               2018-10-04 
    1               2018-10-11 
    1               2018-10-18 
    1               2018-10-25 
    2               2018-10-18 
    2               2018-10-25 

    A no-brainer strategy would be to have in the scaffold one row per cartesian
    product of set(...all date blocks) times the set(...all customer ids).
    As a downside, we will generate rows for when a customer didn't even exist,
    which we want to avoid. (Lest we have to scrape them off "manually"
    afterwards). This is only a concern because customers are a "live" table,
    containing snapshots over time. If the keys were data blocks and product
    ids (which, suppose, are static objects over time), then the cartesian
    product approach would work just fine.

    A perfect strategy would be to check for every "live" table, for which dates
    each of their objects existed. Then, for each combination of tables ids,
    get rid of dates for which at least of the live objects didn't exist. (Hope
    you got that.)
    """

    # NOTE implementing the first strategy is a bit complicated. For now we made
    # it work for outputs of a single "live" table.
    if len(pointers) > 1:
      raise NotImplementedError('Product algorithm not implemented yet.')
    
    # dataframe = self.generate_scaffold_NO_BRAINER(pointers, tables,
    #   date_field, desired_date_counts, block_type)

    desired_date_counts = make_date_counts(date_range, block_type)

    dataframe = self.generate_scaffold_ONE_LIVE_TABLE(pointers, tables,
      date_field, desired_date_counts, block_type)

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

    self._block_type = block_type
    self._date_field = date_field

    Table.__init__(self,
      'output',
      dataframe,
      list(pointers.keys()),
      pointers,
      self._date_field,
    )
  

  def generate_scaffold_ONE_LIVE_TABLE(self, pointers, tables, date_field,
  desired_date_counts, block_type):
    """
    """

    # Remember, assuming a single pointer.
    in_pointer = list(pointers.values())[0]

    if not RE_POINTER.match(in_pointer):
      raise Exception(f'Unexpected pointer format of {in_pointer}')
    
    table_name, field = in_pointer.split('.')
    if table_name not in tables:
      raise Exception('Unrecognized table {table_name}.')
    table = tables[table_name]

    date_and_fields = table.get_unique_date_and_field_rows(field)
    for row in date_and_fields:
      assert row[0] and row[1] # Dates and field valus should all eval to true.

    output_size = len(date_and_fields)
    # print("Generating output of size", output_size)
    assert output_size < 50*1000*1000, "Output is too big!"

    dataframe = pd.DataFrame(date_and_fields, columns=[table_name, date_field])

    # Select in dataframe only those dates that we want to scaffold with.
    dataframe = dataframe[dataframe[date_field].isin(desired_date_counts)]
    
    return dataframe


  def generate_scaffold_NO_BRAINER(self, pointers, tables, date_field,
  desired_date_counts, block_type):
    """
    This is the no-brainer implementation.
    Record each unique value of the tables that the output points to (eg.
    for each product id), so we can scaffold an output dataframe.
    """

    unique_values = {}
    for in_pointer in pointers.values():
      if not RE_POINTER.match(in_pointer):
        raise Exception(f'Unexpected pointer format of {in_pointer}')
      
      table_name, field = in_pointer.split('.')
      if table_name not in tables:
        raise Exception('Unrecognized table {table_name}.')
      table = tables[table_name]
        
      # if 'restrict_ids_to' in config and field in config['restrict_ids_to']:
      #   t, f = config['restrict_ids_to'][field].split('.') # eg: Sales.location
      # # possible = set(df[keyIn].unique()) & set(dataframes[t][f].unique())
      # uniques[field.lower()] = list(set(dataframes[t][f].unique()))

      uniques = table.get_unique_values(field)
      unique_values[table_name] = uniques

      print(f'Unique values for {table_name}.{field}: ({len(uniques)} items)')

    unique_values[date_field] = desired_date_counts

    # print("Using date range", dates, unique_values[date_field], len(dates))

    sizes = map(len, unique_values.values())
    output_size = reduce(lambda x, y: x*y, sizes)

    # print("Generating output of size", output_size)
    assert output_size < 50*1000*1000, "Output is too big!"

    product = gen_product(unique_values.values())
    dataframe = pd.DataFrame(product)
    dataframe.columns = list(unique_values.keys())
    return dataframe
  

  def get_block_type(self):
    return self._block_type


  def get_date_field(self):
    return self._date_field


  def get_date_range(self):
    return sorted(self.get_dataframe()[self._date_field].unique())


  def get_final(self, col_names):
    col_names += self.get_keys()
    final = self.get_dataframe()[col_names]

    # Translate cmonth values to datetimes.
    if self.get_block_type() == 'month':
      min_month = final['__date__'].min()
      max_month = final['__date__'].max()
      mapping = { c: date_yearmonth(cmonth_to_date(c)) for c in range(min_month, max_month+1) }
    elif self.get_block_type() == 'week':
      min_week = final['__date__'].min()
      max_week = final['__date__'].max()
      mapping = { c: datetime.strftime(cweek_to_date(c), '%Y-%m-%d') for c in range(min_week, max_week+1) }
    else:
      raise Exception()

    date_field = self.get_date_field()
    # print("map is", mapping)
    final['__dcount__'] = final[date_field].replace(mapping)
    final[date_field] = final[date_field].replace(mapping)

    _validate_final_dataframe(final)

    return final



