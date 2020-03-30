
from datetime import datetime
from ..lib.tblock import date_to_cmonth, date_to_cweek, yearmonth_date
import pandas as pd
from .Frame import Frame

def create_table_from_config(name, table_config, df, block_type):
  # if not set(table_config.keys()).issubset(['keys', 'key', 'types', 'pointers'])
  # TODO do this validation inside InputConfit

  date_key = None
  if table_config.get('date_key'):
    # TODO find a better place to make this transformation
    date_key = table_config.get('date_key')

    if date_key not in df.columns:
      raise Exception('datekey not in df columns')
    uniques = df[date_key].unique()

    if block_type == 'month':
      mapping = { unique: date_to_cmonth(yearmonth_date(unique)) for unique in uniques }
    elif block_type == 'week':
      mapping = { unique: date_to_cweek(datetime.strptime(unique, '%Y-%m-%d')) for unique in uniques }
    else:
      raise Exception()

    df.replace(mapping, inplace=True)
  
  # Validate the table configuration.
  keys = [table_config['key']]
  extended_keys = keys + (date_key and [date_key] or [])

  return Table(name, df, keys, table_config.get('pointers', {}), date_key)


class Table(object):
  """
  Table corresponds to a.
  """

  def __init__(self, name, dataframe, keys, pointers, date_key=None):
    if dataframe.empty:
      print(f'Dataframe for {name} is empty. This might break things.')

    if not set(keys).issubset(dataframe.columns):
      raise Exception(f'Expected pivots {keys} for dataframe {name} but instead '
          f'found columns {", ".join(dataframe.columns)}.')
    
    extended_keys = keys + (date_key and [date_key] or [])

    if dataframe.duplicated(extended_keys).any():
      raise Exception(f'Table {name} has duplicate values for keys')
    
    self._name = name
    self.date_key = date_key
    self._pointers = pointers
    self._keys = keys
    self._dataframe = dataframe
    self._original_cols = dataframe.columns

  def get_name(self):
    return self._name

  def has_column(self, column):
    return column in self._dataframe.columns

  def get_original_columns(self):
    return self._original_cols

  def get_unique_values(self, field):
    if not field in self._dataframe.columns:
      raise Exception()
    return list(self._dataframe[field].unique())

  def get_unique_date_and_field_rows(self, field):
    """
    Return unique combinations of ([field], [date_key]) present in the
    dataframe.
    """
    
    if not self.date_key:
      raise Exception('only valid for tables with date_key')
    if not field in self._dataframe.columns:
      raise Exception()

    unique_frame = self._dataframe.drop_duplicates([field,'__date__'])[[field,'__date__']]
    # Must convert from numpy.record to tuples manually.
    return list(map(tuple, unique_frame.to_records(index=False)))

    # unique_date_per_field = {}
    # for record in unique_frame.to_dict('records'):
    #   field_value = record[field]
    #   if not field_value in unique_date_per_field:
    #     unique_date_per_field[field_value] = []
    #   unique_date_per_field[field_value].append(record['__date__'])
    # return unique_date_per_field

  def get_keys(self):
    if self.date_key:
      return [self.date_key, *self._keys]
    return self._keys

  def get_pointers(self):
    return self._pointers

  def get_dataframe(self):
    return self._dataframe

  def set_dataframe(self, value):
    self._dataframe = value

  def create_subframe(self, colName, keys=None):
    """Creates a frame derived from the self.current table."""

    if keys is None:
      keys = self.get_keys()

    return Frame(colName, self, keys)

  def merge_frame(self, frame, on=None, right_on=None, left_on=None):
    if on:
      if not set(frame.pivots).issubset(self._dataframe.columns):
        raise Exception('Result can\'t be merged into current dataset: ', \
        frame.pivots, self._dataframe.columns)
      outer_pivots = on

      frame_df = frame.get_stripped()

      how = 'left'
      # if 'FWD(MEAN_DIFF(Order_items{CMONTH(date)=CMONTH(order.date)}.SUM(quantity|CMONTH(order.date),product),CMONTH(date)),1,CMONTH(date))' in frame_df.columns:
      #   print(frame_df.info())
      #   print(self._dataframe.info())
      #   sys.exit(0)
        # how = 'inner'

      # for pivot in on:
      #
      # if 'CMONTH(date)' in frame_df.columns:
      #   print("types", frame_df['CMONTH(date)'].dtype, self._dataframe['CMONTH(date)'].dtype)
      #   frame_df['CMONTH(date)'] = frame_df['CMONTH(date)'].astype('int64')

      # display(frame_df)
      # display(self._dataframe)

      # print("not is gonna get stuck", on, frame_df.dtypes, self._dataframe.dtypes)
      self._dataframe = pd.merge(self._dataframe, \
        frame_df, \
        on=on, \
        how=how, \
        suffixes=(False, False))
    else:
      # Merge frame into self._dataframe where self._dataframe[left_on] == frame[right_on].

      copied_frame = frame.copy()
      copied_frame.rename_pivot(right_on, '__JOIN__')
      copied_frame_df = copied_frame.get_stripped()

      columns_overlap = set(copied_frame_df.columns).intersection(self._dataframe.columns)

      # if frame has columns ['id', 'CMONTH(date)'] and self._dataframe has
      # columns ['customer', '__date__'], then we should throw an error! instead
      # of just merging on self._dataframe.customer=frame.id.
      for pivot in frame.pivots:
        if pivot != right_on and not pivot in columns_overlap:
          raise Exception(f'Pivot {pivot} of {frame.name} not considered in merger')
      
      right_on = '__JOIN__'

      outer_pivots = [left_on]
      if columns_overlap:
        assert len(columns_overlap) == 1 # Just for debugging now, this should be dealt with!!!
        overlap = columns_overlap.pop()
        outer_pivots += [overlap]
        right_on = [right_on, overlap]
        left_on = [left_on, overlap]
    
      
      self._dataframe = pd.merge(self._dataframe, \
        copied_frame_df, \
        left_on=left_on, \
        right_on=right_on, \
        how='left', \
        suffixes=(False, False))
      self._dataframe.drop('__JOIN__', axis=1, inplace=True)

    if not frame.fillnan is None:
      self._dataframe.fillna(value={ frame.name: frame.fillnan }, inplace=True)
    if not frame.dtype is None:
      print("CASTING!", frame.dtype, frame.name)
      self._dataframe[frame.name] = self._dataframe[frame.name].astype(frame.dtype)

    # self.cached_frames[frame.name] = self.graph.pivots[self.current]
    # copied = frame.copy()
    # copied.pivots = outer_pivots
    # self.cached_frames[frame.name] = copied

    return outer_pivots

  def assert_unique_keys(self):
    dropped = self._dataframe.drop_duplicates(self.get_keys())
    if dropped.shape[0] != self._dataframe.shape[0]:
      raise Exception('table has duplicates')

