
from ..lib.cmonth import date_to_cmonth, yearmonth_date

def create_table_from_config(name, config, df):
  # if not set(config.keys()).issubset(['keys', 'key', 'types', 'pointers'])
  # TODO do this validation inside InputConfit

  date_key = None
  if config.get('date_key'):
    # TODO find a better place to make this transformation
    date_key = config.get('date_key')
    uniques = df[date_key].unique()
    mapping = { unique: date_to_cmonth(yearmonth_date(unique)) for unique in uniques }
    df.replace(mapping, inplace=True)
  
  # Validate the table configuration.
  keys = [config['key']]
  extended_keys = keys + (date_key and [date_key] or [])

  if df.duplicated(extended_keys).any():
    print(f'Dropping duplicates in df {name}.')
    df = df.drop_duplicates(keys)

  return Table(name, df, keys, config.get('pointers', {}), date_key)


class Table(object):
  """
  Table corresponds to a.
  """

  def __init__(self, name, dataframe, keys, pointers, date_key=None):
    if dataframe.empty:
      print(f'Dataframe for ${name} is empty. This might break things.')

    if not set(keys).issubset(dataframe.columns):
      raise Exception(f'Expected pivots {keys} for dataframe {name} but instead '
          f'found columns {", ".join(dataframe.columns)}.')
    
    extended_keys = keys + (date_key and [date_key] or [])

    if dataframe.duplicated(extended_keys).any():
      raise Exception(f'Table {name} has duplicate values for keys')
    
    self.name = name
    self.date_key = date_key
    self._pointers = pointers
    self._keys = keys
    self._dataframe = dataframe
    self._original_cols = dataframe.columns

  def has_column(self, column):
    return column in self._dataframe.columns

  def get_original_columns(self):
    return self._original_cols

  def get_unique_values(self, field):
    if not field in self._dataframe.columns:
      raise Exception()
    return list(self._dataframe[field].unique())

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
