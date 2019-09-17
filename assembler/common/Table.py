
from ..lib.cmonth import date_to_cmonth, cmonth_to_date, date_yearmonth, yearmonth_date

class Table(object):
  """Table corresponds to a"""
  def __init__(self, name, config, df):
    self.name = name
    self.config = config
    
    if df.empty:
        print(f'Dataframe for ${name} is empty. This might break things.')

    if 'CMONTH(date)' in config['pivots']:
      # TODO find a better place to make this transformation
      uniques = df['CMONTH(date)'].unique()
      mapping = { unique: date_to_cmonth(yearmonth_date(unique)) for unique in uniques }
      df.replace(mapping, inplace=True)
    
    # Validate the table configuration.
    keys = config['pivots']

    if not set(keys).issubset(df.columns):
      raise Exception(f'Expected pivots {keys} for df {name} but instead'
          'found columns {", ".join(df.columns)}.')
    
    # REVIEW this
    if df.duplicated(keys).any():
      print(f'Dropping duplicates in df {name}', keys, \
        df[keys].shape, df.duplicated(keys).shape[0])
      df = df.drop_duplicates(keys)

    self._keys = config['pivots']
    self._dataframe = df

  def get_unique_field(self, field):
    # TODO check if field is part of the table
    return list(self._dataframe[field].unique())

  def get_keys(self):
    return self._keys

  def get_pointers(self):
    return self.config.get('pointers')

  def get_dataframe(self):
    return self._dataframe

