# REVIEW registering some of these as an extension of pandas, like the
# swifter package does.
# https://github.com/jmcarpenter2/swifter/blob/master/swifter/swifter.py#L95

import pandas as pd
import numpy as np

def __get_hashable_columns(df):
  result = []
  # import pickle
  # pickle.dump(df, open('./neuron_trash_what', 'wb'))

  # import ptvsd
  # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
  # ptvsd.wait_for_attach()
  # breakpoint()
  
  for column in df.columns:
    if df[column].first_valid_index() is None:
      raise Exception()
    first_notnan = df[column][df[column].first_valid_index()]
    # FIXME definitely improve this list
    if isinstance(first_notnan, dict):
      continue
    elif isinstance(first_notnan, list):
      continue
    else:
      result.append(column)
  return result


def drop_hashable_duplicates(df, method='ignore'):
  """
  NOTE pandas will throw if we ask drop_duplicates() to consider a column
  that cannot be hashed. Here we choose to ignore those columns, which might
  result in unexpected performance issues. Alternatively, we could consider
  converting some unhashable types to strings.
  """

  if method != 'ignore':
    raise NotImplementedError()

  df = stringify_unhashables(df)
  return df.drop_duplicates()
  
  hashable_columns = __get_hashable_columns(df)
  if set(df.columns) != set(hashable_columns):
    minus = set(df.columns) - set(hashable_columns)
    print("WARNING: ignoring columns %s in .drop_duplicate()" % minus)
  return df.drop_duplicates(subset=hashable_columns)


def stringify_unhashables(df):
  df = df.copy()
  unhashable_columns = list(set(df.columns) - set(__get_hashable_columns(df)))
  if unhashable_columns:
    print("unhashable_columns", unhashable_columns)
  for uc in unhashable_columns:
    # df[uc] = df[uc].astype(str)
    # https://github.com/pandas-dev/pandas/issues/25716
    # https://stackoverflow.com/questions/47332799
    df[uc] = np.where(pd.isnull(df[uc]), df[uc], df[uc].astype(str))

  return df