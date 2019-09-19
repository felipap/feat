
import pandas as pd

def gen_cartesian(colUniqueVals):
  """Generate cartesian product of the columns in colUniqueVals.
  Uses https://stackoverflow.com/questions/13269890."""

  df = pd.DataFrame().assign(key=1)
  for key, values in colUniqueVals.items():
    this = pd.DataFrame({ key: values })
    df = pd.merge(df, this.assign(key=1), on='key', how='outer')
  df.drop('key', axis=1, inplace=True)

  return df
