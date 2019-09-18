
from timeit import default_timer as timer
from functools import wraps
import pandas as pd
import numpy as np

from .lib import fancy_apply, can_collapse_date, uncollapse_date, assert_constant_nrows

def per_value(innerfn, fillna=0, dtype=None, num_args=1):
  """
  Wraps around a function that takes a value.
  """

  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    df = child.get_stripped()
    df.rename(columns={ child.name: name }, inplace=True)

    print("THIS MIGHT BREAK SOMETHING")
    df = df[pd.notnull(df[name])]

    print("UNIQUE VALUES ARE", len(df[name].unique()))
    
    replace = {}
    for value in df[name].unique():
      replace[value] = innerfn(value, args)
    
    # Something must be so awfully wrong that using Series.replace takes
    # forever while converting the records to dictionaries, replacing them
    # one-by-one in a loop, then converting them back into a DataFrame takes
    # just seconds.
    # df[name] = df[name].replace(replace) # NOTE takes 18 hours.
    
    start = timer()
    records = df.to_records('dict')
    for record in records:
      record[name] = replace[record[name]]
    df = pd.DataFrame(records)
    print("\nFunction took: %s seconds" % round(timer() - start, 2))

    if dtype:
      print("using dtype %s" % repr(dtype))
      df[name] = df[name].astype(dtype)

    result = ctx.table.create_subframe(name, child.pivots)
    result.fill_data(df, fillnan=fillna, dtype=dtype)
    return result

  return {
    'call': magic,
    'num_args': num_args,
    'takes_pivot': False,
  }