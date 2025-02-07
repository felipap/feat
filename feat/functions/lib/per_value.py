
from timeit import default_timer as timer
from functools import wraps
import pandas as pd
import numpy as np

from .lib import fancy_apply, can_collapse_date, uncollapse_date, assert_constant_nrows

def per_value(innerfn, fillna=0, dtype=None, num_args=1, takes_ctx=False):
  """
  Wraps around a function that takes a value.
  """

  # Something must be so awfully wrong that using Series.replace below takes
  # forever while converting the records to dictionaries, replacing them
  # one-by-one in a loop, then converting them back into a DataFrame takes
  # just seconds.
  # df[name] = df[name].replace(replace) # NOTE takes 18 hours.

  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    dataframe = child.get_stripped()
    dataframe.rename(columns={ child.name: name }, inplace=True)

    # NOTE THIS MIGHT BREAK SOMETHING
    dataframe = dataframe[pd.notnull(dataframe[name])]
    
    replace = {}
    for value in dataframe[name].unique():
      if takes_ctx:
        replace[value] = innerfn({
          'ctx': ctx,
          'args': args,
          'block_type': ctx.output.get_block_type(),
        }, value)
      else:
        replace[value] = innerfn(value, args)
    
    start = timer()
    records = dataframe.to_records('dict')
    for record in records:
      record[name] = replace[record[name]]
    dataframe = pd.DataFrame(records)
    print("Per value took: %s seconds" % round(timer() - start, 2), innerfn.__name__)

    if dtype:
      print("using dtype %s" % repr(dtype))
      dataframe[name] = dataframe[name].astype(dtype)

    result = ctx.table.create_subframe(name, child.pivots)
    result.fill_data(dataframe, fillnan=fillna, dtype=dtype)
    return result

  return {
    'call': magic,
    'num_args': num_args,
    'takes_pivots': False,
  }
