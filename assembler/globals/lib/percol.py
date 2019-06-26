
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np

def make_percol(innerfn, fillna=0, dtype=None):
  
  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    df = child.get_stripped()
    df.rename(columns={ child.name: '_value_' }, inplace=True)

    df['_result_'] = innerfn(df['_value_'], args)
    df[name] = innerfn(df['_value_'], args)

    if dtype:
      print("using dtype %s" % repr(dtype))
      df[name] = df[name].astype(dtype)

    result = ctx.create_subframe(name, child.pivots)
    result.fill_data(df, fillnan=fillna, dtype=dtype)
    return result
  return magic