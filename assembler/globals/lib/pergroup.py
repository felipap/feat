
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np

from .groupby_combine import split_respecting_boundaries, process_df_chunk

def make_pergroup(innerfn, fillna=0):
  
  @wraps(innerfn)
  def magic(ctx, name, args, pivots=None):
    child = args[0]

    df = child.get_stripped()
    # df = df[df['customer']=='5b69c4250998ba2b42de9de2']

    df.rename(columns={ child.name: '_value_' }, inplace=True)

    keyminustime = list(set(child.pivots)-{'CMONTH(date)'})
    # print(keyminustime)

    date_counts = sorted(ctx.get_date_range())
    # print(date_counts)

    df_chunks = split_respecting_boundaries(df, keyminustime, 4)
    chunks = [{
      "df": dc,
      "cols": keyminustime,
      "date_counts": date_counts,
      "fn": innerfn,
      # TODO SUPPORT PIVOTS
      "time_col": "__date__", # "CMONTH(date)",
    } for dc in df_chunks]

    start = timer()
    results = list(map(process_df_chunk, chunks))
    # pool = multiprocessing.Pool()
    # results = pool.map(process_df_chunk, chunks)
    
    final = pd.DataFrame(list(np.hstack(results)))
    # print(final)
    print("elapsed: %ds" % (timer() - start))
    final.rename(columns={ '_tcount_': 'CMONTH(date)', '_result_': name }, inplace=True)

    result = ctx.create_subframe(name, child.pivots)
    result.fill_data(final, fillnan=fillna)
    return result
  return magic