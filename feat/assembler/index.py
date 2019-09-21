
import colorful
from timeit import default_timer as timer
import pandas as pd

from ..common.Context import Context
from .column import assemble_column

def validate_intermediary(context, tree, _):
  """
  Miscellaneous tests that the output is OK.
  """
  
  # Calling assemble_column with context.current set to 'output' should take
  # care of merging the assembled columns with the Output dataframe.
  if not context.output.has_column(tree.get_name()):
    raise Exception('Something went wrong')

  df = context.df
  print(df[(df.customer == '5d430b531a55d200152297fb')&(df.__date__==2581)].shape[0])


def assemble_many(graph, output, trees, block_type):
  """
  Assemble a list of features based on a graph of tables and pointers between
  them.
  """ 
  
  # The context holds the state of the program as it explores the tree of
  # trees and assembles the data.
  print("date block is", block_type)
  context = Context(graph, output, block_type)
  
  for index, tree in enumerate(trees):
    print(colorful.green(f'Feature {index+1}/{len(trees)}:'), tree.get_name())

    start = timer()
    result = assemble_column(context, tree)
    end = timer()

    validate_intermediary(context, tree, result)
    print("Feature took: %.2fs" % (end - start))
  
  # # Assert no duplicate features (which might break things later)
  # if sorted(list(set(to_return.columns))) != sorted(to_return.columns):
  #   raise Exception("Duplicate features found.")
  
  col_names = list(map(lambda c: c.get_name(), trees))
  col_names += ['__date__']
  col_names += output.get_pointers().keys()
  
  return context.df[col_names]
