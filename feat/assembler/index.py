
import colorful
from timeit import default_timer as timer
import pandas as pd

from ..common.Context import Context
from .column import assemble_column

def validate_intermediary(context, tree, _):
  """
  Miscellaneous tests that the output is OK.
  """
  
  context.output.assert_unique_keys()
  # Calling assemble_column with context.current set to 'output' should take
  # care of merging the assembled columns with the dataframe of the output
  # table.
  if not context.output.has_column(tree.get_name()):
    raise Exception('Something went wrong')


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
    print("Feature took: %.2fs" % (end - start))

    validate_intermediary(context, tree, result)

  col_names = list(map(lambda c: c.get_name(), trees))
  col_names += ['__date__']
  col_names += output.get_pointers().keys()
  
  return context.df[col_names]
