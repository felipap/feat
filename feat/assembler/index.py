
import colorful
from timeit import default_timer as timer
import pandas as pd

from ..common.Context import Context
from .column import assemble_column

def validate_intermediary(output, tree, _):
  """
  Miscellaneous tests that the output is OK.
  """
  
  output.assert_unique_keys()
  # Calling assemble_column with context.current set to 'output' should take
  # care of merging the assembled columns with the dataframe of the output
  # table.
  if not output.has_column(tree.get_name()):
    raise Exception('Something went wrong')


def assemble_many(graph, output, trees):
  """
  Assemble a list of features based on a graph of tables and pointers between
  them.
  """
  
  # The context holds the state of the program as it explores the tree of
  # trees and assembles the data.
  context = Context(graph, output)
  
  for index, tree in enumerate(trees):
    print(colorful.green(f'Feature {index+1}/{len(trees)}:'), tree.get_name())

    start = timer()
    result = assemble_column(context, tree)
    end = timer()
    print("Feature took: %.2fs" % (end - start))

    validate_intermediary(output, tree, result)
  
  return output
