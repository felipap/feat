
from .common.Graph import Graph
from .common.Table import create_table_from_config
from .common.Output import Output
from .assembler import assemble_many
from .parser import parse_features


def assemble(
  features,
  dataframes,
  table_configs,
  output_config,
  block_type='month',
):
  import pickle
  pickle.dump(dataframes, open('/Users/felipe/Desktop/dataframes.pickle', 'wb'))

  """
  Use the input dataframes and configurations to create the tables and
  initialize the data graph.
  """

  if block_type not in ['month', 'week']:
    raise Exception()

  graph = Graph()
  for table_name, table_config in table_configs.items():
    # Check that the dataframe for this table has been supplied.
    if not table_name in dataframes:
      raise Exception(f'Dataframe for table {table_name} was not supplied.')

    table = create_table_from_config(table_name, table_config, dataframes.pop(table_name), block_type)
    graph.add_table(table)

  # Create the output object and wrap up the graph.
  output = Output(graph.tables, output_config, block_type)
  graph.add_output(output)
  graph.wrap()

  # Parse features.
  command_trees = parse_features(features)

  # Assemble them.
  assemble_many(graph, output, command_trees)
  
  # Get their column names.
  col_names = list(map(lambda c: c.get_name(), command_trees))

  # Extract them from the output object.
  return output.get_final(col_names)
