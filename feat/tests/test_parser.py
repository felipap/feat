

import sys
# Our path must come first, because      
sys.path = ['/home/ubuntu/human/neurons/jobs/python/feat/feat', *sys.path]

import parser

command_trees = parser.parse_features([
  'TREND_DIFF(anexample,4)',
])

print(command_trees[0].get_json_dump())