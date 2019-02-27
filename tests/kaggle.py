%load_ext autoreload
%autoreload 2

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from itertools import product
from sklearn.preprocessing import LabelEncoder
from IPython.display import display, HTML

import time
import sys
import gc
import pickle

pd.set_option('display.max_rows', 5)
pd.set_option('display.max_columns', 100)

##

import sys
sys.path.append("..")

##

import asyncio
import time
from dotenv import load_dotenv, find_dotenv
import logging
import backtrace
from datetime import datetime
import json
import os
import nest_asyncio

sys.path.append("../../settler")
from brain.src.lib.postgres import connect as connectDatabase
from brain.src.lib.s3 import connect as connectS3
from brain.src.lib.postgres import getConnection

nest_asyncio.apply()

LOGGER = logging.getLogger(__name__)

async def connectServices():
    await connectDatabase()
    await connectS3()
    LOGGER.info('Database connected')

backtrace.hook()
load_dotenv(find_dotenv(), verbose=True)

if not os.getenv('DATABASE_URL'):
    raise Exception('Expected DATABASE_URL env variable to be set.')
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -10s %(funcName) '
  '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

loop = asyncio.get_event_loop()
asyncio.run(connectServices())
conn = getConnection()
