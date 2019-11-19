
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

from ..common.Output import make_date_counts, make_week_starts
from .tblock import *

def make_day_range(start, end):
  months = []
  curr = start
  start = start + timedelta(7 - start.weekday())

  while curr <= end:
    months.append(curr)
    curr += relativedelta(days=1)
  return months

def test_date():
   date = datetime.strptime('2019-08-01', '%Y-%m-%d')
   print(date_to_cweek(date))

def test_count():
  # counts = make_date_counts(['2017-11-05', '2019-01-01'], 'week')
  # start = datetime.strptime('2019-07-20', '%Y-%m-%d')
  # end = datetime.strptime('2019-08-05', '%Y-%m-%d')

  # print(make_week_starts(start, end))

  sunday = datetime.strptime('2019-07-28', '%Y-%m-%d')
  monday = datetime.strptime('2019-07-29', '%Y-%m-%d')
  
  assert date_to_cweek(sunday) == date_to_cweek(monday)
  
  print(date_to_cweek(sunday))
  print(cweek_to_date(date_to_cweek(sunday)))

  # all_weeks = make_day_range(start, end)
  # print(all_weeks)

  pass
  #   mapping = { c: datetime.strftime(cweek_to_date(c), '%Y-%m-%d') for c in range(2489, 2585) }

  # while curr <= end:
  #       months.append(curr)
  #       curr += relativedelta(months=1)
  #     return list(map(date_to_cmonth, months))