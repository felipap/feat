
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

def date_to_cmonth(date):
  if isinstance(date, str):
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
  assert isinstance(date, datetime)
  return (date.year-1970)*12+date.month

def date_to_cweek(date: datetime):
  first_sunday = datetime(1970, 1, 4)
  days_since = (date - first_sunday).days

  return days_since // 7
  # # https://stackoverflow.com/questions/2600775
  # assert isinstance(date, datetime)
  # year, week = date.isocalendar()[:2]
  # return (year-1970)*52+week

def cweek_to_date(cweek):
  first_sunday = datetime(1970, 1, 4)
  return first_sunday + relativedelta(days=7*cweek)


def get_next_week_beginning(date, starts_on='sunday'):
  if starts_on == 'sunday':
    if date.weekday() != 6:
      # Calculate closest following sunday.
      return date + timedelta(6 - date.weekday())
      # assert start.weekday() == 0, "This should be a Sunday!"
    date = date.replace(hour=0, minute=0, second=0, microsecond=0)
    return date
  else:
    print('Start on Sundays to match the logic of Moment.js.')
    raise NotImplementedError()

def make_week_starts(start, end, starts_on='sunday'):
  """
  """
  if start >= end:
    raise Exception()
  
  start = get_next_week_beginning(start, starts_on)
  print("starts on", start)

  weeks = []
  curr = start
  while curr <= end:
    weeks.append(curr)
    curr += relativedelta(weeks=1)
  return weeks


def cmonth_to_date(cmonth):
  base_date = datetime(1970, 1, 1)
  date = base_date + relativedelta(months=cmonth-1)
  return date

def date_yearmonth(date):
  return '%d-%02d' % (date.year, date.month)

def yearmonth_date(ym):
  return datetime.strptime(ym, '%Y-%m')

# def encode_cmonth(date_code):
#   year, month = map(int, date_code.split('-'))
#   foo = (year-1970)*12+month
#   print(date_code, foo)
#   return foo

# def decode_cmonth(cmonth):
#   base_date = datetime(1970,1,1)
#   date = base_date + relativedelta(months=cmonth-1)
#   return '%d-%02d' % (date.year, date.month) # TODO use 
#   print("cmonth %s is %s" % (cmonth, '%d-%02d'))


# TODO turn this into actual tests
now = datetime.now()
month_start = datetime(year=now.year, month=now.month, day=1)
print(month_start, date_to_cmonth(month_start), cmonth_to_date(date_to_cmonth(month_start)))
assert cmonth_to_date(date_to_cmonth(month_start)) == month_start