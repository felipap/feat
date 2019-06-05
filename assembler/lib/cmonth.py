
from datetime import datetime

def date_to_cmonth(date):
  assert isinstance(date, datetime)
  return (date.year-1970)*12+date.month
