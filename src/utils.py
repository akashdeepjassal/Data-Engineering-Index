# src/utils.py

import datetime

def get_past_month_dates(end_date=None):
    if end_date is None:
        end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=30)
    return start_date, end_date
