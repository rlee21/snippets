#!/usr/bin/python

import pytz
import datetime

def pst2utc():
    today = datetime.datetime.today()
    hour = str(input("Enter PST Hour: "))
    input_time = today.strftime("%Y-%m-%d") + " " + hour + ":00:00"
    # local_tz = pytz.timezone("US/Pacific")
    local_tz = pytz.timezone("US/Pacific")
    datetime_without_tz = datetime.datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S")
    datetime_with_tz = local_tz.localize(datetime_without_tz, is_dst=True)  # with daylight saving time
    datetime_in_utc = datetime_with_tz.astimezone(pytz.utc)
    str1 = datetime_with_tz.strftime('%Y-%m-%d %H:%M:%S %Z')
    str2 = datetime_in_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
    print('PST Datetime : %s' % (str1))
    print('UTC Datetime : %s' % (str2))


if __name__ == "__main__":
    pst2utc()
