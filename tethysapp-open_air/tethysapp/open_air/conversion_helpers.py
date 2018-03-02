from datetime import datetime

def str2datetime(time_string):
    year_ = int(time_string[0:4])
    month_ = int(time_string[4:6])
    day_ = int(time_string[6:8])
    hour_ = int(time_string[8:10])
    minute_ = int(time_string[10:12])
    second_ = int(time_string[12:14])
    return datetime(year=year_, month=month_, day=day_, hour=hour_, minute=minute_, second=second_)

def datetime2str(dt):
    year = str(dt.year)
    month = str(dt.month)
    day = str(dt.day)
    hour = str(dt.hour)
    minute = str(dt.minute)
    second = str(dt.second)

    if len(month) == 1:
        month = "0" + month
    if len(day) == 1:
        day = "0" + day
    if len(hour) == 1:
        hour = "0" + hour
    if len(minute) == 1:
        minute = "0" + minute
    if len(second) == 1:
        second = "0" + second

    time_string = year
    time_string += month
    time_string += day
    time_string += minute
    time_string += minute
    time_string += second
    return time_string
