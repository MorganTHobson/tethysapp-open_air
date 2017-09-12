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
    time_string = str(dt.year)
    time_string += str(dt.month)
    time_string += str(dt.day)
    time_string += str(dt.hour)
    time_string += str(dt.minute)
    time_string += str(dt.second)
    return time_string
