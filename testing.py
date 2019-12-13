# from datetime import datetime
# from dateutil import tz

# # METHOD 1: Hardcode zones:
# # from_zone = tz.gettz('UTC')
# # to_zone = tz.gettz('GMT+7')
#
# # METHOD 2: Auto-detect zones:
# from_zone = tz.tzutc()
# to_zone = tz.tzlocal()
#
# # utc = datetime.utcnow()
# utc = datetime.strptime('2019-10-10 12:00:17', '%Y-%m-%d %H:%M:%S')
#
# # Tell the datetime object that it's in UTC time zone since
# # datetime objects are 'naive' by default
# utc = utc.replace(tzinfo=from_zone)
#
# # Convert time zone
# central = str(utc.astimezone(to_zone))
# print central


