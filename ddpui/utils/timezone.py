import pytz
import datetime

IST = pytz.IST = pytz.timezone("Asia/Kolkata")


def as_ist(timestamp: datetime.datetime):
    """Return time in IST"""
    return timestamp.astimezone(IST) if timestamp.tzinfo else IST.localize(timestamp)


def ist_time(*args):
    """set ist time"""
    utc_dt = pytz.utc.localize(datetime.datetime.utcnow())
    converted = utc_dt.astimezone(IST)
    return converted.timetuple()
