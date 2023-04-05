import pytz

IST = pytz.IST = pytz.timezone("Asia/Kolkata")


def as_ist(timestamp):
    """Return time in IST"""
    return timestamp.astimezone(IST) if timestamp.tzinfo else IST.localize(timestamp)
