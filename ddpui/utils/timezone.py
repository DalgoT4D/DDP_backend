import pytz

IST = pytz.IST = pytz.timezone('Asia/Kolkata')

def as_IST(timestamp):
  return timestamp.astimezone(IST) if timestamp.tzinfo else IST.localize(timestamp)
  