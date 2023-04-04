import os
import sys
import argparse
from time import sleep
import psycopg2

parser = argparse.ArgumentParser()
parser.add_argument("--yes-really", action="store_true")
args = parser.parse_args()

if not args.yes_really:
    parser.print_usage()
    sys.exit(0)

# ================================================================================
from dotenv import load_dotenv

load_dotenv()


# ================================================================================
def checkenv():
    """Docstring"""
    valid = True
    for envvar in [
        "DBHOST",
        "DBADMINUSER",
        "DBADMINPASSWORD",
        "DBNAME",
        "DBUSER",
        "DBPASSWORD",
    ]:
        if os.getenv(envvar) is None:
            print(f"please set {envvar} in your .env")
            valid = False

    if not valid:
        return False

    return True


if not checkenv():
    sys.exit(0)

# ================================================================================
dbhost = os.getenv("DBHOST")
dbadminuser = os.getenv("DBADMINUSER")
dbadminpassword = os.getenv("DBADMINPASSWORD")
dbname = os.getenv("DBNAME")
dbuser = os.getenv("DBUSER")
dbpassword = os.getenv("DBPASSWORD")

conn = psycopg2.connect(
    host=dbhost, user=dbadminuser, password=dbadminpassword, database="postgres"
)
conn.autocommit = True
cursor = conn.cursor()

for cmd in [
    # f"CREATE USER {dbuser} WITH PASSWORD '{dbpassword}'",
    # f"ALTER USER {dbuser} CREATEDB",
    f"DROP DATABASE IF EXISTS {dbname}",
    f"CREATE DATABASE {dbname}",
    f"GRANT ALL PRIVILEGES on DATABASE {dbname} TO {dbuser}",
]:
    print(cmd)
    cursor.execute(cmd)
    sleep(1)

conn.close()
