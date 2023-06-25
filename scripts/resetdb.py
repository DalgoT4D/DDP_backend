import os
import sys
import argparse
from time import sleep
import psycopg2
from dotenv import load_dotenv

parser = argparse.ArgumentParser()
parser.add_argument("--yes-really", action="store_true")
parser.add_argument("--yes-partially", action="store_true")
args = parser.parse_args()

load_dotenv()

if not args.yes_really and not args.yes_partially:
    parser.print_usage()
    sys.exit(0)


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

if args.yes_really:  # Full reset
    print("Full reset of database")
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

if args.yes_partially:  # Partial reset of some tables
    print("Partial reset of database")
    for cmd in [
        "delete from ddpui_orgdataflow",
        "delete from ddpui_orguser",
        "delete from ddpui_orgwarehouse",
        "delete from ddpui_orgprefectblock",
        "delete from ddpui_org",
        "delete from ddpui_orgdbt",
        "delete from authtoken_token",
        "delete from auth_user",
    ]:
        print(cmd)
        cursor.execute(cmd)
        sleep(1)

conn.close()
