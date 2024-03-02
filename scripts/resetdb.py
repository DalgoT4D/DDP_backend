import os
import sys
import argparse
from time import sleep
import psycopg2
from dotenv import load_dotenv

parser = argparse.ArgumentParser()
parser.add_argument("--yes", action="store_true")
args = parser.parse_args()

load_dotenv()

if not args.yes:
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

if args.yes:  # Partial reset of some tables
    print("Truncating tables...")
    for cmd in [
        "TRUNCATE TABLE ddpui_blocklock       CASCADE",
        "TRUNCATE TABLE ddpui_dataflowblock   CASCADE",
        "TRUNCATE TABLE ddpui_datafloworgtask CASCADE",
        "TRUNCATE TABLE ddpui_dbtedge         CASCADE",
        "TRUNCATE TABLE ddpui_invitation      CASCADE",
        "TRUNCATE TABLE ddpui_org             CASCADE",
        "TRUNCATE TABLE ddpui_orgdataflow     CASCADE",
        "TRUNCATE TABLE ddpui_orgdataflowv1   CASCADE",
        "TRUNCATE TABLE ddpui_orgdbt          CASCADE",
        "TRUNCATE TABLE ddpui_orgdbtmodel     CASCADE",
        "TRUNCATE TABLE ddpui_orgprefectblock CASCADE",
        "TRUNCATE TABLE ddpui_orgtask         CASCADE",
        "TRUNCATE TABLE ddpui_orgtnc          CASCADE",
        "TRUNCATE TABLE ddpui_orguser         CASCADE",
        "TRUNCATE TABLE ddpui_orgwarehouse    CASCADE",
        "TRUNCATE TABLE ddpui_prefectflowrun  CASCADE",
        "TRUNCATE TABLE ddpui_task            CASCADE",
        "TRUNCATE TABLE ddpui_tasklock        CASCADE",
        "TRUNCATE TABLE ddpui_userattributes  CASCADE",
    ]:
        print(cmd)
        cursor.execute(cmd)
        sleep(1)

conn.close()
