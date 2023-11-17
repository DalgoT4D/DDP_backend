"""fetches the logs from the prefect database"""
import os
import json
import argparse
import logging
from dotenv import load_dotenv

from ddpui.utils.prefectlogs import parse_prefect_logs

parser = argparse.ArgumentParser(description="Parse the logs from a flow run")
parser.add_argument("flowrun", help="flow run id")
args = parser.parse_args()

logger = logging.getLogger()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)


if __name__ == "__main__":
    # 3b5473c2-f164-4fee-ad6a-2030d3a3deb3
    # 9755ec98-db63-40c7-8e52-eccf6b220d12
    # 55beb129-ba43-48a9-8139-14765c0b26fc
    # ed16d9ff-3fba-4bf3-bbe9-04ee51a22092
    # be98ad56-b17c-4dc8-a732-f6fe552a50f1
    load_dotenv("scripts/parseprefectlogs.env", verbose=True, override=True)
    connection_info = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    result = parse_prefect_logs(connection_info, args.flowrun)
    print(json.dumps(result, indent=2))
