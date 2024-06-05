import os
from pathlib import Path
from ddpui.models.org import OrgDbt


def create_edr_command(
    orgdbt: OrgDbt,
    bucket_file_path: str,
):
    """Create the elementary command to run"""
    edr_binary = Path(os.getenv("DBT_VENV")) / "venv/bin/edr"
    aws_access_key_id = os.getenv("ELEMENTARY_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("ELEMENTARY_AWS_SECRET_ACCESS_KEY")
    s3_bucket_name = os.getenv("ELEMENTARY_S3_BUCKET")
    project_dir = Path(orgdbt.project_dir) / "dbtrepo"
    profiles_dir = project_dir / "elementary_profiles"
    cmd = [
        str(edr_binary),
        "send-report",
        "--aws-access-key-id",
        aws_access_key_id,
        "--aws-secret-access-key",
        aws_secret_access_key,
        "--s3-bucket-name",
        s3_bucket_name,
        "--bucket-file-path",
        bucket_file_path,
        "--profiles-dir",
        str(profiles_dir),
    ]
    return cmd, project_dir
