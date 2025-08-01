from pathlib import Path
import os
from dotenv import load_dotenv

from dagster_dbt import DbtProject

# Load environment variables from .env file in the dbt project directory
# Handle both running from ecom-dbt root and ecom_dagster directory
current_dir = Path.cwd()
if current_dir.name == "ecom_dagster":
    # Running from ecom_dagster directory
    dbt_project_dir = current_dir.parent / "ecom_dbt"
elif current_dir.name == "ecom-dbt":
    # Running from ecom-dbt root directory
    dbt_project_dir = current_dir / "ecom_dbt"
elif current_dir == Path("/opt/dagster/app"):
    # Running in Docker container
    dbt_project_dir = current_dir / "ecom_dbt"
else:
    # Fallback: try to find ecom_dbt relative to this file
    dbt_project_dir = Path(__file__).parent.parent.parent / "ecom_dbt"

env_file = dbt_project_dir / ".env"
if env_file.exists():
    load_dotenv(env_file)
else:
    print(f"Warning: .env file not found at {env_file}")
    print(f"Current directory: {current_dir}")
    print(f"Looking for dbt project at: {dbt_project_dir}")

ecom_dbt_project = DbtProject(
    project_dir=dbt_project_dir,
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
ecom_dbt_project.prepare_if_dev()