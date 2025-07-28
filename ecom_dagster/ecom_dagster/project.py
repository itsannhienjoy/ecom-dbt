from pathlib import Path

from dagster_dbt import DbtProject

ecom_dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "ecom_dbt").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
ecom_dbt_project.prepare_if_dev()