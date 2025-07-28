from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import ecom_dbt_dbt_assets
from .project import ecom_dbt_project
from .schedules import schedules

defs = Definitions(
    assets=[ecom_dbt_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ecom_dbt_project),
    },
)