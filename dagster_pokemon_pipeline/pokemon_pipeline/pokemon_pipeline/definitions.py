from dagster import Definitions, load_assets_from_package_module, define_asset_job
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from . import assets

all_assets = load_assets_from_package_module(assets)

pokemon_job = define_asset_job("pokemon_job")

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": duckdb_pandas_io_manager.configured(
            {"database": "pokemon_pipeline.duckdb"}
        )
    },
    jobs=[pokemon_job],
)