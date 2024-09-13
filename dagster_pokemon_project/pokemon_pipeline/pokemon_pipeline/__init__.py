from dagster import define_asset_job, Definitions
from .asset import pokemon_pipeline 
from .resources import DltPipeline

all_assets = [pokemon_pipeline]

simple_pipeline = define_asset_job(name="simple_pipeline", selection=['pokemon_pipeline'])

defs = Definitions(
    assets=all_assets,
    jobs=[simple_pipeline],
    resources={
        "pipeline": DltPipeline(
            pipeline_name="pokemon_pipeline",
            dataset_name="dagster_pokemon_data",
            destination="duckdb"
        ),
    }
)
