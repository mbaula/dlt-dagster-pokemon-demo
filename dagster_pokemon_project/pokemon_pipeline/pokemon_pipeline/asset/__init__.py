from dagster import asset, get_dagster_logger, AssetExecutionContext, MetadataValue
from ..resources import DltPipeline
from ..dlt import pokemon_resource

@asset
def pokemon_pipeline(context: AssetExecutionContext, pipeline: DltPipeline):
    logger = get_dagster_logger()
    results = pipeline.create_pipeline(pokemon_resource, table_name='pokemon_data')
    logger.info(results)

    md_content = ""
    for package in results.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                md_content = f"\tTable updated: {table_name}: Column changed: {column_name}: {column['data_type']}"

    context.add_output_metadata(metadata={"Updates": MetadataValue.md(md_content)})
