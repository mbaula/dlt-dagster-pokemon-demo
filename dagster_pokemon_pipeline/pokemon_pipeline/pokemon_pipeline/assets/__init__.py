# pokemon_pipeline/assets/__init__.py

import dlt
from dlt.sources.rest_api import rest_api_source
import pandas as pd
from dagster import asset, get_dagster_logger

@asset
def pokemon() -> pd.DataFrame:
    try:
        logger = get_dagster_logger()

        pokemon_config = {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
            },
            "resource_defaults": {
                "write_disposition": "replace",
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
            "resources": [
                "pokemon",
            ],
        }

        pokemon_source = rest_api_source(pokemon_config)

        data_list = list(pokemon_source)

        df = pd.DataFrame(data_list)

        df.columns = df.columns.map(str)

        df.drop_duplicates(inplace=True)

        logger.info(f"Total unique records fetched for 'pokemon': {len(df)}")

        return df
    except Exception as e:
        logger.error(f"Error in pokemon asset: {e}")
        raise

@asset
def berries() -> pd.DataFrame:
    try:
        logger = get_dagster_logger()

        berries_config = {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
            },
            "resource_defaults": {
                "write_disposition": "replace",
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
            "resources": [
                "berry",
            ],
        }

        berries_source = rest_api_source(berries_config)

        data_list = list(berries_source)

        df = pd.DataFrame(data_list)

        df.columns = df.columns.map(str)

        df.drop_duplicates(inplace=True)

        logger.info(f"Total unique records fetched for 'berries': {len(df)}")

        return df
    except Exception as e:
        logger.error(f"Error in berries asset: {e}")
        raise