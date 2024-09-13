import dlt
import requests

@dlt.resource(write_disposition="replace")
def pokemon_resource(limit: int = 100):
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}"
    
    while url:  
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()
        
        yield data['results']
        
        url = data['next']

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pokemon_pipeline",
        destination="duckdb",  
        dataset_name="pokemon_data"
    )
    
    pipeline.run(pokemon_resource())
