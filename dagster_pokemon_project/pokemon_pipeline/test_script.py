import duckdb
import pandas as pd

con = duckdb.connect('dagster_pokemon_project/pokemon_pipeline/pokemon_pipeline.duckdb')

con.execute('USE dagster_pokemon_data')

result = con.execute('SELECT * FROM pokemon_data').fetchdf()

result.to_csv('pokemon_data.txt', index=False, sep='\t', mode='w')

with open('pokemon_data.txt', 'w') as f:
    f.write(result.to_string(index=False))
