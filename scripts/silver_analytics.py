
import pandas as pd
import glob

bronze_path = "C:/mini-datalake/data/bronze/*/*/*.parquet"
silver_path = "C:/mini-datalake/data/silver"

print("Leyendo datos bronze...")

files = glob.glob(bronze_path)
bronze_path 
df_list = []

for file in files:
    df = pd.read_parquet(file)
    df_list.append(df)

df = pd.concat(df_list, ignore_index=True)

print("Filas leídas:", len(df))

