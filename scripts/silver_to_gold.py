import pandas as pd
import glob
import os

silver_path = "C:/mini-datalake/data/silver/*/*/*.parquet"
gold_path = "C:/mini-datalake/data/gold/clientes_resumen.parquet"

print("Leyendo datos silver...")

files = glob.glob(silver_path)

if len(files) == 0:
    print("No se encontraron archivos en SILVER")
    exit()

df_list = []

for file in files:
    df = pd.read_parquet(file)
    df_list.append(df)

df = pd.concat(df_list, ignore_index=True)

print("Filas silver:", len(df))

clientes = df.groupby("cliente_id").agg(
    total_transacciones=("monto", "count"),
    monto_total=("monto", "sum"),
    monto_promedio=("monto", "mean")
).reset_index()

os.makedirs("C:/mini-datalake/data/gold", exist_ok=True)

clientes.to_parquet(gold_path, index=False)

print("Tabla GOLD creada")
print(clientes.head())