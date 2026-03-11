import pandas as pd
import os
import shutil

bronze_path = "C:/mini-datalake/data/bronze"
silver_path = "C:/mini-datalake/data/silver"

print("Leyendo datos bronze...")

df = pd.read_parquet(bronze_path)

df["fecha"] = pd.to_datetime(df["fecha"])

df = df.sort_values("fecha")

# reglas de negocio
df = df[df["monto"] > 0]

df["year"] = df["fecha"].dt.year
df["month"] = df["fecha"].dt.month

# limpiar silver antes de escribir
if os.path.exists(silver_path):
    shutil.rmtree(silver_path)

os.makedirs(silver_path, exist_ok=True)

df.to_parquet(
    silver_path,
    partition_cols=["year", "month"],
    index=False
)

print("Proceso bronze -> silver completado")