import pandas as pd
import os
import shutil

from utils.logger import get_logger
from utils.config_loader import load_config

logger = get_logger()

quarantine_path = "C:/mini-datalake/data/quarantine"

expected_columns = [
    "cliente_id",
    "fecha",
    "monto",
    "tipo",
    "tipo_transaccion"
]

raw_path = "C:/mini-datalake/data/raw"
bronze_path = "C:/mini-datalake/data/bronze"
metadata_path = "C:/mini-datalake/metadata/processed_files.txt"

os.makedirs(bronze_path, exist_ok=True)
os.makedirs("C:/mini-datalake/metadata", exist_ok=True)

# leer archivos ya procesados
if os.path.exists(metadata_path):
    with open(metadata_path, "r") as f:
        processed = f.read().splitlines()
else:
    processed = []

files = [f for f in os.listdir(raw_path) if f.endswith(".csv")]

new_data = []

for file in files:

    if file in processed:
        print("Archivo ya procesado:", file)
        continue

    print("Procesando:", file)

    df = pd.read_csv(f"{raw_path}/{file}")
    
    ##############################################################
    # validar columnas
    if list(df.columns) != expected_columns:

        logger.warning(f"Archivo con esquema incorrecto: {file}")

        os.makedirs(quarantine_path, exist_ok=True)

        file_path = os.path.join(raw_path, file)
        quarantine_file = os.path.join(quarantine_path, file)

        shutil.move(file_path, quarantine_file)

        print("Archivo movido a quarantine")

        continue
#####################################################################3
    
    

    df["fecha"] = pd.to_datetime(df["fecha"])

    df["year"] = df["fecha"].dt.year
    df["month"] = df["fecha"].dt.month

    df["monto"] = pd.to_numeric(df["monto"], errors="coerce")

    df = df[df["monto"].notna()]

    df = df.drop_duplicates()

    new_data.append(df)

    # guardar archivo como procesado
    with open(metadata_path, "a") as f:
        f.write(file + "\n")

if new_data:

    df_final = pd.concat(new_data)

    # limpiar bronze antes de escribir
    if os.path.exists(bronze_path):
        shutil.rmtree(bronze_path)

    df_final.to_parquet(
        bronze_path,
        partition_cols=["year", "month"],
        index=False
    )

    print("Datos guardados en BRONZE")

else:

    print("No hay archivos nuevos")