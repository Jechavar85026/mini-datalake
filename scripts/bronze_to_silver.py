import pandas as pd

# Leer datos desde bronze
df = pd.read_parquet("C:/mini-datalake/data/bronze/transacciones.parquet")

# Convertir fecha
df["fecha"] = pd.to_datetime(df["fecha"])

# Ordenar datos
df = df.sort_values("fecha")

# ---------------------------
# REGLAS DE NEGOCIO
# ---------------------------

# Solo montos positivos
df = df[df["monto"] > 0]

# ---------------------------
# PARTICIONES
# ---------------------------

df["year"] = df["fecha"].dt.year
df["month"] = df["fecha"].dt.month

# Guardar en silver
df.to_parquet(
    "C:/mini-datalake/data/silver/transacciones.parquet",
    partition_cols=["year", "month"],
    index=False
)

print("Proceso bronze -> silver completado")