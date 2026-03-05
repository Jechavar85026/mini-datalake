import pandas as pd

# Leer datos raw
df = pd.read_csv("C:/mini-datalake/data/raw/transacciones_2026_03_04.csv")


# Limpiar monto (convertir a numérico, errores a NaN)
df["monto"] = pd.to_numeric(df["monto"], errors="coerce")




df = df[df["monto"].notna()]

# Eliminar duplicados
df = df.drop_duplicates()

# Guardar en bronze
df.to_parquet("C:/mini-datalake/data/bronze/transacciones.parquet", index=False)


print("Proceso raw -> bronze completado")