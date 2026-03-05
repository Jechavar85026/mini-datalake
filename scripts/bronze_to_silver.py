import pandas as pd

# Leer datos bronze
df = pd.read_parquet("C:/mini-datalake/data/bronze/transacciones.parquet")

# Regla de negocio ejemplo:
# clasificar tipo de transacción

df["tipo_transaccion"] = df["monto"].apply(
    lambda x: "compra" if x > 0 else "devolucion"
)

# Guardar en silver
df.to_parquet("C:/mini-datalake/data/silver/transacciones.parquet", index=False)

print("Proceso bronze -> silver completado")