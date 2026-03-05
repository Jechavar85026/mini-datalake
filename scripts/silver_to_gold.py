import pandas as pd

# Leer datos desde silver
df = pd.read_parquet("C:/mini-datalake/data/silver/transacciones.parquet")

# -------------------------
# AGREGACIÓN GOLD
# -------------------------

ventas_dia = df.groupby("fecha")["monto"].sum().reset_index()

ventas_dia = ventas_dia.rename(columns={
    "monto": "total_ventas"
})

# Guardar en gold
ventas_dia.to_parquet(
    "C:/mini-datalake/data/gold/ventas_por_dia.parquet",
    index=False
)

print("Proceso silver -> gold completado")