import pandas as pd
import numpy as np
import os
from datetime import datetime

raw_path = "C:/mini-datalake/data/raw"

os.makedirs(raw_path, exist_ok=True)

timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")

file_name = f"transacciones_{timestamp}.csv"
file_path = os.path.join(raw_path, file_name)

np.random.seed()

# cantidad de transacciones aleatoria
n = np.random.randint(20, 50)

clientes = np.random.randint(100, 110, n)

montos = np.random.randint(10000, 100000, n)

tipos = np.random.choice(
    ["compra", "retiro", "transferencia"],
    n
)

tipo_transaccion = np.random.choice(
    ["debito", "credito"],
    n
)

fecha = [datetime.today().strftime("%Y-%m-%d")] * n

df = pd.DataFrame({
    "cliente_id": clientes,
    "fecha": fecha,
    "monto": montos,
    "tipo": tipos,
    "tipo_transaccion": tipo_transaccion
})

df.to_csv(file_path, index=False)

print("Archivo generado:")
print(file_path)
print(df.head())