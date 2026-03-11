import subprocess
import time

while True:

    print("Ejecutando pipeline...")

    subprocess.run(["python", "pipeline.py"])

    print("Pipeline terminado")

    print("Esperando 1800 segundos...\n")

    time.sleep(1800)