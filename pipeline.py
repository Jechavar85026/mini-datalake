import subprocess
from utils.logger import get_logger
from utils.config_loader import load_config

config = load_config()

print(config["paths"]["raw"])
print(config["paths"]["bronze"])

logger = get_logger()

logger.info("========== INICIO PIPELINE ==========")

scripts = [
    "scripts.generate_daily_transactions",
    "scripts.raw_to_bronze",
    "scripts.bronze_to_silver",
    "scripts.silver_to_gold"
]

for script in scripts:

    try:

        logger.info(f"Ejecutando {script}")

        subprocess.run(["python", "-m", script], check=True)

        logger.info(f"{script} ejecutado correctamente")

    except subprocess.CalledProcessError as e:

        logger.error(f"Error ejecutando {script}")
        logger.error(str(e))

        break

logger.info("========== FIN PIPELINE ==========")