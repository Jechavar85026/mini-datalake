import logging
import os
from datetime import datetime

def get_logger():

    if not os.path.exists("logs"):
        os.makedirs("logs")

    log_file = f"logs/pipeline_{datetime.today().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return logging.getLogger()