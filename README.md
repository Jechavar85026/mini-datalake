# Mini Data Lake (Python)

Proyecto de ingeniería de datos que implementa un Data Lake
con arquitectura Medallion.

## Arquitectura

RAW → BRONZE → SILVER → GOLD

## Características

- Ingestión incremental de archivos
- Control de duplicados
- Particiones por año y mes
- Validación de esquema
- Quarantine para archivos corruptos
- Pipeline automatizado
- Logs de ejecución

## Estructura del proyecto

mini-datalake

scripts/  
raw_to_bronze.py  
bronze_to_silver.py  
silver_to_gold.py  

utils/  
logger.py  
config_loader.py  

data/  
raw  
bronze  
silver  
gold  

pipeline.py  

## Tecnologías utilizadas

Python  
Pandas  
Parquet  

## Ejecutar el pipeline