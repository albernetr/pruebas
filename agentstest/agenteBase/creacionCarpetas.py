from pathlib import Path

def crear_estructura_operation(base_dir: str = "."):
    """
    Crea la estructura de carpetas del pipeline operacional dentro de 'operation'.
    base_dir: ruta donde se creará la carpeta 'operation' (por defecto, el directorio actual).
    """
    raiz = Path(base_dir) / "operation"

    # Carpetas principales (1–9)
    carpetas_principales = [
        "01_ingest",        # Punto de entrada de datos
        "02_raw",           # Copia inmutable / forense
        "03_base",          # Aterrizaje organizado
        "04_standardized",  # Esquema estandarizado
        "05_clean",         # Datos limpios
        "06_enriched",      # Datos enriquecidos
        "07_processed",     # Procesados según negocio
        "08_fusion",        # Fusión de fuentes
        "09_ready",         # Listos para consumo
    ]

    # Carpetas opcionales
    carpetas_opcionales = [
        "feature",   # Feature tables para ML/IA
        "store",     # Feature store / data products
        "analytics", # Vistas optimizadas para BI
    ]

    # Carpeta transversal Teorema 5 (espacio de trabajo por columna)
    carpeta_teorema = "teorema5"

    # Crear raíz
    raiz.mkdir(parents=True, exist_ok=True)

    # Crear principales
    for nombre in carpetas_principales:
        (raiz / nombre).mkdir(parents=True, exist_ok=True)

    # Crear opcionales
    for nombre in carpetas_opcionales:
        (raiz / nombre).mkdir(parents=True, exist_ok=True)

    # Crear carpeta transversal
    (raiz / carpeta_teorema).mkdir(parents=True, exist_ok=True)

    print(f"Estructura creada en: {raiz.resolve()}")

if __name__ == "__main__":
    crear_estructura_operation()
