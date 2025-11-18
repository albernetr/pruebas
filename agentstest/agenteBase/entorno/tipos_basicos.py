# entorno/tipos_basicos.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


class CapaDatos(Enum):
    INGEST = "01_ingest"
    RAW = "02_raw"
    BASE = "03_base"
    STANDARDIZED = "04_standardized"
    CLEAN = "05_clean"
    ENRICHED = "06_enriched"
    PROCESSED = "07_processed"
    FUSION = "08_fusion"
    READY = "09_ready"

    FEATURE = "feature"       # opcional
    STORE = "store"           # opcional
    ANALYTICS = "analytics"   # opcional


class EstadoDataset(Enum):
    """Estado lógico de un dataset dentro del método FEID/EFDI."""
    DEFINIDO = auto()         # existe en metadata, aún no cargado
    INGESTADO = auto()        # llegó a ingest/raw
    EN_PROCESO = auto()       # en transformación
    COMPLETO = auto()         # listo en su capa objetivo
    ERROR = auto()            # hubo fallos
    OBSOLETO = auto()         # reemplazado por otra versión
