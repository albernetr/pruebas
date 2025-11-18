from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime


# ================================
# 1. Enumeraciones base
# ================================

class DataLayer(Enum):
    """
    Capas conceptuales del entorno, alineadas con el pipeline FEID/BETAID.
    """
    EXTERNO = auto()          # Fuera del sistema (DBs, APIs, sistemas legados, etc.)
    INGEST = auto()           # 01_ingest/
    RAW = auto()              # 02_raw/
    BASE = auto()             # 03_base/
    STANDARDIZED = auto()     # 04_standardized/
    CLEAN = auto()            # 05_clean/
    ENRICHED = auto()         # 06_enriched/
    PROCESSED = auto()        # 07_processed/
    FUSION = auto()           # 08_fusion/
    READY = auto()            # 09_ready/
    FEATURE = auto()          # feature/
    STORE = auto()            # store/
    ANALYTICS = auto()        # analytics/
    TEOREMA5 = auto()         # subcarpeta transversal (no es una fase lineal)


class DatasetState(Enum):
    """
    Máquina de estados conceptual de una fuente/dataset dentro del pipeline.
    """
    EXTERNA = auto()          # Aún solo existe en el mundo externo
    INGESTADA = auto()        # Materializada en 01_ingest/
    RAW_INTERNA = auto()      # Copia interna inmutable en 02_raw/
    REGISTRADA_BASE = auto()  # Dataset lógico creado en 03_base/
    ESTANDARIZADA = auto()    # 04_standardized/
    LIMPIA = auto()           # 05_clean/
    ENRIQUECIDA = auto()      # 06_enriched/
    PROCESADA = auto()        # 07_processed/
    INTEGRADA = auto()        # 08_fusion/
    LISTA_PARA_USO = auto()   # 09_ready/ (y/o feature/store/analytics)


class TipoFuenteLogica(Enum):
    """
    Tipo lógico de la fuente de datos, independiente de la tecnología concreta.
    """
    ARCHIVO = auto()
    BD = auto()
    API = auto()
    STREAM = auto()
    IOT = auto()
    DATASET_INTERNO = auto()


# ================================
# 2. Objetos básicos del entorno
# ================================

@dataclass
class FileLocation:
    """
    Representa la ubicación física de un artefacto asociado a una fuente/dataset.
    """
    layer: DataLayer
    path: Path

    def __str__(self) -> str:
        return f"{self.layer.name}:{self.path.as_posix()}"


@dataclass
class FuenteDeDatos:
    """
    Modelo canónico de una fuente de datos en FEID/BETAID.
    """
    id: str
    nombre: str
    tipo_logico: TipoFuenteLogica

    # Origen externo (si aplica)
    origen_sistema: Optional[str] = None          # Ej.: "ERP_X", "API_Clima"
    ubicacion_externa: Optional[str] = None       # URL, conn string, ruta FTP, etc.

    # Ubicaciones internas clave a lo largo del pipeline
    carpeta_ingest: Optional[Path] = None         # Ruta en 01_ingest/
    carpeta_raw: Optional[Path] = None            # Ruta en 02_raw/
    carpeta_base: Optional[Path] = None           # Ruta en 03_base/
    carpeta_standardized: Optional[Path] = None   # Ruta en 04_standardized/
    carpeta_clean: Optional[Path] = None          # Ruta en 05_clean/
    carpeta_enriched: Optional[Path] = None       # Ruta en 06_enriched/
    carpeta_processed: Optional[Path] = None      # Ruta en 07_processed/
    carpeta_fusion: Optional[Path] = None         # Ruta en 08_fusion/
    carpeta_ready: Optional[Path] = None          # Ruta en 09_ready/

    # Metadatos
    formato: Optional[str] = None                 # CSV, PARQUET, JSON...
    frecuencia: Optional[str] = None              # ADHOC, DIARIA, HORARIA, REALTIME
    modo_actualizacion: Optional[str] = None      # APPEND, UPSERT, REBUILD
    propietario: Optional[str] = None
    sensibilidad: Optional[str] = None            # PUBLICO, INTERNO, RESTRINGIDO
    sla_disponibilidad: Optional[str] = None      # "D+1 09:00", etc.

    creado_en: datetime = field(default_factory=datetime.utcnow)
    actualizado_en: datetime = field(default_factory=datetime.utcnow)

    def actualizar_timestamp(self) -> None:
        self.actualizado_en = datetime.utcnow()


@dataclass
class EstadoFuente:
    """
    Estado actual de una fuente en el pipeline, asociado a una capa y carpeta.
    """
    fuente_id: str
    estado: DatasetState
    layer: DataLayer
    carpeta_actual: Optional[Path] = None
    agente_responsable: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


# ================================
# 3. Entorno FEID/BETAID
# ================================

class FEIDEnvironment:
    """
    Entorno lógico para el framework FEID/BETAID, donde los agentes perciben
    y actúan sobre fuentes y datasets a través de estados y capas del pipeline.
    """

    def __init__(self, root: Path) -> None:
        self.root = root

        # Registro de fuentes por id
        self._fuentes: Dict[str, FuenteDeDatos] = {}

        # Estado actual por id de fuente
        self._estados: Dict[str, EstadoFuente] = {}

        # Historial de estados por id de fuente
        self._historial_estados: Dict[str, List[EstadoFuente]] = {}

    # ---------- Helpers de paths por capa ----------

    def _path_layer(self, layer: DataLayer) -> Path:
        """
        Devuelve la carpeta raíz para una capa dada según el pipeline.
        """
        mapping = {
            DataLayer.INGEST: "01_ingest",
            DataLayer.RAW: "02_raw",
            DataLayer.BASE: "03_base",
            DataLayer.STANDARDIZED: "04_standardized",
            DataLayer.CLEAN: "05_clean",
            DataLayer.ENRICHED: "06_enriched",
            DataLayer.PROCESSED: "07_processed",
            DataLayer.FUSION: "08_fusion",
            DataLayer.READY: "09_ready",
            DataLayer.FEATURE: "feature",
            DataLayer.STORE: "store",
            DataLayer.ANALYTICS: "analytics",
            # DataLayer.EXTERNO y TEOREMA5 no tienen raíz directa aquí
        }
        if layer not in mapping:
            raise ValueError(f"La capa {layer} no tiene carpeta raíz directa asignada.")
        return self.root / mapping[layer]

    # ================================
    # 3.1 Registro y consulta de fuentes
    # ================================

    def registrar_fuente_externa(
        self,
        id_fuente: str,
        nombre: str,
        tipo_logico: TipoFuenteLogica,
        origen_sistema: Optional[str] = None,
        ubicacion_externa: Optional[str] = None,
        **kwargs,
    ) -> FuenteDeDatos:
        """
        Crea una FuenteDeDatos en estado EXTERNA (aún fuera del pipeline).
        """
        if id_fuente in self._fuentes:
            raise ValueError(f"Ya existe una fuente con id '{id_fuente}'")

        fuente = FuenteDeDatos(
            id=id_fuente,
            nombre=nombre,
            tipo_logico=tipo_logico,
            origen_sistema=origen_sistema,
            ubicacion_externa=ubicacion_externa,
            **kwargs,
        )
        self._fuentes[id_fuente] = fuente

        estado_inicial = EstadoFuente(
            fuente_id=id_fuente,
            estado=DatasetState.EXTERNA,
            layer=DataLayer.EXTERNO,
            carpeta_actual=None,
            agente_responsable=None,
        )
        self._estados[id_fuente] = estado_inicial
        self._historial_estados[id_fuente] = [estado_inicial]

        return fuente

    def obtener_fuente(self, id_fuente: str) -> FuenteDeDatos:
        return self._fuentes[id_fuente]

    def estado_actual(self, id_fuente: str) -> EstadoFuente:
        return self._estados[id_fuente]

    def historial_estados(self, id_fuente: str) -> List[EstadoFuente]:
        return list(self._historial_estados.get(id_fuente, []))

    # ================================
    # 3.2 Transiciones típicas por capa
    # ================================

    def materializar_en_ingest(
        self,
        id_fuente: str,
        subcarpeta: Optional[str] = None,
        agente: Optional[str] = None,
    ) -> FileLocation:
        """
        Transición EXTERNA → INGESTADA.
        Asume que un proceso/agent ha traído los datos a 01_ingest/.
        """
        fuente = self.obtener_fuente(id_fuente)

        base_path = self._path_layer(DataLayer.INGEST)
        if subcarpeta:
            path = base_path / subcarpeta
        else:
            path = base_path / id_fuente

        path.mkdir(parents=True, exist_ok=True)
        fuente.carpeta_ingest = path
        fuente.actualizar_timestamp()

        self._actualizar_estado(
            id_fuente,
            DatasetState.INGESTADA,
            DataLayer.INGEST,
            carpeta=path,
            agente=agente,
        )
        return FileLocation(layer=DataLayer.INGEST, path=path)

    def mover_a_raw(
        self,
        id_fuente: str,
        subcarpeta: Optional[str] = None,
        agente: Optional[str] = None,
    ) -> FileLocation:
        """
        Transición INGESTADA → RAW_INTERNA.
        Implementación simplificada: solo crea carpeta objetivo.
        """
        fuente = self.obtener_fuente(id_fuente)

        base_path = self._path_layer(DataLayer.RAW)
        if subcarpeta:
            path = base_path / subcarpeta
        else:
            path = base_path / id_fuente

        path.mkdir(parents=True, exist_ok=True)
        fuente.carpeta_raw = path
        fuente.actualizar_timestamp()

        self._actualizar_estado(
            id_fuente,
            DatasetState.RAW_INTERNA,
            DataLayer.RAW,
            carpeta=path,
            agente=agente,
        )
        return FileLocation(layer=DataLayer.RAW, path=path)

    def registrar_en_base(
        self,
        id_fuente: str,
        nombre_dataset: str,
        proyecto: Optional[str] = None,
        agente: Optional[str] = None,
    ) -> FileLocation:
        """
        Transición RAW_INTERNA → REGISTRADA_BASE.
        Crea un dataset lógico dentro de 03_base/.
        """
        fuente = self.obtener_fuente(id_fuente)

        base_root = self._path_layer(DataLayer.BASE)
        if proyecto:
            path = base_root / proyecto / nombre_dataset
        else:
            path = base_root / nombre_dataset

        path.mkdir(parents=True, exist_ok=True)
        fuente.carpeta_base = path
        fuente.actualizar_timestamp()

        self._actualizar_estado(
            id_fuente,
            DatasetState.REGISTRADA_BASE,
            DataLayer.BASE,
            carpeta=path,
            agente=agente,
        )
        return FileLocation(layer=DataLayer.BASE, path=path)

    def marcar_estandarizada(
        self,
        id_fuente: str,
        subcarpeta: Optional[str] = None,
        agente: Optional[str] = None,
    ) -> FileLocation:
        """
        Transición REGISTRADA_BASE → ESTANDARIZADA (04_standardized/).
        """
        fuente = self.obtener_fuente(id_fuente)
        base_root = self._path_layer(DataLayer.STANDARDIZED)
        if subcarpeta:
            path = base_root / subcarpeta
        else:
            path = base_root / id_fuente

        path.mkdir(parents=True, exist_ok=True)
        fuente.carpeta_standardized = path
        fuente.actualizar_timestamp()

        self._actualizar_estado(
            id_fuente,
            DatasetState.ESTANDARIZADA,
            DataLayer.STANDARDIZED,
            carpeta=path,
            agente=agente,
        )
        return FileLocation(layer=DataLayer.STANDARDIZED, path=path)

    def marcar_limpia(
        self,
        id_fuente: str,
        subcarpeta: Optional[str] = None,
        agente: Optional[str] = None,
    ) -> FileLocation:
        """
        Transición ESTANDARIZADA → LIMPIA (05_clean/).
        """
        fuente = self.obtener_fuente(id_fuente)
        base_root = self._path_layer(DataLayer.CLEAN)
        if subcarpeta:
            path = base_root / subcarpeta
        else:
            path = base_root / id_fuente

        path.mkdir(parents=True, exist_ok=True)
        fuente.carpeta_clean = path
        fuente.actualizar_timestamp()

        self._actualizar_estado(
            id_fuente,
            DatasetState.LIMPIA,
            DataLayer.CLEAN,
            carpeta=path,
            agente=agente,
        )
        return FileLocation(layer=DataLayer.CLEAN, path=path)

    # De forma análoga podrías implementar:
    # marcar_enriquecida(), marcar_procesada(), marcar_integrada(), marcar_lista_para_uso()

    # ================================
    # 3.3 Utilidades para consulta por capa/estado
    # ================================

    def fuentes_por_layer(self, layer: DataLayer) -> List[FuenteDeDatos]:
        """
        Devuelve todas las fuentes cuyo estado actual está en la capa indicada.
        """
        result: List[FuenteDeDatos] = []
        for fid, estado in self._estados.items():
            if estado.layer == layer:
                result.append(self._fuentes[fid])
        return result

    def fuentes_por_estado(self, estado_buscar: DatasetState) -> List[FuenteDeDatos]:
        """
        Devuelve todas las fuentes cuyo estado actual coincide con estado_buscar.
        """
        result: List[FuenteDeDatos] = []
        for fid, estado in self._estados.items():
            if estado.estado == estado_buscar:
                result.append(self._fuentes[fid])
        return result

    # ================================
    # 3.4 Internos
    # ================================

    def _actualizar_estado(
        self,
        id_fuente: str,
        nuevo_estado: DatasetState,
        nuevo_layer: DataLayer,
        carpeta: Optional[Path],
        agente: Optional[str],
    ) -> None:
        """
        Actualiza estado actual y agrega entrada al historial.
        """
        estado = EstadoFuente(
            fuente_id=id_fuente,
            estado=nuevo_estado,
            layer=nuevo_layer,
            carpeta_actual=carpeta,
            agente_responsable=agente,
        )
        self._estados[id_fuente] = estado
        self._historial_estados.setdefault(id_fuente, []).append(estado)
