# 2.2. Identificadores y metadatos de dataset

@dataclass(frozen=True)
class DatasetId:
    proyecto_id: str
    nombre_logico: str        # p.ej. "ventas_diarias", "sensores_zona"
    version: str = "v1"       # permite versionado

    def as_key(self) -> str:
        return f"{self.proyecto_id}:{self.nombre_logico}:{self.version}"


@dataclass
class DatasetMetadata:
    id: DatasetId
    capa_actual: CapaDatos
    estado: EstadoDataset
    ruta_fisica: Path

    # Metadatos opcionales / extensibles
    esquema: Optional[Dict[str, str]] = None       # col -> tipo lógico
    filas_aprox: Optional[int] = None
    columnas: Optional[List[str]] = None
    ultima_actualizacion: Optional[datetime] = None
    tags: Dict[str, str] = field(default_factory=dict)


# 2.3. Slice de columna para Teorema5
@dataclass
class ColumnSlice:
    """
    Vista columna-centrada: materialización operativa de Teorema5.
    Representa una columna de un dataset en una capa específica.
    """
    dataset_id: DatasetId
    capa: CapaDatos
    nombre_columna: str
    ruta_columna: Path    # donde se almacenan las tripletas (idFila, dataset, valor[, t])

# 2.4. Eventos del entorn

class TipoEventoEntorno(Enum):
    DATASET_CREADO = auto()
    DATASET_MOVIDO = auto()
    DATASET_ACTUALIZADO = auto()
    DATASET_ERROR = auto()
    COLUMNA_EXTRAIDA = auto()
    COLUMNA_RECONSTRUIDA = auto()
    # luego: MENSAJE_AGENTE, CONTRATO_EMITIDO, etc.


@dataclass
class EventoEntorno:
    tipo: TipoEventoEntorno
    timestamp: datetime
    descripcion: str
    dataset_id: Optional[DatasetId] = None
    payload: Dict[str, Any] = field(default_factory=dict)


#

#

#
#
