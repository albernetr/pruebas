# entorno/entorno_datos_mas.py

"""3. Clase central: EntornoDatosMAS

Esta será la “fachada” que los agentes van a usar para:

percibir el estado del mundo (datasets, capas, columnas, eventos),

consultar qué hay disponible,

proponer acciones (que luego el entorno ejecuta)."""

#3.1. Estructura interna mínima

class EntornoDatosMAS:
    """
    Entorno técnico para la plataforma multiagente FEID/EFDI.

    - Mantiene un registro en memoria de los datasets conocidos y sus estados.
    - Expone métodos de consulta/percepción para los agentes.
    - Expone un método 'aplicar_accion' para cambiar el entorno de forma controlada.
    - Tiene soporte para el espacio Teorema5 (columnas).
    """

    def __init__(self, raiz_pipeline: Path):
        self.raiz_pipeline = raiz_pipeline

        # Registro en memoria de datasets: key -> metadata
        self._datasets: Dict[str, DatasetMetadata] = {}

        # Registro de ColumnSlices (Teorema5)
        self._column_slices: Dict[str, ColumnSlice] = {}

        # Historial de eventos (simple por ahora)
        self._eventos: List[EventoEntorno] = []

# 3.2. Registro y consulta de datasets

    # ---------- Registro de datasets ----------

    def registrar_dataset(self, meta: DatasetMetadata) -> None:
        """Registra un dataset nuevo en el entorno."""
        key = meta.id.as_key()
        self._datasets[key] = meta
        self._registrar_evento(
            TipoEventoEntorno.DATASET_CREADO,
            f"Dataset registrado en capa {meta.capa_actual.value}",
            meta.id,
        )

    def obtener_dataset(self, dataset_id: DatasetId) -> Optional[DatasetMetadata]:
        return self._datasets.get(dataset_id.as_key())

    def listar_datasets_por_capa(self, capa: CapaDatos) -> List[DatasetMetadata]:
        return [d for d in self._datasets.values() if d.capa_actual == capa]

    def listar_todos_los_datasets(self) -> List[DatasetMetadata]:
        return list(self._datasets.values())

"""3.3. Movimiento entre capas del pipeline

Esto es lo que un agente de “pipeline” podría pedir como acción:
mover dataset de BASE a STANDARDIZED, de STANDARDIZED a CLEAN, etc."""

    # ---------- Cambio de capa / estado ----------

def mover_dataset_a_capa(
        self,
        dataset_id: DatasetId,
        nueva_capa: CapaDatos,
        nueva_ruta: Optional[Path] = None,
    ) -> None:
        """
        Actualiza la capa de un dataset. La lógica de mover el archivo físico
        (copiar/mover en disco, escribir Parquet, etc.) va fuera o se integra luego.
        """
        meta = self.obtener_dataset(dataset_id)
        if not meta:
            raise ValueError(f"Dataset no encontrado: {dataset_id.as_key()}")

        capa_original = meta.capa_actual
        meta.capa_actual = nueva_capa
        meta.estado = EstadoDataset.EN_PROCESO  # o COMPLETO según el caso
        if nueva_ruta:
            meta.ruta_fisica = nueva_ruta
        meta.ultima_actualizacion = datetime.utcnow()

        self._registrar_evento(
            TipoEventoEntorno.DATASET_MOVIDO,
            f"Dataset {dataset_id.as_key()} movido de {capa_original.value} a {nueva_capa.value}",
            dataset_id,
            payload={
                "capa_origen": capa_original.value,
                "capa_destino": nueva_capa.value,
            },
        )


def marcar_dataset_completo(self, dataset_id: DatasetId) -> None:
    meta = self.obtener_dataset(dataset_id)
    if not meta:
        raise ValueError(f"Dataset no encontrado: {dataset_id.as_key()}")
    meta.estado = EstadoDataset.COMPLETO
    meta.ultima_actualizacion = datetime.utcnow()
    self._registrar_evento(
        TipoEventoEntorno.DATASET_ACTUALIZADO,
        f"Dataset {dataset_id.as_key()} marcado como COMPLETO",
        dataset_id,
    )

"""3.4. Soporte para Teorema5: extracción de columnas

Aquí definimos cómo el entorno registra el “espacio columna”.
La lógica de leer/escribir columnas la puedes implementar después, pero el contrato técnico ya queda claro."""

    # ---------- Teorema5: columnas como objetos de primera clase ----------

def registrar_column_slice(
    self,
    dataset_id: DatasetId,
    capa: CapaDatos,
    nombre_columna: str,
    ruta_columna: Path,
) -> ColumnSlice:
    """
    Registra un ColumnSlice (vista columna) en el entorno.
    Puede ser llamado por un agente de descomposición de columnas.
    """
    slice_obj = ColumnSlice(
        dataset_id=dataset_id,
        capa=capa,
        nombre_columna=nombre_columna,
        ruta_columna=ruta_columna,
    )
    key = self._key_column_slice(slice_obj)
    self._column_slices[key] = slice_obj

    self._registrar_evento(
        TipoEventoEntorno.COLUMNA_EXTRAIDA,
        f"Columna {nombre_columna} extraída de {dataset_id.as_key()} en {capa.value}",
        dataset_id,
        payload={
            "columna": nombre_columna,
            "ruta_columna": str(ruta_columna),
        },
    )
    return slice_obj

def listar_column_slices(
    self,
    dataset_id: Optional[DatasetId] = None,
    capa: Optional[CapaDatos] = None,
) -> List[ColumnSlice]:
    res = list(self._column_slices.values())
    if dataset_id:
        res = [c for c in res if c.dataset_id == dataset_id]
    if capa:
        res = [c for c in res if c.capa == capa]
    return res

@staticmethod
def _key_column_slice(slice_obj: ColumnSlice) -> str:
    return f"{slice_obj.dataset_id.as_key()}:{slice_obj.capa.value}:{slice_obj.nombre_columna}"

"""3.5. Eventos y percepciones para agentes

Ahora definimos cómo el entorno registra eventos y cómo un agente los puede percibir."""

# ---------- Eventos ----------

def _registrar_evento(
    self,
    tipo: TipoEventoEntorno,
    descripcion: str,
    dataset_id: Optional[DatasetId] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    ev = EventoEntorno(
        tipo=tipo,
        timestamp=datetime.utcnow(),
        descripcion=descripcion,
        dataset_id=dataset_id,
        payload=payload or {},
    )
    self._eventos.append(ev)

def listar_eventos(
    self,
    tipo: Optional[TipoEventoEntorno] = None,
    desde: Optional[datetime] = None,
) -> List[EventoEntorno]:
    eventos = self._eventos
    if tipo:
        eventos = [e for e in eventos if e.tipo == tipo]
    if desde:
        eventos = [e for e in eventos if e.timestamp >= desde]
    return eventos


"""3.6. Percepción resumida para un agente

Podemos definir un objeto “percepción” que el agente leerá:"""

@dataclass
class PercepcionEntorno:
    timestamp: datetime
    datasets_disponibles: List[DatasetMetadata]
    eventos_recientes: List[EventoEntorno]
    columnas_disponibles: List[ColumnSlice]

    # ---------- Percepción para agentes ----------

def percibir(
    self,
    capas_interes: Optional[List[CapaDatos]] = None,
    desde_evento: Optional[datetime] = None,
) -> PercepcionEntorno:
    """
    Devuelve una 'foto' del entorno relevante para un agente:
    - datasets en capas de interés,
    - eventos recientes,
    - columnas registradas (Teorema5) asociadas.
    """
    if capas_interes:
        datasets = [
            d for d in self._datasets.values()
            if d.capa_actual in capas_interes
        ]
    else:
        datasets = list(self._datasets.values())

    eventos = self.listar_eventos(desde=desde_evento)
    columnas = list(self._column_slices.values())

    return PercepcionEntorno(
        timestamp=datetime.utcnow(),
        datasets_disponibles=datasets,
        eventos_recientes=eventos,
        columnas_disponibles=columnas,
    )
