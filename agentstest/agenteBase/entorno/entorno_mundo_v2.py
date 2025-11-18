from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Literal, Any
import uuid


# ==========
# Utilidades
# ==========

def generar_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@dataclass
class BaseEntidad:
    id: str
    creado_en: datetime = field(default_factory=datetime.utcnow)
    actualizado_en: datetime = field(default_factory=datetime.utcnow)

    def touch(self) -> None:
        self.actualizado_en = datetime.utcnow()


# ==========================
# 1. PROYECTO / CASO ESTUDIO
# ==========================

@dataclass
class Proyecto(BaseEntidad):
    nombre: str = ""
    objetivo: str = ""
    owner: str = ""
    descripcion: str = ""
    casos_estudio_ids: List[str] = field(default_factory=list)


@dataclass
class CasoDeEstudio(BaseEntidad):
    proyecto_id: str = ""
    nombre: str = ""
    descripcion: str = ""
    configuracion_id: Optional[str] = None  # ConfiguraciónProyecto
    fuentes_ids: List[str] = field(default_factory=list)
    datasets_ids: List[str] = field(default_factory=list)


@dataclass
class ConfiguracionProyecto(BaseEntidad):
    proyecto_id: str = ""
    # Aquí puedes guardar tu betaid_setup / FEID config completa
    data: Dict[str, Any] = field(default_factory=dict)


# ==========================
# 2. FUENTES / CONECTORES
# ==========================

TipoFuente = Literal["archivo", "bd", "api", "stream", "iot"]


@dataclass
class FuenteDeDatos(BaseEntidad):
    nombre: str = ""
    tipo: TipoFuente = "archivo"
    descripcion: str = ""
    conector_id: Optional[str] = None


@dataclass
class ConectorFuente(BaseEntidad):
    nombre: str = ""
    tipo: str = ""        # "postgres", "s3", "rest", etc.
    endpoint: str = ""
    parametros: Dict[str, Any] = field(default_factory=dict)
    activo: bool = True


# ==========================
# 3. DATASETS / ESTADOS
# ==========================

EstadoDataset = Literal[
    "ingest",
    "raw",
    "base",
    "standardized",
    "clean",
    "enriched",
    "processed",
    "fusion",
    "ready",
]


@dataclass
class Dataset(BaseEntidad):
    nombre: str = ""
    proyecto_id: str = ""
    caso_estudio_id: Optional[str] = None
    fuente_id: Optional[str] = None

    ruta_fisica: str = ""         # Ej: "03_base/ventas/2025-11-17.parquet"
    estado: EstadoDataset = "ingest"

    esquema_id: Optional[str] = None
    diccionario_id: Optional[str] = None

    # espacio para metadatos adicionales
    metadatos: Dict[str, Any] = field(default_factory=dict)


# ==========================
# 4. CALIDAD / PERFIL / INDICADORES
# ==========================

SeveridadRegla = Literal["info", "warning", "error"]


@dataclass
class ReglaDeCalidadDeDatos(BaseEntidad):
    nombre: str = ""
    descripcion: str = ""
    dataset_id: str = ""
    expresion: str = ""  # SQL, DSL, expresión python serializada, etc.
    severidad: SeveridadRegla = "warning"
    umbral: Optional[float] = None  # ej. % máximo de nulos
    activa: bool = True


@dataclass
class PerfilDeDatos(BaseEntidad):
    dataset_id: str = ""
    columna: Optional[str] = None  # None si es global
    estadisticas: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IndicadorDeCalidad(BaseEntidad):
    nombre: str = ""
    descripcion: str = ""
    dataset_id: Optional[str] = None
    valor: float = 0.0
    unidad: str = ""
    tags: List[str] = field(default_factory=list)


# ==========================
# 5. LOGS / EJECUCIÓN
# ==========================

NivelLog = Literal["INFO", "WARN", "ERROR"]


@dataclass
class LogDeEjecucion(BaseEntidad):
    nivel: NivelLog = "INFO"
    mensaje: str = ""
    origen: str = ""          # "agente_X", "pipeline_Y"
    contexto: Dict[str, Any] = field(default_factory=dict)


# ==========================
# 6. MAS: MENSAJES / EVENTOS / CONTRATOS
# ==========================

ActoDeHabla = Literal[
    "inform",
    "request",
    "query",
    "propose",
    "accept",
    "refuse",
    "failure",
]


@dataclass
class MensajeMAS(BaseEntidad):
    emisor: str = ""             # id del agente
    receptor: str = ""           # id del agente o "entorno"
    acto: ActoDeHabla = "inform"
    contenido: Dict[str, Any] = field(default_factory=dict)
    conversation_id: Optional[str] = None


@dataclass
class EventoDeEntorno(BaseEntidad):
    tipo: str = ""               # "dataset_creado", "calidad_fallida", etc.
    descripcion: str = ""
    datos: Dict[str, Any] = field(default_factory=dict)


EstadoContrato = Literal[
    "pendiente",
    "asignado",
    "en_progreso",
    "completado",
    "fallido",
]


@dataclass
class ContratoDeTarea(BaseEntidad):
    descripcion: str = ""
    dataset_id: Optional[str] = None
    tipo_tarea: Literal[
        "adquisicion",
        "limpieza",
        "transformacion",
        "fusion",
        "validacion_calidad",
        "otro",
    ] = "otro"
    parametros: Dict[str, Any] = field(default_factory=dict)

    agente_asignado_id: Optional[str] = None
    estado: EstadoContrato = "pendiente"
    resultado: Optional[Dict[str, Any]] = None


@dataclass
class EstadoDelEntorno(BaseEntidad):
    resumen_datasets: Dict[str, Any] = field(default_factory=dict)
    resumen_calidad: Dict[str, Any] = field(default_factory=dict)
    resumen_contratos: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EntornoMundoV2:
    # Colecciones principales
    proyectos: Dict[str, Proyecto] = field(default_factory=dict)
    casos_estudio: Dict[str, CasoDeEstudio] = field(default_factory=dict)
    configuraciones: Dict[str, ConfiguracionProyecto] = field(default_factory=dict)

    fuentes: Dict[str, FuenteDeDatos] = field(default_factory=dict)
    conectores: Dict[str, ConectorFuente] = field(default_factory=dict)
    datasets: Dict[str, Dataset] = field(default_factory=dict)

    reglas_calidad: Dict[str, ReglaDeCalidadDeDatos] = field(default_factory=dict)
    perfiles_datos: Dict[str, PerfilDeDatos] = field(default_factory=dict)
    indicadores_calidad: Dict[str, IndicadorDeCalidad] = field(default_factory=dict)

    logs: List[LogDeEjecucion] = field(default_factory=list)
    mensajes: List[MensajeMAS] = field(default_factory=list)
    eventos: List[EventoDeEntorno] = field(default_factory=list)
    contratos: Dict[str, ContratoDeTarea] = field(default_factory=dict)

    estado_entorno: Optional[EstadoDelEntorno] = None

    # ==========
    # Métodos de ayuda para crear/actualizar entidades
    # ==========

    # --- DATASETS & PIPELINE ---

    def registrar_dataset(
        self,
        nombre: str,
        proyecto_id: str,
        caso_estudio_id: Optional[str],
        fuente_id: Optional[str],
        ruta_fisica: str,
        estado: EstadoDataset = "ingest",
    ) -> Dataset:
        dataset_id = generar_id("ds")
        ds = Dataset(
            id=dataset_id,
            nombre=nombre,
            proyecto_id=proyecto_id,
            caso_estudio_id=caso_estudio_id,
            fuente_id=fuente_id,
            ruta_fisica=ruta_fisica,
            estado=estado,
        )
        self.datasets[dataset_id] = ds

        # Actualizar caso de estudio si aplica
        if caso_estudio_id and caso_estudio_id in self.casos_estudio:
            self.casos_estudio[caso_estudio_id].datasets_ids.append(dataset_id)

        # Evento de entorno
        self.registrar_evento(
            tipo="dataset_creado",
            descripcion=f"Dataset {nombre} creado en estado {estado}",
            datos={"dataset_id": dataset_id, "estado": estado},
        )

        return ds

    def actualizar_estado_dataset(
        self,
        dataset_id: str,
        nuevo_estado: EstadoDataset,
        nueva_ruta: Optional[str] = None,
    ) -> None:
        ds = self.datasets.get(dataset_id)
        if not ds:
            raise ValueError(f"Dataset {dataset_id} no existe")

        ds.estado = nuevo_estado
        if nueva_ruta is not None:
            ds.ruta_fisica = nueva_ruta
        ds.touch()

        self.registrar_evento(
            tipo="dataset_estado_actualizado",
            descripcion=f"Dataset {ds.nombre} pasa a {nuevo_estado}",
            datos={"dataset_id": dataset_id, "estado": nuevo_estado},
        )

    # --- CONTRATOS / TAREAS ---

    def crear_contrato_tarea(
        self,
        descripcion: str,
        tipo_tarea: Literal[
            "adquisicion",
            "limpieza",
            "transformacion",
            "fusion",
            "validacion_calidad",
            "otro",
        ],
        dataset_id: Optional[str] = None,
        parametros: Optional[Dict[str, Any]] = None,
    ) -> ContratoDeTarea:
        contrato_id = generar_id("ct")
        contrato = ContratoDeTarea(
            id=contrato_id,
            descripcion=descripcion,
            dataset_id=dataset_id,
            tipo_tarea=tipo_tarea,
            parametros=parametros or {},
        )
        self.contratos[contrato_id] = contrato

        self.registrar_evento(
            tipo="contrato_creado",
            descripcion=f"Contrato {contrato_id} creado para tarea {tipo_tarea}",
            datos={"contrato_id": contrato_id, "tipo_tarea": tipo_tarea},
        )

        return contrato

    def asignar_contrato_a_agente(
        self,
        contrato_id: str,
        agente_id: str,
    ) -> None:
        contrato = self.contratos.get(contrato_id)
        if not contrato:
            raise ValueError(f"Contrato {contrato_id} no existe")

        contrato.agente_asignado_id = agente_id
        contrato.estado = "asignado"
        contrato.touch()

        self.registrar_evento(
            tipo="contrato_asignado",
            descripcion=f"Contrato {contrato_id} asignado a {agente_id}",
            datos={"contrato_id": contrato_id, "agente_id": agente_id},
        )

    def actualizar_estado_contrato(
        self,
        contrato_id: str,
        nuevo_estado: EstadoContrato,
        resultado: Optional[Dict[str, Any]] = None,
    ) -> None:
        contrato = self.contratos.get(contrato_id)
        if not contrato:
            raise ValueError(f"Contrato {contrato_id} no existe")

        contrato.estado = nuevo_estado
        if resultado is not None:
            contrato.resultado = resultado
        contrato.touch()

        self.registrar_evento(
            tipo="contrato_actualizado",
            descripcion=f"Contrato {contrato_id} pasa a {nuevo_estado}",
            datos={"contrato_id": contrato_id, "estado": nuevo_estado},
        )

    # --- EVENTOS / LOGS / MENSAJES ---

    def registrar_evento(
        self,
        tipo: str,
        descripcion: str,
        datos: Optional[Dict[str, Any]] = None,
    ) -> EventoDeEntorno:
        ev = EventoDeEntorno(
            id=generar_id("ev"),
            tipo=tipo,
            descripcion=descripcion,
            datos=datos or {},
        )
        self.eventos.append(ev)
        return ev

    def registrar_log(
        self,
        nivel: NivelLog,
        mensaje: str,
        origen: str,
        contexto: Optional[Dict[str, Any]] = None,
    ) -> LogDeEjecucion:
        log = LogDeEjecucion(
            id=generar_id("log"),
            nivel=nivel,
            mensaje=mensaje,
            origen=origen,
            contexto=contexto or {},
        )
        self.logs.append(log)
        return log

    def enviar_mensaje(
        self,
        emisor: str,
        receptor: str,
        acto: ActoDeHabla,
        contenido: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[str] = None,
    ) -> MensajeMAS:
        msg = MensajeMAS(
            id=generar_id("msg"),
            emisor=emisor,
            receptor=receptor,
            acto=acto,
            contenido=contenido or {},
            conversation_id=conversation_id,
        )
        self.mensajes.append(msg)
        return msg

    # --- PERCEPCIÓN PARA AGENTES ---

    def percepcion_resumen(self) -> EstadoDelEntorno:
        """
        Construye un snapshot simple del entorno para que lo consuma un agente.
        """
        resumen_datasets = {
            "total": len(self.datasets),
            "por_estado": {},
        }
        for ds in self.datasets.values():
            resumen_datasets["por_estado"].setdefault(ds.estado, 0)
            resumen_datasets["por_estado"][ds.estado] += 1

        resumen_contratos = {
            "total": len(self.contratos),
            "por_estado": {},
        }
        for ct in self.contratos.values():
            resumen_contratos["por_estado"].setdefault(ct.estado, 0)
            resumen_contratos["por_estado"][ct.estado] += 1

        estado = EstadoDelEntorno(
            id=generar_id("st"),
            resumen_datasets=resumen_datasets,
            resumen_calidad={},  # se puede rellenar más adelante
            resumen_contratos=resumen_contratos,
            extra={},
        )
        self.estado_entorno = estado
        return estado

