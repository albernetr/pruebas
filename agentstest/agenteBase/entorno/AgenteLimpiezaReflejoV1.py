from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from entorno_mundo_v2 import EntornoMundoV2  # o como se llame tu archivo nuevo

# --- Percepción específica para el agente de limpieza ---

@dataclass
class PercepcionLimpieza:
    contratos_pendientes: List[str]  # ids de contratos pendientes de limpieza

class AgenteLimpiezaReflejoV1:
    """
    Agente reflejo simple especializado en tareas de LIMPIEZA de datasets.
    - Trabaja sobre EntornoMundoV2.
    - Consume ContratosDeTarea de tipo 'limpieza'.
    - Actualiza el estado del Dataset a 'clean' cuando termina.
    """

    def __init__(self, agente_id: str, entorno: EntornoMundoV2):
        self.id = agente_id
        self.entorno = entorno

    # ==========
    # 1. PERCEPCIÓN
    # ==========

    def percibir(self) -> PercepcionLimpieza:
        """
        Construye una percepción simple:
        - Lista de contratos de LIMPIEZA en estado 'pendiente'.
        """
        contratos_pendientes = [
            cid
            for cid, c in self.entorno.contratos.items()
            if c.estado == "pendiente" and c.tipo_tarea == "limpieza"
        ]
        return PercepcionLimpieza(contratos_pendientes=contratos_pendientes)

    # ==========
    # 2. DECISIÓN
    # ==========

    def decidir(self, percepcion: PercepcionLimpieza) -> Optional[str]:
        """
        Política de decisión reflejo:
        - Si hay al menos un contrato pendiente, toma el primero.
        - Si no hay nada, no actúa.
        """
        if not percepcion.contratos_pendientes:
            # No hay trabajo
            self.entorno.registrar_log(
                nivel="INFO",
                mensaje="AgenteLimpiezaReflejoV1: sin contratos de limpieza pendientes.",
                origen=self.id,
            )
            return None

        # Política más simple: FIFO
        contrato_id = percepcion.contratos_pendientes[0]
        return contrato_id

    # ==========
    # 3. ACCIÓN
    # ==========

    def actuar(self, contrato_id: str) -> None:
        """
        Ejecuta la limpieza asociada a un contrato:
        - Marca el contrato como 'en_progreso'.
        - Ejecuta una limpieza simulada.
        - Actualiza el dataset a estado 'clean'.
        - Marca el contrato como 'completado'.
        """

        contrato = self.entorno.contratos.get(contrato_id)
        if not contrato:
            self.entorno.registrar_log(
                nivel="ERROR",
                mensaje=f"Contrato {contrato_id} no existe.",
                origen=self.id,
            )
            return

        # Marcar contrato como asignado/en progreso a este agente
        if contrato.agente_asignado_id is None:
            self.entorno.asignar_contrato_a_agente(contrato_id, self.id)

        self.entorno.actualizar_estado_contrato(
            contrato_id=contrato_id,
            nuevo_estado="en_progreso",
        )

        dataset_id = contrato.dataset_id
        if not dataset_id or dataset_id not in self.entorno.datasets:
            self.entorno.registrar_log(
                nivel="ERROR",
                mensaje=f"Contrato {contrato_id} sin dataset válido.",
                origen=self.id,
            )
            self.entorno.actualizar_estado_contrato(
                contrato_id=contrato_id,
                nuevo_estado="fallido",
                resultado={"motivo": "dataset_invalido"},
            )
            return

        dataset = self.entorno.datasets[dataset_id]

        # ==============
        # AQUÍ IRÍA LA LÓGICA REAL DE LIMPIEZA
        # ==============
        # Por ahora, simulamos:
        resultado_limpieza: Dict[str, Any] = {
            "filas_antes": dataset.metadatos.get("filas", None),
            "filas_despues": dataset.metadatos.get("filas", None),  # igual, por ahora
            "duplicados_eliminados": 0,
            "nulos_imputados": 0,
            "resumen": "Limpieza simulada OK",
        }

        # Actualizar estado del dataset a 'clean'
        self.entorno.actualizar_estado_dataset(
            dataset_id=dataset_id,
            nuevo_estado="clean",
            nueva_ruta=dataset.ruta_fisica.replace("raw", "clean")
            if "raw" in dataset.ruta_fisica
            else dataset.ruta_fisica,
        )

        # Marcar contrato como completado
        self.entorno.actualizar_estado_contrato(
            contrato_id=contrato_id,
            nuevo_estado="completado",
            resultado=resultado_limpieza,
        )

        # Log + evento
        self.entorno.registrar_log(
            nivel="INFO",
            mensaje=f"Limpieza completada para dataset {dataset.nombre} (contrato {contrato_id})",
            origen=self.id,
            contexto={"dataset_id": dataset_id, "contrato_id": contrato_id},
        )

    # ==========
    # 4. CICLO SIMPLE
    # ==========

    def ciclo(self) -> None:
        """
        Un ciclo de percepción → decisión → acción.
        Esto es lo que llamarías en un bucle externo o scheduler.
        """
        percepcion = self.percibir()
        contrato_id = self.decidir(percepcion)

        if contrato_id is None:
            # Nada que hacer en este ciclo
            return

        self.actuar(contrato_id)
