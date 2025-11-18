from dataclasses import dataclass, field
from typing import Literal
from typing import Optional
from dataclasses import dataclass
from typing import List

# 1) Definimos los valores permitidos para el estado de un ítem
EstadoItem = Literal["nuevo", "procesando", "ok", "fallido"]


# 2) Definimos un "contenedor de datos" para un ítem de trabajo
@dataclass
class ItemDeTrabajo:

    """
    Representa un objeto sobre el que pueden trabajar los agentes.
    """
    id: str           # Identificador único dentro del entorno
    tipo: str         # Tipo de ítem (por ejemplo: 'registro', 'archivo', etc.)
    estado: EstadoItem
    detalle: str = "" # Información adicional (opcional)

    def __post_init__(self):
        if self.estado not in ("nuevo", "procesando", "ok", "fallido"):
            raise ValueError(f"Estado inválido: {self.estado}")
        
@dataclass
class EventoDominio:
    """
    Representa algo relevante que ocurrió en el entorno.
    Puede ser usado para logs, monitoreo o para que otros agentes lo lean.
    """
    tipo: str     # Nombre del evento (ej: 'estado_actualizado', 'sin_trabajo')
    detalle: str  # Descripción humana o técnica del evento

@dataclass
class PercepcionMundo:
    """
    Lo que un agente ve del entorno en un instante dado.
    Esta es una 'foto parcial' del mundo, no todo el estado completo.
    """
    item_actual: Optional[ItemDeTrabajo]  # Un ítem pendiente a procesar, o None si no hay
    total_items: int                      # Cuántos ítems hay en total
    total_pendientes: int                 # Cuántos ítems están en estado 'nuevo'


@dataclass
class EntornoMundo:

    """
    Representa el 'mundo' sobre el que actúan los agentes.
    Es el único dueño del estado global (ítems y eventos).
    """
    items: List[ItemDeTrabajo] = field(default_factory=list)
    eventos: List[EventoDominio] = field(default_factory=list)

    def percibir(self) -> PercepcionMundo:
        """
        Construye y devuelve una percepción para un agente.
        Regla simple: si hay ítems en estado 'nuevo', entrega el primero.
        """
        # 1) Filtramos los ítems pendientes
        pendientes = [it for it in self.items if it.estado == "nuevo"]

        # 2) Elegimos el 'item_actual' si hay alguno
        item_actual = pendientes[0] if pendientes else None

        # 3) Construimos la percepción
        percepcion = PercepcionMundo(
            item_actual=item_actual,
            total_items=len(self.items),
            total_pendientes=len(pendientes),
        )
        return percepcion

    def aplicar_cambio_estado(self, item_id: str, nuevo_estado: EstadoItem, detalle: str = "") -> None:
        """
        Punto único para cambiar el estado de un ítem del mundo.
        Un agente NO debe modificar 'items' directamente, siempre pasa por aquí.
        """
        for it in self.items:
            if it.id == item_id:
                # 1) Cambiamos el estado del ítem
                it.estado = nuevo_estado

                # 2) Actualizamos el detalle si se proporcionó uno
                if detalle:
                    it.detalle = detalle

                # 3) Registramos un evento de dominio para dejar rastro
                mensaje = f"Item {item_id} cambió a estado '{nuevo_estado}'"
                self.eventos.append(
                    EventoDominio(
                        tipo="estado_actualizado",
                        detalle=mensaje,
                    )
                )
                break

    def registrar_evento(self, tipo: str, detalle: str) -> None:
        """
        Registra algo relevante que ocurrió en el entorno,
        aun cuando no se haya cambiado el estado de ningún ítem.
        """
        self.eventos.append(
            EventoDominio(
                tipo=tipo,
                detalle=detalle,
            )
        )

    def agregar_item(self, item: ItemDeTrabajo) -> None:
        """
        Agrega un nuevo ítem al entorno.
        Útil para inicializar el mundo antes de que actúen los agentes.
        """
        self.items.append(item)
        self.eventos.append(
            EventoDominio(
                tipo="item_agregado",
                detalle=f"Se agregó el ítem {item.id} de tipo '{item.tipo}' con estado '{item.estado}'",
            )
        )






