# agente.py

from dataclasses import dataclass
from entorno_mundo import EntornoMundo, PercepcionMundo, ItemDeTrabajo

@dataclass
class AgenteProcesadorSimple:
    """
    Agente reflejo simple que:
    - si ve un item 'nuevo', lo marca como 'ok'
    - si no hay items 'nuevo', registra que no hay trabajo
    """
    nombre: str = "AgenteProcesadorSimple"

    def decidir_y_actuar(self, entorno: EntornoMundo) -> None:
        """
        Ejecuta un ciclo completo de:
        1. percibir el mundo,
        2. decidir qué hacer,
        3. actuar sobre el entorno.
        """
        # 1) El agente pide una percepción al entorno
        percepcion: PercepcionMundo = entorno.percibir()

        # 2) Decide qué hacer según la percepción
        item = percepcion.item_actual

        if item is not None:
            # Hay un ítem pendiente en estado 'nuevo'
            # 2.a) El agente "procesa" el ítem y decide que quedará en 'ok'
            nuevo_estado = "ok"
            detalle = f"Procesado por {self.nombre}"

            # 3.a) En vez de cambiar el item directo, pide al entorno que lo haga
            entorno.aplicar_cambio_estado(
                item_id=item.id,
                nuevo_estado=nuevo_estado,
                detalle=detalle,
            )

        else:
            # No hay ítems pendientes
            # 2.b) El agente decide registrar que no hay trabajo que hacer
            mensaje = f"{self.nombre}: no hay ítems en estado 'nuevo'"

            # 3.b) Delega en el entorno el registro del evento
            entorno.registrar_evento(
                tipo="sin_trabajo",
                detalle=mensaje,
            )

"""if __name__ == "__main__":
    # 1) Creamos el entorno y le agregamos algunos ítems
    entorno = EntornoMundo()
    entorno.agregar_item(ItemDeTrabajo(id="item-1", tipo="registro", estado="nuevo"))
    entorno.agregar_item(ItemDeTrabajo(id="item-2", tipo="registro", estado="ok"))

    # 2) Creamos el agente
    agente = AgenteProcesadorSimple(nombre="AgenteDemo")

    # 3) Ejecutamos algunos ciclos
    print("=== Ciclo 1 ===")
    agente.decidir_y_actuar(entorno)
    print("Estado items:", entorno.items)
    print("Eventos:", entorno.eventos)

    print("\n=== Ciclo 2 ===")
    agente.decidir_y_actuar(entorno)
    print("Estado items:", entorno.items)
    print("Eventos:", entorno.eventos)

"""
# Fin de agente.py