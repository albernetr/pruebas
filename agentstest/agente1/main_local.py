# main_local.py
from entorno_mundo import EntornoMundo, ItemDeTrabajo
from agente import AgenteProcesadorSimple


def main():
    entorno = EntornoMundo()
    entorno.agregar_item(ItemDeTrabajo(id="item-1", tipo="registro", estado="nuevo"))
    entorno.agregar_item(ItemDeTrabajo(id="item-2", tipo="registro", estado="ok"))

    agente = AgenteProcesadorSimple(nombre="AgenteDemo")

    print("=== Ciclo 1 ===")
    agente.decidir_y_actuar(entorno)
    print("Estado items:", entorno.items)
    print("Eventos:", entorno.eventos)

    print("\n=== Ciclo 2 ===")
    agente.decidir_y_actuar(entorno)
    print("Estado items:", entorno.items)
    print("Eventos:", entorno.eventos)


if __name__ == "__main__":
    main()
