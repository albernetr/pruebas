# test_limpieza.py

from entorno_mundo_v2 import (
    EntornoMundoV2,
    Proyecto,
    generar_id,
)

from AgenteLimpiezaReflejoV1 import AgenteLimpiezaReflejoV1



def demo_agente_limpieza():
    entorno = EntornoMundoV2()

    # 1) Crear un proyecto y un dataset de ejemplo
    proyecto = Proyecto(
        id=generar_id("prj"),
        nombre="Proyecto FEID Demo",
        objetivo="Probar agente de limpieza",
        owner="Leon",
    )
    entorno.proyectos[proyecto.id] = proyecto

    ds = entorno.registrar_dataset(
        nombre="ventas_raw_2025_11_17",
        proyecto_id=proyecto.id,
        caso_estudio_id=None,
        fuente_id=None,
        ruta_fisica="02_raw/ventas/ventas_2025_11_17.csv",
        estado="raw",
    )
    ds.metadatos["filas"] = 1000  # simulación

    # 2) Crear contrato de limpieza
    contrato = entorno.crear_contrato_tarea(
        descripcion="Limpieza básica del dataset de ventas",
        tipo_tarea="limpieza",
        dataset_id=ds.id,
        parametros={"reglas": ["eliminar_duplicados", "imputar_nulos_basicos"]},
    )

    # 3) Crear el agente y ejecutar un ciclo
    agente = AgenteLimpiezaReflejoV1(
        agente_id="agente_limpieza_1",
        entorno=entorno,
    )
    agente.ciclo()

    # 4) Ver resultados en consola
    print("Estado final del dataset:", entorno.datasets[ds.id].estado)
    print("Estado del contrato:", entorno.contratos[contrato.id].estado)
    print("Resultado del contrato:", entorno.contratos[contrato.id].resultado)

    print("\nLogs:")
    for log in entorno.logs:
        print(f"[{log.nivel}] {log.mensaje} (origen={log.origen})")


if __name__ == "__main__":
    demo_agente_limpieza()
