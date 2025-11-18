class AgenteStandardizer:
    """
    Agente que toma datasets en BASE, los estandariza
    y los mueve a STANDARDIZED.
    """

    def decidir_y_actuar(self, entorno: EntornoDatosMAS) -> None:
        percepcion = entorno.percibir(capas_interes=[CapaDatos.BASE])

        for meta in percepcion.datasets_disponibles:
            if meta.estado == EstadoDataset.INGESTADO:
                # 1) Leer archivo, detectar esquema estándar (fuera del entorno)
                # 2) Escribir a nueva ruta en 04_standardized
                nueva_ruta = Path(
                    entorno.raiz_pipeline,
                    CapaDatos.STANDARDIZED.value,
                    f"{meta.id.nombre_logico}.parquet",
                )
                # 3) Avisar al entorno del cambio de capa
                entorno.mover_dataset_a_capa(
                    dataset_id=meta.id,
                    nueva_capa=CapaDatos.STANDARDIZED,
                    nueva_ruta=nueva_ruta,
                )
                entorno.marcar_dataset_completo(meta.id)
