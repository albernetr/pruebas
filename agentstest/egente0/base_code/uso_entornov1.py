from pathlib import Path

entorno = FEIDEnvironment(root=Path("/ruta/feid_proyecto"))

# 1) Registrar una fuente externa
fuente = entorno.registrar_fuente_externa(
    id_fuente="clientes_erp",
    nombre="Clientes ERP X",
    tipo_logico=TipoFuenteLogica.BD,
    origen_sistema="ERP_X",
    ubicacion_externa="sqlserver://erp-x:1433;db=erp",
)

# 2) Un agente de ingesta la trae a 01_ingest/
loc_ingest = entorno.materializar_en_ingest("clientes_erp", subcarpeta="erp/clientes", agente="AgenteIngestaBD")

# 3) Otro agente la mueve a 02_raw/
loc_raw = entorno.mover_a_raw("clientes_erp", subcarpeta="erp/clientes", agente="AgenteRawManager")

# 4) Se registra como dataset en 03_base/
loc_base = entorno.registrar_en_base("clientes_erp", nombre_dataset="clientes_raw", proyecto="proyecto_demo", agente="AgenteBaseManager")

# 5) Más tarde, un agente de estandarización hace:
loc_std = entorno.marcar_estandarizada("clientes_erp", subcarpeta="clientes_std", agente="AgenteEstandarizador")
