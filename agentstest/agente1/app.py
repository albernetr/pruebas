# app.py
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

from entorno_mundo import EntornoMundo, ItemDeTrabajo, EstadoItem
from agente import AgenteProcesadorSimple

# ==========================
# 1. Instancias globales (long-lived)
# ==========================

entorno = EntornoMundo()
agente = AgenteProcesadorSimple(nombre="AgenteServidor")

# ==========================
# 2. FastAPI app
# ==========================

app = FastAPI(title="Agente + Entorno Demo")


# ==========================
# 3. Modelo de entrada (mundo externo → dominio)
# ==========================

class ItemIn(BaseModel):
    id: str
    tipo: str
    estado: EstadoItem  # reutilizamos el mismo Literal


# ==========================
# 4. Bucle del agente (tarea de fondo)
# ==========================

async def bucle_agente():
    """
    Bucle infinito donde el agente:
    - percibe el entorno,
    - decide,
    - actúa,
    - descansa un poco.
    """
    while True:
        agente.decidir_y_actuar(entorno)
        await asyncio.sleep(10.0)  # 1 segundo entre ciclos (ajustable)


@app.on_event("startup")
async def on_startup():
    """
    Cuando arranca el servidor FastAPI:
    - lanzamos el bucle del agente en segundo plano.
    """
    asyncio.create_task(bucle_agente())


# ==========================
# 5. Endpoints HTTP
# ==========================

@app.post("/items")
async def crear_item(item_in: ItemIn):
    """
    Recibe un ítem desde el mundo externo y lo agrega al entorno.
    """
    item = ItemDeTrabajo(
        id=item_in.id,
        tipo=item_in.tipo,
        estado=item_in.estado,
    )
    entorno.agregar_item(item)
    return {"status": "ok", "message": f"Item {item.id} agregado al entorno"}


@app.get("/items")
async def listar_items():
    """
    Devuelve el estado actual de los ítems del entorno.
    """
    return {
        "items": [
            {
                "id": it.id,
                "tipo": it.tipo,
                "estado": it.estado,
                "detalle": it.detalle,
            }
            for it in entorno.items
        ]
    }


@app.get("/eventos")
async def listar_eventos():
    """
    Devuelve el historial de eventos registrados en el entorno.
    """
    return {
        "eventos": [
            {
                "tipo": ev.tipo,
                "detalle": ev.detalle,
            }
            for ev in entorno.eventos
        ]
    }
