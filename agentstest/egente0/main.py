# main.py
from entorno_saludo import EntornoSaludo
from agente_saludo import AgenteSaludadorReflejo

def main() -> None:
    # Ejemplo: usuario "Leon", hora 10 (10 a.m.)
    entorno = EntornoSaludo(nombre_usuario="Leon", hora=10)
    agente = AgenteSaludadorReflejo()
    agente.ciclo(entorno)


if __name__ == "__main__":
    main()
