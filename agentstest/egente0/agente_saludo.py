# agente_saludo.py
from dataclasses import dataclass
from entorno_saludo import PercepcionSaludo, EntornoSaludo

@dataclass
class AccionSaludo:
    mensaje: str


class AgenteSaludadorReflejo:
    """
    Agente reflejo simple:
    - No tiene memoria
    - La acción depende solo de la percepción actual
    """

    def percibir(self, entorno: EntornoSaludo) -> PercepcionSaludo:
        return entorno.percibir()

    def decidir(self, percepcion: PercepcionSaludo) -> AccionSaludo:
        """
        Regla de decisión:
        - 5–11: Buenos días
        - 12–18: Buenas tardes
        - resto: Buenas noches
        """
        hora = percepcion.hora

        if 5 <= hora <= 11:
            saludo_base = "Buenos días"
        elif 12 <= hora <= 18:
            saludo_base = "Buenas tardes"
        else:
            saludo_base = "Buenas noches"

        mensaje = f"{saludo_base}, {percepcion.nombre} 👋"
        return AccionSaludo(mensaje=mensaje)

    def actuar(self, accion: AccionSaludo, entorno: EntornoSaludo) -> None:
        entorno.aplicar_accion(accion.mensaje)

    def ciclo(self, entorno: EntornoSaludo) -> None:
        """
        Un ciclo completo:
        Percibir -> Decidir -> Actuar
        """
        percepcion = self.percibir(entorno)
        accion = self.decidir(percepcion)
        self.actuar(accion, entorno)
