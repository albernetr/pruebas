# entorno_saludo.py
from dataclasses import dataclass

@dataclass
class PercepcionSaludo:
    nombre: str
    hora: int  # 0–23


class EntornoSaludo:
    """
    Entorno mínimo:
    - guarda el nombre del usuario
    - guarda la hora del día
    - ofrece percepciones al agente
    - ejecuta acciones (por ahora, imprimir el saludo)
    """

    def __init__(self, nombre_usuario: str, hora: int) -> None:
        self.nombre_usuario = nombre_usuario
        self.hora = hora

    def percibir(self) -> PercepcionSaludo:
        """
        Devuelve lo que el agente 've' del mundo.
        """
        return PercepcionSaludo(nombre=self.nombre_usuario, hora=self.hora)

    def aplicar_accion(self, mensaje_saludo: str) -> None:
        """
        Aplica la acción del agente. Aquí, simplemente mostramos el saludo.
        En un entorno más complejo, esto podría escribir a un log, enviar un mail, etc.
        """
        print(mensaje_saludo)
