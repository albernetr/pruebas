
"""
fechas_utiles.py
-----------------
Utilidades para calcular meses completos y días restantes entre rangos de fechas.

Funciones expuestas:
  - meses_y_dias_en_intervalo(inicio: date, fin: date, *, fin_inclusivo: bool = True) -> tuple[int, int]
  - meses_y_dias_en_anio(inicio: date, fin: date, anio: int, *, fin_inclusivo: bool = True) -> tuple[int, int]
  - meses_y_dias_en_rango(inicio_str: str, fin_str: str, formato: str = "%m/%d/%Y", *, fin_inclusivo: bool = True) -> tuple[int, int]
  - meses_y_dias_en_anio_desde_str(inicio_str: str, fin_str: str, anio: int, formato: str = "%m/%d/%Y", *, fin_inclusivo: bool = True) -> tuple[int, int]
"""
from __future__ import annotations
from datetime import datetime, date, timedelta

__all__ = [
    'ultimo_dia_mes',
    'add_months',
    'meses_y_dias_en_intervalo',
    'meses_y_dias_en_anio',
    'meses_y_dias_en_rango',
    'meses_y_dias_en_anio_desde_str'
]

def ultimo_dia_mes(y: int, m: int) -> date:
    """Devuelve el último día del mes (y, m)."""
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)

def add_months(d: date, months: int) -> date:
    """
    Suma 'months' meses a la fecha d. Si el día no existe en el mes destino,
    ajusta al último día de ese mes (ej.: 31/01 + 1 mes -> 29/02 o 28/02).
    """
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, ultimo_dia_mes(y, m).day)
    return date(y, m, day)

def meses_y_dias_en_intervalo(inicio: date, fin: date, *, fin_inclusivo: bool = True) -> tuple[int, int]:
    """
    Calcula meses completos y días restantes en el intervalo:
      - [inicio, fin]  si fin_inclusivo=True (por defecto)
      - [inicio, fin)  si fin_inclusivo=False

    Retorna: (meses_completos, dias_restantes)
    """
    if fin < inicio:
        return 0, 0

    # Internamente usamos intervalo semiabierto [ini, fin_excl)
    fin_excl = fin + timedelta(days=1) if fin_inclusivo else fin

    # Cálculo O(1) de meses completos con ajuste por día
    meses = (fin_excl.year - inicio.year) * 12 + (fin_excl.month - inicio.month)
    if fin_excl.day < inicio.day:
        meses -= 1
    if meses < 0:
        meses = 0

    # Días restantes desde el final de los meses completos hasta fin_excl
    cursor = add_months(inicio, meses)
    dias = (fin_excl - cursor).days
    if dias < 0:
        # Caso borde: si el ajuste sobrepasa el fin, no hay meses completos
        meses = 0
        cursor = inicio
        dias = (fin_excl - cursor).days

    return meses, dias

def meses_y_dias_en_anio(inicio: date, fin: date, anio: int, *, fin_inclusivo: bool = True) -> tuple[int, int]:
    """
    Calcula (meses_completos, dias_restantes) contenidos dentro del año `anio`,
    limitando el rango [inicio, fin] al traslape con [01/01/anio, 12/31/anio].
    """
    if fin < inicio:
        return 0, 0

    ini_anio = max(inicio, date(anio, 1, 1))
    fin_anio = min(fin, date(anio, 12, 31))
    if fin_anio < ini_anio:
        return 0, 0

    return meses_y_dias_en_intervalo(ini_anio, fin_anio, fin_inclusivo=fin_inclusivo)

# Wrappers que aceptan strings MM/DD/YYYY
def meses_y_dias_en_rango(inicio_str: str, fin_str: str, formato: str = "%m/%d/%Y", *, fin_inclusivo: bool = True) -> tuple[int, int]:
    inicio = datetime.strptime(inicio_str, formato).date()
    fin = datetime.strptime(fin_str, formato).date()
    return meses_y_dias_en_intervalo(inicio, fin, fin_inclusivo=fin_inclusivo)

def meses_y_dias_en_anio_desde_str(inicio_str: str, fin_str: str, anio: int, formato: str = "%m/%d/%Y", *, fin_inclusivo: bool = True) -> tuple[int, int]:
    inicio = datetime.strptime(inicio_str, formato).date()
    fin = datetime.strptime(fin_str, formato).date()
    return meses_y_dias_en_anio(inicio, fin, anio, fin_inclusivo=fin_inclusivo)
