from datetime import datetime, date, timedelta

def ultimo_dia_mes(y, m):
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)

def add_months(d, months):
    # Suma 'months' meses a la fecha d, "anclando" el día:
    # si el día no existe en el mes destino, usa el último día de ese mes.
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, ultimo_dia_mes(y, m).day)
    return date(y, m, day)

def meses_y_dias_en_2025(inicio_str, fin_str, formato="%m/%d/%Y", fin_inclusivo=True):
    """
    Devuelve (meses_completos, dias_restantes) del año 2025
    contenidos en el traslape con el rango [inicio, fin].
    - inicio_str, fin_str: fechas en texto, ej. '12/15/2024' y '03/20/2025'
    - formato: patrón para datetime.strptime (por defecto MM/DD/YYYY)
    - fin_inclusivo: si True, el fin del rango se considera inclusivo
    """
    # 1) Parseo de fechas
    inicio = datetime.strptime(inicio_str, formato).date()
    fin = datetime.strptime(fin_str, formato).date()
    if fin < inicio:
        return 0, 0  # rango inválido

    # 2) Acotar a 2025
    ini25 = max(inicio, date(2025, 1, 1))
    fin25 = min(fin, date(2025, 12, 31))

    if fin25 < ini25:
        return 0, 0  # sin traslape con 2025

    # 3) Hacer el fin inclusivo si así se requiere
    fin_efectivo_excl = fin25 + timedelta(days=1) if fin_inclusivo else fin25

    # 4) Contar meses completos desde ini25
    meses = 0
    cursor = ini25
    while True:
        siguiente = add_months(cursor, 1)
        if siguiente <= fin_efectivo_excl:
            cursor = siguiente
            meses += 1
        else:
            break

    # 5) Días restantes
    dias = (fin_efectivo_excl - cursor).days
    return meses, dias

# ---------------------------
# Ejemplos de uso:
if __name__ == "__main__":
    ejemplos = [
        ("1/1/2025", "12/31/2025"),  # traslape: 01/01/2025–03/20/2025
        ("1/1/2025", "12/31/2025"),  # traslape: todo 2025 => 12 meses, 0 días
        ("1/1/2025", "12/31/2025"),  # traslape: 07/10–08/05 => 0 meses, 27 días (fin incl.)
        ("4/19/2025", "4/18/2026"),  # cuidado con fin de mes
    ]

    for ini, fin in ejemplos:
        m, d = meses_y_dias_en_2025(ini, fin)
        print(f"{ini} – {fin} => {m} meses y {d} días de 2025")0

from datetime import datetime
import pandas as pd

def normalizar_fechas(data, sep_in='/', formato_in='m-d-a', sep_out='/', formato_out='m-d-a'):
    """
    Normaliza fechas en una lista de listas (varias columnas).
    Convierte día y mes a dos dígitos y año a 4 dígitos.
    
    Parámetros:
    - data: lista de listas con las fechas (ej. [[col1, col2], [col1, col2], ...])
    - sep_in: separador de entrada (por defecto '/')
    - formato_in: formato de entrada (por defecto 'm-d-a')
    - sep_out: separador de salida (por defecto '/')
    - formato_out: formato de salida (por defecto 'm-d-a')
    
    Retorna:
    - Lista de listas con las fechas normalizadas.
    """
    formato_map = {'d': '%d', 'm': '%m', 'a': '%Y'}
    formato_map_in = {'d': '%d', 'm': '%m', 'a': '%Y'}

    # Construir formato de entrada y salida
    fmt_in = sep_in.join([formato_map_in[x] for x in formato_in.split('-')])
    fmt_out = sep_out.join([formato_map[x] for x in formato_out.split('-')])

    resultado = []
    for fila in data:
        nueva_fila = []
        for fecha in fila:
            try:
                # Intentar parsear con año largo
                dt = datetime.strptime(fecha, fmt_in)
            except ValueError:
                # Intentar con año corto
                fmt_in_short = fmt_in.replace('%Y', '%y')
                try:
                    dt = datetime.strptime(fecha, fmt_in_short)
                except ValueError:
                    nueva_fila.append(fecha)
                    continue
            nueva_fila.append(dt.strftime(fmt_out))
        resultado.append(nueva_fila)
    return resultado

# Ejemplo de uso
entrada = [
    ["02/10/2025", "11/06/2025"],
    ["3/28/25", "11/06/2025"],
    ["9/24/25", "10/07/2025"],
    ["03/04/2025", "05/09/2025"],
    ["03/04/2025", "05/09/2025"],
    ["03/04/2025", "03/12/2025"],
    ["3/24/25", "4/18/25"],
    ["4/18/25", "4/24/25"],
    ["03/10/2025", "9/30/25"],
    ["03/10/2025", "11/12/2025"]
]

entrada2 = [
    ["02-10-2025", "11-06-2025"],
    ["23-8-25", "11-06-2025"],
    ["24-11-25", "24-04-25"],
    ["03-10-2025", "9-03-25"],
    ["03-10-2025", "11-12-2025"]
]

# Normalización
salida1 = normalizar_fechas(entrada, sep_in='/', formato_in='m-d-a', sep_out='/', formato_out='m-d-a')
salida2 = normalizar_fechas(entrada2, sep_in='-', formato_in='d-m-a', sep_out='-', formato_out='d-m-a')

# Mostrar resultados
print("Ejemplo 1:")
print(pd.DataFrame(salida1, columns=['InicioReal', 'FinReal']))
print("\nEjemplo 2:")
print(pd.DataFrame(salida2, columns=['InicioReal', 'FinReal']))
