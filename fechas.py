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
        print(f"{ini} – {fin} => {m} meses y {d} días de 2025")
