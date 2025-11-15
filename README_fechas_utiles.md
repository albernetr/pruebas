
# fechas_utiles: utilidades de meses/días entre fechas

## Instalación / Uso local
No requiere instalación. Coloca `fechas_utiles.py` junto a tu script y haz:

```python
from datetime import date
from fechas_utiles import (
    meses_y_dias_en_intervalo,
    meses_y_dias_en_anio,
    meses_y_dias_en_rango,
    meses_y_dias_en_anio_desde_str,
)

# Ejemplos
print(meses_y_dias_en_intervalo(date(2025,1,1), date(2025,12,31)))  # (12, 0)
print(meses_y_dias_en_anio(date(2024,12,15), date(2025,3,20), 2025)) # (2, 20)
print(meses_y_dias_en_rango("12/15/2024", "03/20/2025"))          # (3, 6)
```

## Pruebas
Ejecuta:
```bash
python -m unittest test_fechas_utiles.py -v
```
Las pruebas validan:

- Año completo 2025 → (12, 0)
- Intervalo con solo días → (0, 27)
- Fin exclusivo → (1, 1)
- Ajustes de fin de mes en bisiesto (2024)
- Traslape dentro de 2025
- Wrappers con cadenas
  
## Notas
- `fin_inclusivo=True` considera el final incluido. Para exclusivo, usa `fin_inclusivo=False`.
- El cómputo de meses completos es O(1) y maneja ajustes de fin de mes.

Inclusividad del fin:
- fin_inclusivo=True → tratamos el intervalo como [inicio, fin] (sumamos 1 día internamente).
- fin_inclusivo=False → se interpreta como [inicio, fin).
Meses completos: cálculo O(1) con ajuste por día (fin_excl.day < inicio.day), y los días restantes se miden desde el final de esos meses hasta el fin efectivo.
Casos borde bien manejados: 31 de mes, febrero bisiesto, intervalos cortos, e intervalos sin meses completos.


