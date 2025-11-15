# Seleccionar columnas relevantes y eliminar duplicados
aplicaciones = df[['id', 'app']].drop_duplicates().reset_index(drop=True)

# Agregar columna de identificador único
aplicaciones['id_unico'] = range(1, len(aplicaciones) + 1)

# Guardar en un archivo CSV
aplicaciones.to_csv('aplicaciones.csv', index=False)


# Reemplazar valores nulos en la columna 'tuid'
df['uid'] = df['uid'].fillna('TEST-APP-2025')

# Convertir la columna 'unblended_cost' a tipo numérico
df['cost'] = pd.to_numeric(df['cost'], errors='coerce')


# Calcular el costo total por uid
costo_por_uid = df.groupby('uid')['cost'].sum().reset_index()
costo_por_uid.rename(columns={'cost': 'costo_total'}, inplace=True)

# Unir el costo total al DataFrame original
df = df.merge(costo_por_uid, on='uid', how='left')

# Redondear a 2 decimales
df['cost'] = df['cost'].round(2)

# Si los valores están como texto (strings), primero conviértelos a numérico
df['cost'] = pd.to_numeric(df['cost'], errors='coerce').round(2)

# Convertir la columna a numérico y redondear a 2 decimales
df['cost'] = pd.to_numeric(df['cost'].str.replace("'", ""), errors='coerce').round(2)

# Redondear la columna 'costo_total' a dos decimales
df['total'] = df['total'].round(2)


# Aplicar utilidades
df = limpiar_columna(df, 'cost')
df = convertir_a_numerico(df, 'cost')
df = reemplazar_nulos(df, 'uid', 'TEST-2025')
apps = agregar_costo_total(df, apps)

# groupby permite agrupar filas de un DataFrame según los valores de una o más columnas, y luego aplicar funciones como:

sum() → suma de valores
mean() → promedio
count() → conteo
max(), min() → máximo y mínimo
agg() → aplicar múltiples funciones


``` import pandas as pd

df = pd.DataFrame({
    'uid': ['uid1', 'uid2', 'uid1', 'uid3'],
    'unblended_cost': [100, 200, 150, 300]
})

df.groupby('uid')['cost'].sum()
df.groupby('uid')['cost'].agg(['sum', 'mean', 'count'])

# Agrupacion multiples columnas

df.groupby(['uid', 'otra_columna']).sum()
```

# función reutilizable que puedes incluir en tu script de utilidades para realizar agrupaciones con groupby y aplicar funciones agregadas como suma, promedio y conteo
```
def agrupar_y_agregar(df, columnas_agrupacion, columna_valor, funciones=['sum']):
    """
    Agrupa un DataFrame por una o más columnas y aplica funciones agregadas sobre otra columna.

    Parámetros:
    - df: DataFrame de entrada
    - columnas_agrupacion: lista de columnas por las que se agrupará
    - columna_valor: columna sobre la que se aplicarán las funciones
    - funciones: lista de funciones agregadas (por defecto ['sum'])

    Retorna:
    - DataFrame con resultados agregados
    """
    resultado = df.groupby(columnas_agrupacion)[columna_valor].agg(funciones).reset_index()
    return resultado
```

Invocación

```
from utilidades_datos import agrupar_y_agregar

resultado = agrupar_y_agregar(
    df,
    columnas_agrupacion=['troux_uid'],
    columna_valor='unblended_cost',
    funciones=['sum', 'mean', 'count']
)
print(resultado)
```
# Obtener Columnas del dataframe

columnas = df.columns.tolist()
print(columnas)


# Intentamos convertir a datetime
df['Fin Contrato Parsed'] = pd.to_datetime(df['Fin Contrato'], format='%m/%d/%Y', errors='coerce')

# Filas que no son fechas válidas
filas_invalidas = df[df['Fin Fecha Parsed'].isna()]
print(filas_invalidas)

# ✅ 2. Estrategias para sanitizar el campo
Dependiendo del contexto, puedes aplicar una de estas estrategias:

### Eliminar filas inválidas:
df = df.dropna(subset=['Fin Fecha  Parsed'])

### Reemplazar por una fecha por defecto (por ejemplo, 01/01/1900):
df['Fin Fecha Parsed'] = df['Fin Fecha  Parsed'].fillna(pd.Timestamp('1900-01-01'))

### Intentar corregir formatos comunes (por ejemplo, espacios extra, guiones en vez de barras):
df['Fin Fecha'] = df['Fin Fecha'].str.strip().str.replace('-', '/')
df['Fin Fecha Parsed'] = pd.to_datetime(df['Fin Fecha'], errors='coerce')

## reemplazar los valores inválidos por la fecha por defecto 31/12/2025 de esta manera:

### Convertimos la columna a datetime, marcando inválidos como NaT
df['Fin Contrato'] = pd.to_datetime(df['Fin Contrato'], format='%m/%d/%Y', errors='coerce')

### Reemplazamos los NaT por la fecha por defecto
fecha_defecto = pd.Timestamp('2025-12-31')
df['Fin Contrato'] = df['Fin Contrato'].fillna(fecha_defecto)

print(df['Fin Contrato'])

# código completo en Python para:

Convertir las columnas a fecha.
Reemplazar valores inválidos por 31/12/2025.
Calcular Meses, Días Restantes y Días Totales.
Exportar el resultado a un archivo CSV.

```
import pandas as pd

# === 1. Cargar el DataFrame (ejemplo: desde un CSV) ===
# df = pd.read_csv('tu_archivo.csv')

# Para este ejemplo, supongamos que ya tienes el DataFrame:
# df = pd.DataFrame({
#     'Inicio Contrato': ['01/01/2023', '03/15/2024', '07/20/2025'],
#     'Fin Contrato': ['12/31/2023', 'invalid_date', '08/25/2025']
# })

# === 2. Convertir columnas a datetime ===
df['Inicio Contrato'] = pd.to_datetime(df['Inicio Contrato'], format='%m/%d/%Y', errors='coerce')
df['Fin Contrato'] = pd.to_datetime(df['Fin Contrato'], format='%m/%d/%Y', errors='coerce')

# === 3. Reemplazar fechas inválidas por 31/12/2025 ===
fecha_defecto = pd.Timestamp('2025-12-31')
df['Fin Contrato'] = df['Fin Contrato'].fillna(fecha_defecto)

# === 4. Calcular días totales ===
df['Dias Totales'] = (df['Fin Contrato'] - df['Inicio Contrato']).dt.days

# === 5. Calcular meses completos y días restantes ===
df['Meses'] = df['Dias Totales'] // 30
df['Dias Restantes'] = df['Dias Totales'] % 30

# === 6. Exportar a CSV ===
df.to_csv('contratos_sanitizados.csv', index=False)

print("Archivo 'contratos_sanitizados.csv' generado con éxito.")
```

#  solución completa para crear una función que distribuya el valor mensual en las columnas de enero a diciembre para un año específico (por defecto 2025), cumpliendo todas tus condiciones:

✅ Lógica de la función

Año por defecto: 2025.
Si la fecha de inicio:
- Es del año especificado, el mes de inicio es el de la fecha.
- Es anterior al año, el mes de inicio será enero.
- Es posterior al año, todos los meses serán cero.

Desde el mes de inicio, asignar el valor mensual (Valor_Mes_Contrato_COP) hasta completar:
- El número de meses (Meses_Ajustados_2025).
- O diciembre (no sobrepasar diciembre).
Meses anteriores al inicio → 0.


```
import pandas as pd

# 1. Convertir columnas a numérico antes de distribuir
df['Valor_Mes_Contrato_COP'] = pd.to_numeric(df['Valor_Mes_Contrato_COP'], errors='coerce').fillna(-1)
df['Meses_Ajustados_2025'] = pd.to_numeric(df['Meses_Ajustados_2025'], errors='coerce').fillna(0).astype(int)

# 2. Función para distribuir valores por meses
def distribuir_valor_mensual(inicio, valor_mensual, meses_ajustados, year=2025):
    meses = [0.00] * 12
    if pd.isna(inicio) or valor_mensual == -1 or meses_ajustados <= 0:
        return meses
    
    try:
        valor_mensual = float(valor_mensual)
    except:
        return meses
    
    # Determinar mes de inicio
    if inicio.year > year:
        return meses
    elif inicio.year < year:
        mes_inicio = 1
    else:
        mes_inicio = inicio.month
    
    # Calcular hasta dónde asignar
    mes_fin = min(mes_inicio + meses_ajustados - 1, 12)
    
    # Asignar valor mensual
    for i in range(mes_inicio - 1, mes_fin):
        meses[i] = round(valor_mensual, 2)
    
    return meses

# 3. Crear columnas Enero-Diciembre
meses_cols = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
              'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

df[meses_cols] = df.apply(
    lambda row: pd.Series(distribuir_valor_mensual(row['Inicio Contrato'],
                                                   row['Valor_Mes_Contrato_COP'],
                                                   row['Meses_Ajustados_2025'])),
    axis=1
)

# 4. Calcular total anual
df['Total_Anual_2025'] = df[meses_cols].sum(axis=1)

# 5. Formatear todas las columnas a dos decimales
for col in meses_cols + ['Total_Anual_2025']:
    df[col] = df[col].apply(lambda x: f"{x:.2f}")

# Exportar a CSV
df.to_csv('contratos_distribucion_2025.csv', index=False)

print(df[['Inicio Contrato', 'Valor_Mes_Contrato_COP', 'Meses_Ajustados_2025', 'Total_Anual_2025'] + meses_cols])
```

```

```

```

```


