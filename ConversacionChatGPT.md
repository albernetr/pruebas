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



