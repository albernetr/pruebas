import pandas as pd
import re
from datetime import datetime, timedelta

def cargar_archivo(ruta_csv):
    """
    Lee un archivo CSV y lo carga en un DataFrame.
    
    Parámetros:
        ruta_csv (str): Ruta del archivo CSV.
    
    Retorna:
        pd.DataFrame: DataFrame con los datos del inventario.
    """
    try:
        # Leer el CSV con separador adecuado (tabulación o coma)
        df = pd.read_csv(ruta_csv, sep=',', encoding='utf-8')
        
        # Mostrar información básica
        print("Archivo cargado correctamente.")
        print(f"Filas: {df.shape[0]}, Columnas: {df.shape[1]}")
        
        return df
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None




def limpiar_columna(df, columna, reemplazo, default=""):
    """
    Elimina caracteres no deseados de una columna.
    """
    df[columna] = df[columna].astype(str).str.replace(reemplazo, default, regex=False)
    return df

def convertir_a_numerico(df, columna, decimales=2):
    """
    Convierte una columna a tipo numérico y la redondea.
    """
    df[columna] = pd.to_numeric(df[columna], errors='coerce').round(decimales)
    return df

def reemplazar_nulos(df, columna, valor_por_defecto):
    """
    Reemplaza valores nulos en una columna por un valor por defecto.
    """
    df[columna] = df[columna].fillna(valor_por_defecto)
    return df

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

def extraer_columnas(df, columnas, incluir_duplicados=True):
    """
    Extrae columnas específicas de un DataFrame con opción de eliminar duplicados.

    Parámetros:
    - df (pd.DataFrame): El DataFrame original.
    - columnas (list): Lista de nombres de columnas a extraer.
    - incluir_duplicados (bool): Si es True, mantiene duplicados. Si es False, los elimina.

    Retorna:
    - pd.DataFrame con las columnas seleccionadas, con o sin duplicados.
    """
    df_filtrado = df[columnas]
    if not incluir_duplicados:
        df_filtrado = df_filtrado.drop_duplicates()
    return df_filtrado






def agregar_costo_total(df, aplicaciones, columna_uid='troux_uid', columna_costo='unblended_cost'):
    """
    Agrega una columna 'costo_total' al DataFrame de aplicaciones con la suma de costos por troux_uid.
    """
    costos = df.groupby(columna_uid)[columna_costo].sum().reset_index()
    costos.rename(columns={columna_costo: 'costo_total'}, inplace=True)
    aplicaciones = aplicaciones.merge(costos, on=columna_uid, how='left')
    aplicaciones['costo_total'] = aplicaciones['costo_total'].round(2)
    return aplicaciones


def separar_nombres_apellidos(df):
    """
    Separa la columna 'Nombres' en Apellido1, Apellido2, Nombre1 y Nombre2.
    Maneja casos donde puede haber solo un apellido o un nombre.
    """
    # Separar por coma: [apellidos, nombres]
    nombres_split = df['Nombres'].str.split(',', n=1, expand=True)
    df['Apellidos'] = nombres_split[0].str.strip()
    df['Nombres_'] = nombres_split[1].str.strip()

    # Separar apellidos
    apellidos_split = df['Apellidos'].str.split(' ', n=1, expand=True)
    df['Apellido1'] = apellidos_split[0]
    df['Apellido2'] = apellidos_split[1].fillna('')

    # Separar nombres
    nombres_split = df['Nombres_'].str.split(' ', n=1, expand=True)
    df['Nombre1'] = nombres_split[0]
    df['Nombre2'] = nombres_split[1].fillna('')

    # Eliminar columnas auxiliares
    df = df.drop(columns=['Apellidos', 'Nombres_'])

    return df

def cargar_hojas_excel(ruta_archivo, hojas):
    """
    Carga varias hojas de un archivo Excel y retorna un diccionario de DataFrames.

    Parámetros:
    - ruta_archivo (str): Ruta del archivo Excel.
    - hojas (list): Lista con los nombres de las hojas a cargar.

    Retorna:
    - dict: Diccionario {nombre_hoja: DataFrame}
    """
    dataframes = {}
    for hoja in hojas:
        print(hoja)
        dataframes[hoja] = pd.read_excel(ruta_archivo, sheet_name=hoja, engine='openpyxl')
    return dataframes  # ¡Este return es clave!

import pandas as pd
import re

def dividir_registros(df, columna_codigo='Codigo', columna_valor='AplicacionesImpactadas '):
    """
    Divide los registros de un DataFrame en múltiples filas si la columna especificada
    contiene separadores por saltos de línea o comas.
    
    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columna_codigo (str): Nombre de la columna que contiene el código.
    - columna_valor (str): Nombre de la columna que contiene los valores a dividir.
    
    Retorna:
    - pd.DataFrame: Nuevo DataFrame con los registros descompuestos.
    """
    registros = []

    for _, fila in df.iterrows():
        codigo = fila[columna_codigo]
        valores = str(fila[columna_valor])

        # Dividir por saltos de línea o comas
        partes = re.split(r'[\n,]+', valores)

        # Limpiar espacios y crear nuevos registros
        for parte in partes:
            parte = parte.strip()
            if parte:  # Evitar vacíos
                registros.append({columna_codigo: codigo, columna_valor: parte})

    # Crear nuevo DataFrame
    nuevo_df = pd.DataFrame(registros)
    return nuevo_df


import re

def agregar_columnas_porcentaje(df, columna_fuente='RecursosInternos'):
    """
    Agrega dos columnas:
    - PorcentajeAsignacion: el valor dentro de corchetes (ej. '5%')
    - ValorPorcentajeAsignacion: el valor numérico en formato decimal (ej. 0.05)
    """
    # Extraer el texto dentro de corchetes
    df['PorcentajeAsignacion'] = df[columna_fuente].str.extract(r'\[(.*?)\]')

    # Convertir a decimal (quitando el % y dividiendo entre 100)
    df['ValorPorcentajeAsignacion'] = (
        df['PorcentajeAsignacion']
        .str.replace('%', '', regex=False)
        .astype(float) / 100
    )

    return df

def separar_recursos_externos(df, columna_origen):
    """
    Separa la columna 'RecursosExternosProveedoSM' en:
    Proveedor, ContratoJira, Consultor, Observaciones.
    
    Si faltan posiciones, se completa con cadenas vacías.
    
    Retorna:
    - pd.DataFrame con las nuevas columnas agregadas.
    """
    def procesar_fila(valor):
        if pd.isna(valor):
            return ["", "", "", ""]
        partes = [p.strip() for p in valor.split('|')]
        # Completar con blancos si faltan posiciones
        while len(partes) < 4:
            partes.append("")
        return partes[:4]

    # Aplicar la función fila por fila
    nuevas_columnas = df[columna_origen].apply(procesar_fila)
    
    # Crear nuevas columnas
    df[['Proveedor', 'ContratoJira', 'Consultor', 'Observaciones']] = pd.DataFrame(nuevas_columnas.tolist(), index=df.index)
    return df
    
def agregar_columnas_porcentaje_v1(df, columna_fuente='RecursosInternos'):
    """
    Agrega dos columnas:
    - PorcentajeAsignacion: el valor dentro de corchetes (ej. '5%')
    - ValorPorcentajeAsignacion: el valor numérico en formato decimal (ej. 0.05)
    
    Mejoras:
    ✅ Maneja valores nulos sin error.
    ✅ Extrae solo números y símbolo %.
    ✅ Si no hay porcentaje, deja NaN.
    """
    # Asegurar que la columna es string para evitar errores
    df[columna_fuente] = df[columna_fuente].astype(str)

    # Extraer el texto dentro de corchetes (ej. [10%])
    df['PorcentajeAsignacion'] = df[columna_fuente].str.extract(r'\[(\d+%?)\]')

    # Convertir a decimal (quitando el % y dividiendo entre 100)
    df['ValorPorcentajeAsignacion'] = (
        df['PorcentajeAsignacion']
        .str.replace('%', '', regex=False)
        .astype(float)
        .div(100)
    )

    return df


def filtrar_dataframe(df, filtros):
    """
    Filtra un DataFrame según uno o varios criterios.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - filtros (dict): Diccionario con {columna: valor} para filtrar.
                      Ejemplo: {'TipoRequerimiento': 'Proyecto'} 
                      o {'TipoRequerimiento': 'Proyecto', 'EstadoProyecto': 'Activo'}

    Retorna:
    - pd.DataFrame: DataFrame filtrado.
    """
    df_filtrado = df.copy()
    for columna, valor in filtros.items():
        df_filtrado = df_filtrado[df_filtrado[columna] == valor]
    return df_filtrado

import pandas as pd

def dividir_registros_1(df, columna, nuevo_nombre=None, separador=','):
    """
    Divide los valores separados por un separador en la columna indicada
    y genera nuevos registros por cada valor.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columna (str): Nombre de la columna a dividir.
    - nuevo_nombre (str): Nombre de la nueva columna (si None, usa el original).
    - separador (str): Separador de los valores (por defecto ',').

    Retorna:
    - pd.DataFrame: DataFrame expandido con la columna dividida.
    """
    df_expandido = df.copy()

    # Dividir la columna en listas
    df_expandido[columna] = df_expandido[columna].str.split(separador)

    # Explode para generar filas por cada elemento
    df_expandido = df_expandido.explode(columna)

    # Limpiar espacios extra
    df_expandido[columna] = df_expandido[columna].str.strip()

    # Renombrar la columna si se especifica
    if nuevo_nombre:
        df_expandido = df_expandido.rename(columns={columna: nuevo_nombre})

    return df_expandido

def dividir_registros_multiple(df, columnas_config, separador=','):
    """
    Divide los valores separados por un separador en múltiples columnas y genera nuevos registros.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columnas_config (dict): Diccionario {columna_original: nuevo_nombre}.
                               Ejemplo: {'PorcentajeAsignación': 'RecursoAsignado', 'OtraColumna': 'NuevoNombre'}
    - separador (str): Separador de los valores (por defecto ',').

    Retorna:
    - pd.DataFrame: DataFrame expandido con las columnas divididas y renombradas.
    """
    df_expandido = df.copy()

    for columna_original, nuevo_nombre in columnas_config.items():
        # Dividir la columna en listas
        df_expandido[columna_original] = df_expandido[columna_original].str.split(separador)
        # Explode para generar filas por cada elemento
        df_expandido = df_expandido.explode(columna_original)
        # Limpiar espacios extra
        df_expandido[columna_original] = df_expandido[columna_original].str.strip()
        # Renombrar la columna
        df_expandido = df_expandido.rename(columns={columna_original: nuevo_nombre})

    return df_expandido


def contar_elementos(df, columnas_config, separador=','):
    """
    Agrega nuevas columnas con el conteo de elementos separados por un delimitador.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columnas_config (dict): Diccionario {columna_original: nueva_columna_conteo}.
                               Ejemplo: {'PorcentajeAsignación': 'numRecursosAsignados'}
    - separador (str): Separador de los valores (por defecto ',').

    Retorna:
    - pd.DataFrame: DataFrame con las nuevas columnas de conteo.
    """
    df_resultado = df.copy()

    for columna_original, nueva_columna in columnas_config.items():
        
        if columna_original not in df_resultado.columns:
            print(f"⚠️ La columna '{columna_original}' no existe en el DataFrame.")
            continue

        df_resultado[nueva_columna] = (
            df_resultado[columna_original]
            .fillna('')  # Maneja valores nulos
            .apply(lambda x: len([item for item in x.split(separador) if item.strip() != '']))
        )

    return df_resultado

import os

def guardar_columnas_csv(df, columnas, nombre_archivo, carpeta="ProccesData"):
    """
    Guarda solo las columnas especificadas de un DataFrame en un archivo CSV.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columnas (list): Lista de columnas a guardar.
    - nombre_archivo (str): Nombre del archivo CSV.
    - carpeta (str): Carpeta donde se guardará el archivo (por defecto 'ProccesData').

    Retorna:
    - str: Ruta completa del archivo guardado.
    """
    # Crear carpeta si no existe
    os.makedirs(carpeta, exist_ok=True)

    # Validar columnas existentes
    columnas_existentes = [col for col in columnas if col in df.columns]
    if not columnas_existentes:
        raise ValueError("⚠️ Ninguna de las columnas especificadas existe en el DataFrame.")

    # Filtrar el DataFrame
    df_filtrado = df[columnas_existentes]

    # Ruta completa
    ruta = os.path.join(carpeta, nombre_archivo)

    # Guardar con codificación UTF-8 BOM para evitar problemas con acentos
    df_filtrado.to_csv(ruta, index=False, encoding="utf-8-sig")

    return ruta

import re

def limpiar_columnas(df, reglas):
    """
    Limpia valores en columnas según reglas definidas.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - reglas (list): Lista de reglas, cada una es un diccionario con:
        {
            'columna': 'NombreColumna',
            'tipo': 'replace' o 'regex',
            'patron': 'texto o regex a eliminar'
        }

    Retorna:
    - pd.DataFrame: DataFrame con las columnas limpiadas.
    """
    df_resultado = df.copy()

    for regla in reglas:
        col = regla['columna']
        tipo = regla.get('tipo', 'replace')
        patron = regla['patron']

        if col not in df_resultado.columns:
            print(f"⚠️ La columna '{col}' no existe en el DataFrame.")
            continue

        if tipo == 'replace':
            # Reemplazo simple
            df_resultado[col] = df_resultado[col].astype(str).str.replace(patron, '', regex=False)
        elif tipo == 'regex':
            # Reemplazo usando expresión regular
            df_resultado[col] = df_resultado[col].astype(str).str.replace(patron, '', regex=True)

        # Limpiar espacios extra
        df_resultado[col] = df_resultado[col].str.strip()

    return df_resultado

def formatear_fechas(df, columnas, formato='m-d-a', separador='/', separador_salida=None):
    """
    Formatea fechas en las columnas especificadas según el formato y separador.

    Parámetros:
    - df (pd.DataFrame): DataFrame original.
    - columnas (list): Lista de columnas a formatear.
    - formato (str): Formato esperado en la entrada ('d-m-a' o 'm-d-a').
    - separador (str): Separador actual en las fechas (por defecto '/').
    - separador_salida (str): Separador para la salida (si None, usa el mismo que entrada).

    Retorna:
    - pd.DataFrame: DataFrame con las fechas formateadas.
    """
    df_resultado = df.copy()
    if separador_salida is None:
        separador_salida = separador

    for col in columnas:
        if col not in df_resultado.columns:
            print(f"⚠️ La columna '{col}' no existe.")
            continue

        def convertir_fecha(fecha):
            if pd.isna(fecha) or fecha.strip() == '':
                return ''
            partes = fecha.split(separador)
            try:
                if formato == 'm-d-a':
                    mes, dia, anio = partes
                elif formato == 'd-m-a':
                    dia, mes, anio = partes
                else:
                    return fecha  # Formato desconocido
                # Normalizar a dos dígitos día y mes, año a 4 dígitos
                dia = dia.zfill(2)
                mes = mes.zfill(2)
                anio = anio.zfill(4) if len(anio) == 4 else f"20{anio}"
                return f"{dia}{separador_salida}{mes}{separador_salida}{anio}"
            except:
                return fecha  # Si falla, deja el original

        df_resultado[col] = df_resultado[col].astype(str).apply(convertir_fecha)

    return df_resultado

def calcular_rangos_fechas(df, col_inicio, col_fin, formato='d-m-a', feriados=None):
    """
    Calcula métricas entre dos columnas de fechas:
    1) Día, mes, año para cada fecha.
    2) Total días, meses, años calendario.
    3) Total días laborables (incluyendo inicio y fin).
    4) Detalle de días y meses laborables por año.

    Parámetros:
    - df: DataFrame original.
    - col_inicio: Nombre de la columna con fecha inicial.
    - col_fin: Nombre de la columna con fecha final.
    - formato: 'd-m-a' o 'm-d-a' (por defecto 'd-m-a').
    - feriados: Lista opcional de fechas (datetime) que son no laborables.

    Retorna:
    - DataFrame con columnas adicionales.
    """
    df_resultado = df.copy()
    feriados = set(feriados) if feriados else set()

    # Definir formato para pandas
    fmt = "%d/%m/%Y" if formato == 'd-m-a' else "%m/%d/%Y"

    # Convertir a datetime
    df_resultado[col_inicio] = pd.to_datetime(df_resultado[col_inicio], format=fmt, errors='coerce')
    df_resultado[col_fin] = pd.to_datetime(df_resultado[col_fin], format=fmt, errors='coerce')

    # Validación: inicio <= fin
    df_resultado['ValidacionFechas'] = df_resultado[col_inicio] <= df_resultado[col_fin]

    # Día, mes, año para cada fecha
    for col in [col_inicio, col_fin]:
        df_resultado[f"{col}_Dia"] = df_resultado[col].dt.day
        df_resultado[f"{col}_Mes"] = df_resultado[col].dt.month
        df_resultado[f"{col}_Anio"] = df_resultado[col].dt.year

    # Diferencias calendario
    df_resultado['TotalDias'] = (df_resultado[col_fin] - df_resultado[col_inicio]).dt.days + 1
    df_resultado['TotalMeses'] = ((df_resultado[col_fin].dt.year - df_resultado[col_inicio].dt.year) * 12 +
                                  (df_resultado[col_fin].dt.month - df_resultado[col_inicio].dt.month))
    df_resultado['TotalAnios'] = df_resultado[col_fin].dt.year - df_resultado[col_inicio].dt.year

    # Calcular días laborables
    def dias_laborables(inicio, fin):
        if pd.isna(inicio) or pd.isna(fin) or inicio > fin:
            return 0
        dias = pd.date_range(inicio, fin)
        laborables = [d for d in dias if d.weekday() < 5 and d not in feriados]
        return len(laborables)

    df_resultado['DiasLaborables'] = df_resultado.apply(lambda x: dias_laborables(x[col_inicio], x[col_fin]), axis=1)

    # Detalle por año: días y meses laborables
    detalles = []
    for i, row in df_resultado.iterrows():
        inicio, fin = row[col_inicio], row[col_fin]
        if pd.isna(inicio) or pd.isna(fin) or inicio > fin:
            detalles.append({})
            continue
        detalle_anual = {}
        for year in range(inicio.year, fin.year + 1):
            rango_inicio = max(inicio, datetime(year, 1, 1))
            rango_fin = min(fin, datetime(year, 12, 31))
            dias = pd.date_range(rango_inicio, rango_fin)
            laborables = [d for d in dias if d.weekday() < 5 and d not in feriados]
            detalle_anual[year] = {'dias': len(laborables), 'meses': len(set(d.month for d in laborables))}
        detalles.append(detalle_anual)
    df_resultado['DetalleLaborablePorAnio'] = detalles

    return df_resultado














