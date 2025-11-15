
# Librerías generales
import os
import sys
import re

# Manipulación de datos
import pandas as pd
import numpy as np

# Visualización
#import matplotlib.pyplot as plt
#import seaborn as sns

# Fechas
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Warnings
import warnings
warnings.filterwarnings('ignore')



# Utilidades y librerias propias
from utils import limpiar_columna, convertir_a_numerico, reemplazar_nulos, agregar_costo_total, cargar_archivo, agrupar_y_agregar
from utils import extraer_columnas,separar_nombres_apellidos, cargar_hojas_excel, dividir_registros, agregar_columnas_porcentaje
from utils import separar_recursos_externos, agregar_columnas_porcentaje_v1, filtrar_dataframe, dividir_registros_1
from utils import dividir_registros_multiple, contar_elementos,guardar_columnas_csv
from utils import limpiar_columnas,formatear_fechas
from utils import calcular_rangos_fechas


