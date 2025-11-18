import pyodbc
import pandas as pd
import os

def connect_to_access_db(db_path):
    """
    Conecta a la base de datos Access
    """
    try:
        # Cadena de conexión para Access
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            f'DBQ={db_path};'
        )
        connection = pyodbc.connect(conn_str)
        print(f"Conexión exitosa a {db_path}")
        return connection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def read_table(connection, table_name):
    """
    Lee una tabla específica de la base de datos
    """
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)
        print(f"Tabla '{table_name}' leída exitosamente. Filas: {len(df)}")
        return df
    except Exception as e:
        print(f"Error al leer la tabla {table_name}: {e}")
        return None

def show_table_info(df, table_name):
    """
    Muestra información básica de la tabla
    """
    print(f"\n=== Información de la tabla '{table_name}' ===")
    print(f"Número de filas: {len(df)}")
    print(f"Número de columnas: {len(df.columns)}")
    print(f"Columnas: {list(df.columns)}")
    print("\n=== Primeras 5 filas ===")
    print(df.head())
    print("\n=== Tipos de datos ===")
    print(df.dtypes)

def main():
    # Ruta del archivo Access
    db_path = os.path.join(os.getcwd(), "Test.accdb")
    
    # Verificar que el archivo existe
    if not os.path.exists(db_path):
        print(f"Error: No se encontró el archivo {db_path}")
        return
    
    # Conectar a la base de datos
    conn = connect_to_access_db(db_path)
    if conn is None:
        return
    
    try:
        # Leer la tabla 'test'
        df_test = read_table(conn, "costos1")
        
        if df_test is not None:
            # Mostrar información de la tabla
            show_table_info(df_test, "test")
            
            # Opcional: Guardar en CSV
            csv_filename = "test_table_export.csv"
            df_test.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"\nTabla exportada a {csv_filename}")
            
            # Opcional: Mostrar estadísticas básicas si hay columnas numéricas
            numeric_columns = df_test.select_dtypes(include=['number']).columns
            if len(numeric_columns) > 0:
                print("\n=== Estadísticas básicas (columnas numéricas) ===")
                print(df_test[numeric_columns].describe())
    
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    
    finally:
        # Cerrar la conexión
        conn.close()
        print("\nConexión cerrada.")

if __name__ == "__main__":
    main()
