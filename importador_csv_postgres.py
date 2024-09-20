
# -*- coding: utf-8 -*-

import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime

def connect_to_db(host_db, name_db, user, passdb):
    try:
        # Intentar conectar a la base de datos
        conn = psycopg2.connect(
            host=host_db,
            database=name_db,
            user=user,
            password=passdb
        )
        print("Conexion exitosa a la base de datos.")
        return conn
    
    except psycopg2.OperationalError as e:
        # Mostrar el mensaje de error si no se puede conectar
        print(f"Error al conectar con la base de datos: {e}")
        return None
def procesar(host_db, name_db, user, passdb, file_csv, sep_csv):
    # Llamar a la función para conectar a la base de datos
    conn = connect_to_db(host_db, name_db, user, passdb)
    
    # Verificar si se estableció la conexión
    if conn is None:
        print("No se pudo establecer la conexion. Saliendo del proceso.")
        return

    try:
        # Leer archivo csv, separado por el delimitador proporcionado
        data = pd.read_csv(file_csv, delimiter=sep_csv)

        # Obtener los nombres de las columnas del encabezado del CSV
        columnas = data.columns.tolist()

        now = datetime.now()
        formatted_datetime = now.strftime("%Y%b%d_%H%M%S")
        tabla_postgres = "table_" + formatted_datetime

        # Convertir DataFrame a una lista de tuplas
        filas = [tuple(x) for x in data.to_numpy()]

        cur = conn.cursor()

        # Crear la tabla en PostgreSQL usando los nombres de las columnas del CSV
        column_def = ", ".join([f"{col} TEXT" for col in columnas])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {tabla_postgres} ({column_def});"
        cur.execute(create_table_query)

        # Insertar los datos en la tabla
        insert_query = f"INSERT INTO {tabla_postgres} ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))});"
        cur.executemany(insert_query, filas)

        conn.commit()

        print(f"Datos cargados correctamente en la tabla {tabla_postgres}")

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        conn.rollback()  # Deshacer cualquier cambio en caso de error
    
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    host_db = input("Ingrese host de la base de datos: ")
    
    name_db = input("Ingrese el nombre de la base de datos: ")

    user = input("Ingrese el usuario de postgres: ")

    passdb = input("Ingrese clave de postgres: ")

    file_csv = input("Ingrese ruta absoluta  y nombre del archivo csv: ")

    sep_csv = input("Ingrese caracter separador del csv (',', ';', etc): ")
    
    procesar(host_db, name_db, user, passdb, file_csv, sep_csv )
    
   