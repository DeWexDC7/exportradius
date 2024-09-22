import json
import psycopg2
from psycopg2 import sql
import os
import time
import logging
from logging.handlers import RotatingFileHandler

# Configurar logging con rotación de logs
log_handler = RotatingFileHandler('migration.log', maxBytes=5 * 1024 * 1024, backupCount=3)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[log_handler]
)

# Función para leer las credenciales desde el archivo conexion.json
def load_db_config():
    config_path = os.path.join('config', 'conexion.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config

# Función para eliminar las tablas en la base de datos de exportación
def drop_tables():
    db_config = load_db_config()
    db_config_radiusexport = db_config['radiusexport']

    try:
        conn_radiusexport = psycopg2.connect(**db_config_radiusexport)
        cursor_radiusexport = conn_radiusexport.cursor()

        cursor_radiusexport.execute("DROP TABLE IF EXISTS anime_studio CASCADE;")
        cursor_radiusexport.execute("DROP TABLE IF EXISTS anime CASCADE;")
        cursor_radiusexport.execute("DROP TABLE IF EXISTS studio CASCADE;")
        conn_radiusexport.commit()

        logging.info("Tablas eliminadas correctamente de la base de datos de exportación.")
    
    except (psycopg2.DatabaseError) as error:
        logging.error(f"Error al eliminar las tablas: {error}")
    finally:
        if conn_radiusexport:
            cursor_radiusexport.close()
            conn_radiusexport.close()

# Función para migrar tablas en lotes
def migrate_table(table_name, query, column_names, batch_size=1000):
    db_config = load_db_config()
    conn_radiusmain = None
    conn_radiusexport = None

    try:
        # Conexión a la base de datos principal (main)
        conn_radiusmain = psycopg2.connect(**db_config['radiusmain'])
        cursor_radiusmain = conn_radiusmain.cursor(name='main_cursor')  # Usar un cursor con nombre para el paginado
        cursor_radiusmain.execute(query)

        # Conexión a la base de datos de exportación (export)
        conn_radiusexport = psycopg2.connect(**db_config['radiusexport'])
        cursor_radiusexport = conn_radiusexport.cursor()

        # Crear la tabla en la base de datos de exportación si no existe
        if table_name == "anime":
            cursor_radiusexport.execute("""
                CREATE TABLE IF NOT EXISTS anime (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    title TEXT NOT NULL,
                    genre TEXT NOT NULL,
                    release_year INT,
                    last_modified TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        elif table_name == "studio":
            cursor_radiusexport.execute("""
                CREATE TABLE IF NOT EXISTS studio (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    name TEXT NOT NULL,
                    country TEXT,
                    last_modified TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        elif table_name == "anime_studio":
            cursor_radiusexport.execute("""
                CREATE TABLE IF NOT EXISTS anime_studio (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    anime_id BIGINT,
                    studio_id BIGINT,
                    UNIQUE (anime_id, studio_id),
                    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
                    FOREIGN KEY (studio_id) REFERENCES studio(id) ON DELETE CASCADE,
                    last_modified TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn_radiusexport.commit()

        # Preparar la consulta de inserción
        insert_query = sql.SQL(
            """
            INSERT INTO {} ({}) 
            OVERRIDING SYSTEM VALUE
            VALUES ({}) 
            ON CONFLICT (id) 
            DO UPDATE SET {}
            """
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(sql.Placeholder() * len(column_names)),
            sql.SQL(', ').join(
                sql.Composed([sql.Identifier(col), sql.SQL(" = EXCLUDED."), sql.Identifier(col)]) for col in column_names[1:]
            )
        )

        # Procesar los registros en lotes
        total_rows_migrated = 0
        while True:
            rows = cursor_radiusmain.fetchmany(batch_size)
            if not rows:
                break
            cursor_radiusexport.executemany(insert_query, rows)
            conn_radiusexport.commit()
            total_rows_migrated += len(rows)
            logging.info(f"Migrados {len(rows)} registros para '{table_name}'")

        logging.info(f"Migración completada para '{table_name}' con un total de {total_rows_migrated} registros.")
    
    except (psycopg2.DatabaseError) as error:
        logging.error(f"Error durante la migración de '{table_name}': {error}")
    
    finally:
        if conn_radiusmain:
            cursor_radiusmain.close()
        if conn_radiusexport:
            cursor_radiusexport.close()

if __name__ == "__main__":
    tiempo_inicial = time.time()
    
    # Eliminar todas las tablas de la base de datos de exportación
    drop_tables()

    # Migrar las tablas desde la base de datos principal a la base de datos de exportación

    # Migrar la tabla 'anime'
    migrate_table(
        'anime', 
        """
        SELECT id, title, genre, release_year, last_modified FROM anime ORDER BY id ASC;
        """, 
        ['id', 'title', 'genre', 'release_year', 'last_modified']
    )

    # Migrar la tabla 'studio'
    migrate_table(
        'studio', 
        """
        SELECT id, name, country, last_modified FROM studio ORDER BY id ASC;
        """, 
        ['id', 'name', 'country', 'last_modified']
    )

    # Migrar la tabla 'anime_studio'
    migrate_table(
        'anime_studio', 
        """
        SELECT id, anime_id, studio_id, last_modified FROM anime_studio ORDER BY id ASC;
        """, 
        ['id', 'anime_id', 'studio_id', 'last_modified']
    )

    tiempo_final = time.time()
    tiempo_ejecucion = tiempo_final - tiempo_inicial
    logging.info(f"Tiempo total de ejecución: {tiempo_ejecucion:.2f} segundos")
    print(f"Tiempo total de ejecución: {tiempo_ejecucion:.2f} segundos")
