import json
import psycopg2
import random
from faker import Faker
import time

# Configuración de Faker para generar datos ficticios
fake = Faker()

# Función para leer las credenciales desde el archivo conexion.json
def load_db_config():
    config_path = 'config/conexion.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config

# Función para insertar registros en la tabla 'anime'
def insert_anime_records(cursor, batch_size=1000, total_records=100000):
    genres = ['Action', 'Adventure', 'Comedy', 'Drama', 'Fantasy', 'Horror', 'Romance', 'Sci-Fi']
    insert_query = """
        INSERT INTO anime (title, genre, release_year, last_modified)
        VALUES (%s, %s, %s, NOW())
    """
    
    for i in range(0, total_records, batch_size):
        batch = [
            (fake.catch_phrase(), random.choice(genres), random.randint(1980, 2023))
            for _ in range(batch_size)
        ]
        cursor.executemany(insert_query, batch)
        print(f"Inserted {i + batch_size} anime records")

# Función para insertar registros en la tabla 'studio'
def insert_studio_records(cursor, batch_size=100, total_records=100):
    countries = ['Japan', 'USA', 'Canada', 'UK', 'Germany', 'France', 'China', 'South Korea']
    insert_query = """
        INSERT INTO studio (name, country, last_modified)
        VALUES (%s, %s, NOW())
    """
    
    for i in range(0, total_records, batch_size):
        batch = [
            (fake.company(), random.choice(countries))
            for _ in range(batch_size)
        ]
        cursor.executemany(insert_query, batch)
        print(f"Inserted {i + batch_size} studio records")

# Función para insertar registros en la tabla 'anime_studio'
def insert_anime_studio_records(cursor, total_anime, total_studio, batch_size=1000, total_records=100000):
    insert_query = """
        INSERT INTO anime_studio (anime_id, studio_id, last_modified)
        VALUES (%s, %s, NOW())
        ON CONFLICT (anime_id, studio_id) DO NOTHING
    """
    
    for i in range(0, total_records, batch_size):
        batch = [
            (random.randint(1, total_anime), random.randint(1, total_studio))
            for _ in range(batch_size)
        ]
        cursor.executemany(insert_query, batch)
        print(f"Inserted {i + batch_size} anime_studio records")

def main():
    # Leer la configuración de la base de datos
    db_config = load_db_config()
    conn_radiusmain = None
    
    try:
        # Conectar a la base de datos principal
        conn_radiusmain = psycopg2.connect(**db_config['radiusmain'])
        cursor = conn_radiusmain.cursor()

        # Insertar 100,000 registros en la tabla 'anime'
        insert_anime_records(cursor, total_records=100000)

        # Insertar 100 estudios en la tabla 'studio'
        insert_studio_records(cursor, total_records=100)

        # Relacionar registros de anime y studio en 'anime_studio'
        insert_anime_studio_records(cursor, total_anime=100000, total_studio=100, total_records=100000)

        # Confirmar los cambios
        conn_radiusmain.commit()

        print("All records have been inserted successfully.")
    
    except (psycopg2.DatabaseError) as error:
        print(f"Error during insertion: {error}")
        if conn_radiusmain:
            conn_radiusmain.rollback()
    
    finally:
        if conn_radiusmain:
            cursor.close()
            conn_radiusmain.close()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")
