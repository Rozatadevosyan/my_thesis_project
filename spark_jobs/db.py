import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="thesis_db",
        user="postgres",
        password="postgres123",
        port="5440"
    )