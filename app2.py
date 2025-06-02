import streamlit as st
import time
import pandas as pd
import mysql.connector
import pyodbc

# Diccionario global
performance_data = {
    'database': [],
    'operation': [],
    'time_ms': []
}

# Clase base
class DatabaseConnector:
    def __init__(self, db_type):
        self.db_type = db_type
        self.connection = None
        self.cursor = None

    def connect(self, **kwargs):
        raise NotImplementedError

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def execute_query(self, query):
        start_time = time.time()
        self.cursor.execute(query)
        execution_time = (time.time() - start_time) * 1000
        return execution_time

    def create_tables(self):
        raise NotImplementedError

    def execute_sp(self, sp_name, params):
        raise NotImplementedError

# Conector MySQL
class MySQLConnector(DatabaseConnector):
    def connect(self, server, database, username, password, port=3306):
        self.connection = mysql.connector.connect(
            host=server,
            database=database,
            user=username,
            password=password,
            port=port
        )
        self.cursor = self.connection.cursor()

    def create_tables(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS Clientes (
                cliente_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200))""",
            """CREATE TABLE IF NOT EXISTS Personal (
                personal_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                rol VARCHAR(50))""",
            """CREATE TABLE IF NOT EXISTS Producto (
                producto_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INT)""",
            """CREATE TABLE IF NOT EXISTS Factura (
                factura_id INT PRIMARY KEY,
                cliente_id INT,
                personal_id INT,
                fecha DATETIME,
                total DECIMAL(10,2),
                FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id),
                FOREIGN KEY (personal_id) REFERENCES Personal(personal_id))""",
            """CREATE TABLE IF NOT EXISTS Detalle_Factura (
                detalle_id INT PRIMARY KEY AUTO_INCREMENT,
                factura_id INT,
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                FOREIGN KEY (factura_id) REFERENCES Factura(factura_id),
                FOREIGN KEY (producto_id) REFERENCES Producto(producto_id))"""
        ]
        for q in queries:
            self.cursor.execute(q)
        self.connection.commit()

    def execute_sp(self, sp_name, params):
        return {"msg": "SP simulado para MySQL"}

# Conector SQL Server
class SQLServerConnector(DatabaseConnector):
    def connect(self, server, database, username, password):
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        self.connection = pyodbc.connect(conn_str)
        self.cursor = self.connection.cursor()

    def create_tables(self):
        queries = [
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Clientes')
               CREATE TABLE Clientes (
                   cliente_id INT PRIMARY KEY,
                   nombre VARCHAR(100),
                   email VARCHAR(100),
                   telefono VARCHAR(20),
                   direccion VARCHAR(200))""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Personal')
               CREATE TABLE Personal (
                   personal_id INT PRIMARY KEY,
                   nombre VARCHAR(100),
                   rol VARCHAR(50))""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Producto')
               CREATE TABLE Producto (
                   producto_id INT PRIMARY KEY,
                   nombre VARCHAR(100),
                   precio DECIMAL(10,2),
                   stock INT)""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Factura')
               CREATE TABLE Factura (
                   factura_id INT PRIMARY KEY,
                   cliente_id INT FOREIGN KEY REFERENCES Clientes(cliente_id),
                   personal_id INT FOREIGN KEY REFERENCES Personal(personal_id),
                   fecha DATETIME,
                   total DECIMAL(10,2))""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Detalle_Factura')
               CREATE TABLE Detalle_Factura (
                   detalle_id INT PRIMARY KEY,
                   factura_id INT FOREIGN KEY REFERENCES Factura(factura_id),
                   producto_id INT FOREIGN KEY REFERENCES Producto(producto_id),
                   cantidad INT,
                   precio_unitario DECIMAL(10,2),
                   subtotal DECIMAL(10,2))"""
        ]
        for q in queries:
            self.cursor.execute(q)
        self.connection.commit()

    def execute_sp(self, sp_name, params):
        return {"msg": "SP simulado para SQL Server"}

# Obtener credenciales simuladas
def get_db_credentials(db_name):
    if db_name == "MySQL":
        return {
            "server": "localhost",
            "port": 3308,  # Cambia si usas otro
            "database": "facturacion",
            "username": "root",
            "password": "admin"
        }
    elif db_name == "SQLServer":
        return {
            "server": "localhost",
            "database": "facturacion",
            "username": "admin",
            "password": "password"
        }

# Interfaz principal
def main():
    st.title("📦 Comparación de Bases de Datos - Prueba de Conexión")

    db_options = ["MySQL", "SQLServer"]
    selected_db = st.selectbox("Selecciona Base de Datos", db_options)

    if st.button("Probar Conexión y Crear Tablas"):
        db_class = MySQLConnector if selected_db == "MySQL" else SQLServerConnector
        db = db_class(selected_db)
        creds = get_db_credentials(selected_db)

        try:
            db.connect(**creds)
            db.create_tables()
            st.success(f"✅ Conexión y creación de tablas en {selected_db} completadas.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
        finally:
            db.disconnect()

if __name__ == "__main__":
    main()
