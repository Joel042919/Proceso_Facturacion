import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import sys
import os

# Configuración de la página
st.set_page_config(page_title="Comparación de Bases de Datos", layout="wide")

# Diccionario para almacenar los tiempos de cada operación
performance_data = {
    'database': [],
    'operation': [],
    'time_ms': []
}

# Clase base para la conexión a bases de datos
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
        execution_time = (time.time() - start_time) * 1000  # ms
        return execution_time
    
    def execute_sp(self, sp_name, params):
        start_time = time.time()
        # Implementación específica para cada base de datos
        execution_time = (time.time() - start_time) * 1000  # ms
        return execution_time
    
    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = (time.time() - start_time) * 1000  # ms
        
        performance_data['database'].append(self.db_type)
        performance_data['operation'].append(operation_name)
        performance_data['time_ms'].append(execution_time)
        
        return result, execution_time
    
    def create_tables(self):
        raise NotImplementedError
    
    def create_stored_procedures(self):
        raise NotImplementedError
    
    def generate_test_data(self):
        raise NotImplementedError

# Implementaciones específicas para cada base de datos (simplificadas para el ejemplo)
class SQLServerConnector(DatabaseConnector):
    def connect(self, server, database, username, password):
        import pyodbc
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        self.connection = pyodbc.connect(conn_str)
        self.cursor = self.connection.cursor()
        
    def create_tables(self):
        queries = [
            """CREATE TABLE Clientes (
                cliente_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200))""",
            """CREATE TABLE Personal (
                personal_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                rol VARCHAR(50))""",
            """CREATE TABLE Producto (
                producto_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INT)""",
            """CREATE TABLE Factura (
                factura_id INT PRIMARY KEY,
                cliente_id INT FOREIGN KEY REFERENCES Clientes(cliente_id),
                personal_id INT FOREIGN KEY REFERENCES Personal(personal_id),
                fecha DATETIME,
                total DECIMAL(10,2))""",
            """CREATE TABLE Detalle_Factura (
                detalle_id INT PRIMARY KEY,
                factura_id INT FOREIGN KEY REFERENCES Factura(factura_id),
                producto_id INT FOREIGN KEY REFERENCES Producto(producto_id),
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2))"""
        ]
        
        for query in queries:
            self.execute_query(query)
            
    def create_stored_procedures(self):
        # Procedimiento para generar una factura
        sp_query = """
        CREATE PROCEDURE sp_generar_factura
            @cliente_id INT,
            @personal_id INT,
            @productos_json NVARCHAR(MAX)
        AS
        BEGIN
            BEGIN TRANSACTION;
            
            DECLARE @factura_id INT;
            DECLARE @total DECIMAL(10,2) = 0;
            
            -- Obtener el próximo ID de factura
            SELECT @factura_id = ISNULL(MAX(factura_id), 0) + 1 FROM Factura;
            
            -- Insertar la factura
            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
            VALUES (@factura_id, @cliente_id, @personal_id, GETDATE(), 0);
            
            -- Procesar los productos
            DECLARE @productos TABLE (
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2)
            );
            
            INSERT INTO @productos
            SELECT 
                producto_id, 
                cantidad,
                (SELECT precio FROM Producto WHERE producto_id = j.producto_id) as precio_unitario
            FROM OPENJSON(@productos_json)
            WITH (
                producto_id INT '$.producto_id',
                cantidad INT '$.cantidad'
            ) j;
            
            -- Insertar detalles y calcular total
            INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
            SELECT 
                ROW_NUMBER() OVER (ORDER BY producto_id) + ISNULL((SELECT MAX(detalle_id) FROM Detalle_Factura), 0),
                @factura_id,
                producto_id,
                cantidad,
                precio_unitario,
                cantidad * precio_unitario
            FROM @productos;
            
            -- Actualizar stock y calcular total
            UPDATE p
            SET p.stock = p.stock - pr.cantidad
            FROM Producto p
            JOIN @productos pr ON p.producto_id = pr.producto_id;
            
            SELECT @total = SUM(cantidad * precio_unitario) FROM @productos;
            
            -- Actualizar total de factura
            UPDATE Factura SET total = @total WHERE factura_id = @factura_id;
            
            COMMIT TRANSACTION;
            
            SELECT @factura_id as factura_id, @total as total;
        END
        """
        self.execute_query(sp_query)

class DB2Connector(DatabaseConnector):
    def connect(self, server, database, username, password):
        import ibm_db
        conn_str = f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        self.connection = ibm_db.connect(database,username,password)

    def create_tables(self):
        queries=[
            """CREATE TABLE Clientes (
                cliente_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200))""",
            """CREATE TABLE Personal (
                personal_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                rol VARCHAR(50))""",
            """CREATE TABLE Producto (
                producto_id INT PRIMARY KEY,
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INT)""",
            """CREATE TABLE Factura (
                factura_id INT PRIMARY KEY,
                cliente_id INT FOREIGN KEY REFERENCES Clientes(cliente_id),
                personal_id INT FOREIGN KEY REFERENCES Personal(personal_id),
                fecha DATETIME,
                total DECIMAL(10,2))""",
            """CREATE TABLE Detalle_Factura (
                detalle_id INT PRIMARY KEY,
                factura_id INT FOREIGN KEY REFERENCES Factura(factura_id),
                producto_id INT FOREIGN KEY REFERENCES Producto(producto_id),
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2))"""
        ]
        for query in queries:
            self.execute_query(query)
    
    def create_stored_procedures(self):
        sp_query = """
            CREATE PROCEDURE sp_generar_factura
            @cliente_id INT,
            @personal_id INT,
            @productos_json NVARCHAR(MAX)
        AS
        BEGIN
            BEGIN TRANSACTION;
            
            DECLARE @factura_id INT;
            DECLARE @total DECIMAL(10,2) = 0;
            
            -- Obtener el próximo ID de factura
            SELECT @factura_id = ISNULL(MAX(factura_id), 0) + 1 FROM Factura;
            
            -- Insertar la factura
            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
            VALUES (@factura_id, @cliente_id, @personal_id, GETDATE(), 0);
            
            -- Procesar los productos
            DECLARE @productos TABLE (
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2)
            );
            
            INSERT INTO @productos
            SELECT 
                producto_id, 
                cantidad,
                (SELECT precio FROM Producto WHERE producto_id = j.producto_id) as precio_unitario
            FROM OPENJSON(@productos_json)
            WITH (
                producto_id INT '$.producto_id',
                cantidad INT '$.cantidad'
            ) j;
            
            -- Insertar detalles y calcular total
            INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
            SELECT 
                ROW_NUMBER() OVER (ORDER BY producto_id) + ISNULL((SELECT MAX(detalle_id) FROM Detalle_Factura), 0),
                @factura_id,
                producto_id,
                cantidad,
                precio_unitario,
                cantidad * precio_unitario
            FROM @productos;
            
            -- Actualizar stock y calcular total
            UPDATE p
            SET p.stock = p.stock - pr.cantidad
            FROM Producto p
            JOIN @productos pr ON p.producto_id = pr.producto_id;
            
            SELECT @total = SUM(cantidad * precio_unitario) FROM @productos;
            
            -- Actualizar total de factura
            UPDATE Factura SET total = @total WHERE factura_id = @factura_id;
            
            COMMIT TRANSACTION;
            
            SELECT @factura_id as factura_id, @total as total;
        END
        """
        self.execute_query(sp_query)


# Clases similares para Oracle, DB2, PostgreSQL, MySQL, Cassandra, MongoDB
# (Implementaciones omitidas por brevedad, pero seguirían el mismo patrón)

# Interfaz de usuario con Streamlit
def main():
    st.title("Sistema de Comparación de Bases de Datos para Facturación")
    
    # Menú lateral
    menu_options = [
        "Mantenedores",
        "Generar Datos de Prueba",
        "Ejecutar Pruebas de Rendimiento",
        "Resultados y Estadísticas",
        "Proceso de Facturación"
    ]
    choice = st.sidebar.selectbox("Menú", menu_options)
    
    # Conexiones a bases de datos (simuladas para el ejemplo)
    databases = {
        "SQLServer": SQLServerConnector("SQLServer"),
        "Oracle": None,  # Se implementaría similar a SQLServer
        "DB2": DB2Connector("DB2"),
        "PostgreSQL": None,
        "MySQL": None,
        "Cassandra": None,
        "MongoDB": None
    }
    
    if choice == "Mantenedores":
        st.header("Mantenedores de Tablas")
        
        tab_options = ["Clientes", "Personal", "Producto", "Factura", "Detalle Factura"]
        tab_choice = st.selectbox("Seleccione tabla", tab_options)
        
        if tab_choice == "Clientes":
            st.subheader("Mantenedor de Clientes")
            # Implementar CRUD para clientes
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Listado de Clientes")
                # Mostrar tabla de clientes
                
            with col2:
                st.write("Agregar/Editar Cliente")
                # Formulario para agregar/editar
    
    elif choice == "Generar Datos de Prueba":
        st.header("Generar Datos de Prueba")
        
        if st.button("Generar Datos en Todas las Bases de Datos"):
            with st.spinner("Generando datos de prueba..."):
                for db_name, db_conn in databases.items():
                    if db_conn:
                        try:
                            db_conn.connect(**get_db_credentials(db_name))
                            db_conn.generate_test_data()
                            st.success(f"Datos generados en {db_name}")
                        except Exception as e:
                            st.error(f"Error en {db_name}: {str(e)}")
                        finally:
                            db_conn.disconnect()
    
    elif choice == "Ejecutar Pruebas de Rendimiento":
        st.header("Pruebas de Rendimiento")
        
        if st.button("Ejecutar Todas las Pruebas"):
            test_operations = [
                ("Carga inicial", "generate_test_data"),
                ("Búsqueda de cliente", "search_client"),
                ("Búsqueda de producto", "search_product"),
                ("Generación de factura", "generate_invoice"),
                ("Consulta de factura", "query_invoice"),
                ("Reporte de ventas", "sales_report")
            ]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, (op_name, op_func) in enumerate(test_operations):
                status_text.text(f"Ejecutando: {op_name}...")
                
                for db_name, db_conn in databases.items():
                    if db_conn:
                        try:
                            db_conn.connect(**get_db_credentials(db_name))
                            db_conn.measure_time(op_name, getattr(db_conn, op_func))
                            st.write(f"{db_name} - {op_name}: OK")
                        except Exception as e:
                            st.write(f"{db_name} - {op_name}: Error - {str(e)}")
                        finally:
                            db_conn.disconnect()
                
                progress_bar.progress((i + 1) / len(test_operations))
            
            status_text.text("Pruebas completadas!")
            st.balloons()
    
    elif choice == "Resultados y Estadísticas":
        st.header("Resultados de Rendimiento")
        
        if not performance_data['database']:
            st.warning("No hay datos de rendimiento disponibles. Ejecute las pruebas primero.")
        else:
            df = pd.DataFrame(performance_data)
            
            st.subheader("Datos Crudos de Tiempos de Ejecución")
            st.dataframe(df)
            
            st.subheader("Resumen Estadístico")
            st.dataframe(df.groupby(['database', 'operation'])['time_ms'].describe())
            
            st.subheader("Gráficos Comparativos")
            
            # Gráfico de barras por operación
            fig, ax = plt.subplots(figsize=(12, 6))
            df.pivot(index='database', columns='operation', values='time_ms').plot(kind='bar', ax=ax)
            ax.set_title("Tiempo de Ejecución por Operación y Base de Datos")
            ax.set_ylabel("Tiempo (ms)")
            ax.set_xlabel("Base de Datos")
            st.pyplot(fig)
            
            # Gráfico de líneas para comparación
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            for db in df['database'].unique():
                db_data = df[df['database'] == db]
                ax2.plot(db_data['operation'], db_data['time_ms'], label=db, marker='o')
            
            ax2.set_title("Comparación de Rendimiento entre Bases de Datos")
            ax2.set_ylabel("Tiempo (ms)")
            ax2.set_xlabel("Operación")
            ax2.legend()
            ax2.grid(True)
            st.pyplot(fig2)
    
    elif choice == "Proceso de Facturación":
        st.header("Proceso de Facturación")
        
        # Simulador de facturación
        with st.form("facturacion_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                cliente_id = st.number_input("ID Cliente", min_value=1, value=1)
                personal_id = st.number_input("ID Vendedor", min_value=1, value=1)
            
            with col2:
                productos = st.text_area("Productos (JSON)", value='[{"producto_id": 1, "cantidad": 2}]')
                db_seleccionada = st.selectbox("Base de Datos", list(databases.keys()))
            
            if st.form_submit_button("Generar Factura"):
                if db_seleccionada in databases and databases[db_seleccionada]:
                    try:
                        db_conn = databases[db_seleccionada]
                        db_conn.connect(**get_db_credentials(db_seleccionada))
                        
                        _, exec_time = db_conn.measure_time(
                            "Generación de Factura (UI)",
                            db_conn.execute_sp,
                            "sp_generar_factura",
                            (cliente_id, personal_id, productos)
                        )
                        
                        st.success(f"Factura generada en {db_seleccionada} en {exec_time:.2f} ms")
                    except Exception as e:
                        st.error(f"Error al generar factura: {str(e)}")
                    finally:
                        db_conn.disconnect()
                else:
                    st.warning("Base de datos no disponible")

# Función auxiliar para obtener credenciales (simulada)
def get_db_credentials(db_name):
    # En un caso real, esto obtendría credenciales de un archivo de configuración o variables de entorno
    return {
        "server": "localhost",
        "database": "facturacion",
        "username": "admin",
        "password": "password"
    }

if __name__ == "__main__":
    main()




