import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime
import sys
import os

# Configuración de la página
st.set_page_config(page_title="Comparación de Bases de Datos", layout="wide")

# Diccionario para almacenar los tiempos de cada operación
if 'performance_data' not in st.session_state:
    st.session_state.performance_data = {
    'database': [],
    'operation': [],
    'time_ms': []
}

performance_data = st.session_state.performance_data

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
class MySQLConnector(DatabaseConnector):
    def connect(self, server, database, port, username, password):
        import mysql.connector
        self.connection = mysql.connector.connect(
            host=server,
            database=database,
            port=port,
            user=username,
            password=password
        )
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


    def generate_test_data(self):
        self.cursor.execute("DELETE FROM Detalle_Factura")
        self.cursor.execute("DELETE FROM Factura")
        self.cursor.execute("DELETE FROM Producto")
        self.cursor.execute("DELETE FROM Clientes")
        self.cursor.execute("DELETE FROM Personal")

        # Datos realistas de nombres y roles
        nombres_clientes = ["Carlos", "María", "José", "Ana", "Luis", "Carmen", "Pedro", "Lucía", "Juan", "Diana",
                            "Jorge", "Patricia", "Fernando", "Verónica", "Miguel", "Isabel", "Diego", "Rosa", "Víctor", "Sandra"]
        apellidos = ["Pérez", "González", "Rodríguez", "López", "Fernández", "Torres", "Ramírez", "Vargas", "Ríos", "Mendoza"]
        roles = ["Vendedor", "Cajero", "Supervisor", "Administrador"]

        productos_ejemplo = [
            ("Laptop HP", 3200.50),
            ("Impresora Epson", 560.99),
            ("Mouse Logitech", 75.90),
            ("Teclado Mecánico", 180.00),
            ("Monitor 24'' Samsung", 899.99),
            ("Silla Ergonómica", 420.00),
            ("Webcam Full HD", 150.00),
            ("Audífonos Sony", 230.75),
            ("Disco SSD 1TB", 350.00),
            ("Memoria RAM 16GB", 290.00),
            ("Smartphone Samsung", 1200.00),
            ("Tablet Lenovo", 850.00),
            ("Router TP-Link", 180.00),
            ("Proyector Epson", 1900.00),
            ("UPS APC", 610.00),
            ("Cámara de seguridad", 390.00),
            ("Microfono Blue", 250.00),
            ("Cargador universal", 90.00),
            ("Pantalla táctil", 1350.00),
            ("Switch de red", 280.00)
        ]

        # Insertar 20 clientes
        for i in range(1, 21):
            nombre = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            email = f"{nombre.replace(' ', '.').lower()}@gmail.com"
            telefono = f"9{random.randint(10000000, 99999999)}"
            direccion = f"Avenida {random.randint(1, 99)} - Mz. {random.choice('ABCDEFGHIJ')} Lote {random.randint(1, 20)}"
            self.cursor.execute("""
                INSERT INTO Clientes (cliente_id, nombre, email, telefono, direccion)
                VALUES (%s, %s, %s, %s, %s)
            """, (i, nombre, email, telefono, direccion))

        # Insertar 20 empleados
        for i in range(1, 21):
            nombre = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            rol = random.choice(roles)
            self.cursor.execute("""
                INSERT INTO Personal (personal_id, nombre, rol)
                VALUES (%s, %s, %s)
            """, (i, nombre, rol))

        # Insertar 20 productos con stock aleatorio
        for i, (nombre_producto, precio) in enumerate(productos_ejemplo, start=1):
            stock = random.randint(20, 100)
            self.cursor.execute("""
                INSERT INTO Producto (producto_id, nombre, precio, stock)
                VALUES (%s, %s, %s, %s)
            """, (i, nombre_producto, precio, stock))

        self.connection.commit()

    # Dentro de tu clase MySQLConnector
    def search_client(self):
        self.cursor.execute("SELECT * FROM Clientes WHERE nombre LIKE '%%'")
        return self.cursor.fetchall()

    def search_product(self):
        self.cursor.execute("SELECT * FROM Producto WHERE nombre LIKE '%%'")
        return self.cursor.fetchall()

    def generate_invoice(self):
        self.cursor.execute("SELECT cliente_id FROM Clientes LIMIT 1")
        cliente_id = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT personal_id FROM Personal LIMIT 1")
        personal_id = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT producto_id, precio FROM Producto LIMIT 1")
        producto = self.cursor.fetchone()
        producto_id, precio = producto[0], producto[1]

        self.cursor.execute("SELECT IFNULL(MAX(factura_id), 0) + 1 FROM Factura")
        factura_id = self.cursor.fetchone()[0]

        self.cursor.execute("""
            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
            VALUES (%s, %s, %s, NOW(), %s)
        """, (factura_id, cliente_id, personal_id, precio))

        self.cursor.execute("""
            INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
            VALUES ((SELECT IFNULL(MAX(detalle_id), 0) + 1), %s, %s, %s, %s, %s)
        """, (factura_id, producto_id, 1, precio, precio))

        self.connection.commit()

    def query_invoice(self):
        self.cursor.execute("SELECT * FROM Factura ORDER BY factura_id DESC LIMIT 5")
        return self.cursor.fetchall()

    def sales_report(self):
        self.cursor.execute("""
            SELECT DATE(fecha) AS Fecha, SUM(total) AS Total
            FROM Factura
            GROUP BY DATE(fecha)
            ORDER BY Fecha DESC
        """)
        return self.cursor.fetchall()





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
        "SQLServer": None,
        "Oracle": None,  # Se implementaría similar a SQLServer
        "DB2": DB2Connector("DB2"),
        "PostgreSQL": None,
        "MySQL": MySQLConnector("MySQL"),
        "Cassandra": None,
        "MongoDB": None
    }
    
    if choice == "Mantenedores":
        st.header("Mantenedores de Tablas")
        
        tab_options = ["Clientes", "Personal", "Producto", "Factura", "Detalle Factura"]
        tab_choice = st.selectbox("Seleccione tabla", tab_options)
        
        if tab_choice == "Clientes":
            st.subheader("Mantenedor de Clientes")

            db = databases["MySQL"]
            db.connect(**get_db_credentials("MySQL"))

            # Implementar CRUD para clientes
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Listado de Clientes")
                # Mostrar tabla de clientes
                st.write("📋 Listado de Clientes")
                db.cursor.execute("SELECT * FROM Clientes")
                rows = db.cursor.fetchall()
                df_clientes = pd.DataFrame(rows, columns=["ID", "Nombre", "Email", "Teléfono", "Dirección"])
                st.dataframe(df_clientes)
                
            with col2:
                st.write("Agregar/Editar Cliente")
                # Formulario para agregar/editar
                cliente_id = st.number_input("ID Cliente", min_value=1)
                nombre = st.text_input("Nombre")
                email = st.text_input("Email")
                telefono = st.text_input("Teléfono")
                direccion = st.text_input("Dirección")

                if st.button("Guardar Cliente"):
                    try:
                        db.cursor.execute("SELECT COUNT(*) FROM Clientes WHERE cliente_id = %s", (cliente_id,))
                        existe = db.cursor.fetchone()[0] > 0

                        if existe:
                            db.cursor.execute("""
                                UPDATE Clientes
                                SET nombre = %s, email = %s, telefono = %s, direccion = %s
                                WHERE cliente_id = %s
                            """, (nombre, email, telefono, direccion, cliente_id))
                            st.success("✅ Cliente actualizado correctamente.")
                        else:
                            db.cursor.execute("""
                                INSERT INTO Clientes (cliente_id, nombre, email, telefono, direccion)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (cliente_id, nombre, email, telefono, direccion))
                            st.success("✅ Cliente agregado correctamente.")

                        db.connection.commit()
                    except Exception as e:
                        st.error(f"❌ Error al guardar cliente: {str(e)}")

            # Eliminar cliente
            st.markdown("---")
            eliminar_id = st.number_input("ID Cliente a eliminar", min_value=1, key="del")
            if st.button("Eliminar Cliente"):
                try:
                    db.cursor.execute("DELETE FROM Clientes WHERE cliente_id = %s", (eliminar_id,))
                    db.connection.commit()
                    st.success("🗑️ Cliente eliminado correctamente.")
                except Exception as e:
                    st.error(f"❌ Error al eliminar cliente: {str(e)}")
        elif tab_choice=="Personal":
            st.subheader("Mantenedor de Personal")

            db = databases["MySQL"]
            db.connect(**get_db_credentials("MySQL"))

            # Diseño en columnas
            col1, col2 = st.columns(2)

            with col1:
                st.write("📋 Listado de Personal")
                try:
                    db.cursor.execute("SELECT * FROM Personal")
                    rows = db.cursor.fetchall()
                    df_personal = pd.DataFrame(rows, columns=["ID", "Nombre", "Rol"])
                    st.dataframe(df_personal)
                except Exception as e:
                    st.error(f"❌ Error al cargar personal: {str(e)}")

            with col2:
                st.write("➕ Agregar o ✏️ Editar Personal")
                personal_id = st.number_input("ID Personal", min_value=1)
                nombre = st.text_input("Nombre completo")
                rol = st.selectbox("Rol", ["Vendedor", "Cajero", "Supervisor", "Administrador", "Otro"])

                if st.button("Guardar Personal"):
                    try:
                        db.cursor.execute("SELECT COUNT(*) FROM Personal WHERE personal_id = %s", (personal_id,))
                        existe = db.cursor.fetchone()[0] > 0

                        if existe:
                            db.cursor.execute("""
                                UPDATE Personal
                                SET nombre = %s, rol = %s
                                WHERE personal_id = %s
                            """, (nombre, rol, personal_id))
                            st.success("✅ Personal actualizado correctamente.")
                        else:
                            db.cursor.execute("""
                                INSERT INTO Personal (personal_id, nombre, rol)
                                VALUES (%s, %s, %s)
                            """, (personal_id, nombre, rol))
                            st.success("✅ Personal agregado correctamente.")

                        db.connection.commit()
                    except Exception as e:
                        st.error(f"❌ Error al guardar personal: {str(e)}")

            # Sección para eliminar personal
            st.markdown("---")
            eliminar_personal_id = st.number_input("ID Personal a eliminar", min_value=1, key="del_personal")
            if st.button("Eliminar Personal"):
                try:
                    db.cursor.execute("DELETE FROM Personal WHERE personal_id = %s", (eliminar_personal_id,))
                    db.connection.commit()
                    st.success("🗑️ Personal eliminado correctamente.")
                except Exception as e:
                    st.error(f"❌ Error al eliminar personal: {str(e)}")

        elif tab_choice=="Producto":
            st.subheader("Mantenedor de Productos")

            db = databases["MySQL"]
            db.connect(**get_db_credentials("MySQL"))

            col1, col2 = st.columns(2)

            # Mostrar listado de productos
            with col1:
                st.write("📦 Listado de Productos")
                try:
                    db.cursor.execute("SELECT * FROM Producto")
                    rows = db.cursor.fetchall()
                    df_productos = pd.DataFrame(rows, columns=["ID", "Nombre", "Precio (S/)", "Stock"])
                    st.dataframe(df_productos)
                except Exception as e:
                    st.error(f"❌ Error al cargar productos: {str(e)}")

            # Formulario para agregar/editar productos
            with col2:
                st.write("➕ Agregar o ✏️ Editar Producto")
                producto_id = st.number_input("ID Producto", min_value=1)
                nombre = st.text_input("Nombre del Producto")
                precio = st.number_input("Precio (S/)", min_value=0.0, format="%.2f")
                stock = st.number_input("Stock Disponible", min_value=0)

                if st.button("Guardar Producto"):
                    try:
                        db.cursor.execute("SELECT COUNT(*) FROM Producto WHERE producto_id = %s", (producto_id,))
                        existe = db.cursor.fetchone()[0] > 0

                        if existe:
                            db.cursor.execute("""
                                UPDATE Producto
                                SET nombre = %s, precio = %s, stock = %s
                                WHERE producto_id = %s
                            """, (nombre, precio, stock, producto_id))
                            st.success("✅ Producto actualizado correctamente.")
                        else:
                            db.cursor.execute("""
                                INSERT INTO Producto (producto_id, nombre, precio, stock)
                                VALUES (%s, %s, %s, %s)
                            """, (producto_id, nombre, precio, stock))
                            st.success("✅ Producto agregado correctamente.")

                        db.connection.commit()
                    except Exception as e:
                        st.error(f"❌ Error al guardar producto: {str(e)}")

            # Eliminar producto
            st.markdown("---")
            eliminar_producto_id = st.number_input("ID Producto a eliminar", min_value=1, key="del_producto")
            if st.button("Eliminar Producto"):
                try:
                    db.cursor.execute("DELETE FROM Producto WHERE producto_id = %s", (eliminar_producto_id,))
                    db.connection.commit()
                    st.success("🗑️ Producto eliminado correctamente.")
                except Exception as e:
                    st.error(f"❌ Error al eliminar producto: {str(e)}")
        elif tab_choice == "Factura":
            st.subheader("Mantenedor de Facturas")

            db = databases["MySQL"]
            db.connect(**get_db_credentials("MySQL"))

            col1, col2 = st.columns(2)

            # Mostrar facturas existentes
            with col1:
                st.write("📄 Listado de Facturas")
                try:
                    db.cursor.execute("""
                        SELECT f.factura_id, c.nombre AS cliente, p.nombre AS vendedor, f.fecha, f.total
                        FROM Factura f
                        JOIN Clientes c ON f.cliente_id = c.cliente_id
                        JOIN Personal p ON f.personal_id = p.personal_id
                    """)
                    rows = db.cursor.fetchall()
                    df_facturas = pd.DataFrame(rows, columns=["ID", "Cliente", "Vendedor", "Fecha", "Total (S/)"])
                    st.dataframe(df_facturas)
                except Exception as e:
                    st.error(f"❌ Error al cargar facturas: {str(e)}")

            # Formulario para nueva factura
            with col2:
                st.write("🧾 Registrar Nueva Factura")

                # Obtener IDs y nombres para dropdowns
                try:
                    db.cursor.execute("SELECT cliente_id, nombre FROM Clientes")
                    clientes = db.cursor.fetchall()
                    db.cursor.execute("SELECT personal_id, nombre FROM Personal")
                    personal = db.cursor.fetchall()
                except Exception as e:
                    st.error(f"❌ Error al cargar opciones: {str(e)}")
                    clientes = []
                    personal = []

                cliente_opciones = {f"{c[1]} (ID: {c[0]})": c[0] for c in clientes}
                personal_opciones = {f"{p[1]} (ID: {p[0]})": p[0] for p in personal}

                cliente_id = st.selectbox("Cliente", list(cliente_opciones.keys()))
                personal_id = st.selectbox("Vendedor", list(personal_opciones.keys()))
                fecha = st.date_input("Fecha de Factura", value=datetime.today())
                total = st.number_input("Total (S/)", min_value=0.0, format="%.2f")

                if st.button("Guardar Factura"):
                    try:
                        db.cursor.execute("""
                            SELECT IFNULL(MAX(factura_id), 0) + 1 FROM Factura
                        """)
                        next_id = db.cursor.fetchone()[0]

                        db.cursor.execute("""
                            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            next_id,
                            cliente_opciones[cliente_id],
                            personal_opciones[personal_id],
                            fecha.strftime("%Y-%m-%d"),
                            total
                        ))
                        db.connection.commit()
                        st.success("✅ Factura registrada correctamente.")
                    except Exception as e:
                        st.error(f"❌ Error al guardar factura: {str(e)}")
        elif tab_choice == "Detalle Factura":
            st.subheader("Mantenedor de Detalles de Factura")

            db = databases["MySQL"]
            db.connect(**get_db_credentials("MySQL"))

            col1, col2 = st.columns(2)

            # Mostrar detalles de factura existentes
            with col1:
                st.write("📑 Detalles Registrados")
                try:
                    db.cursor.execute("""
                        SELECT df.detalle_id, f.factura_id, p.nombre AS producto, df.cantidad, df.precio_unitario, df.subtotal
                        FROM Detalle_Factura df
                        JOIN Producto p ON df.producto_id = p.producto_id
                        JOIN Factura f ON df.factura_id = f.factura_id
                    """)
                    rows = db.cursor.fetchall()
                    df_detalles = pd.DataFrame(rows, columns=["ID Detalle", "ID Factura", "Producto", "Cantidad", "Precio Unitario (S/)", "Subtotal (S/)"])
                    st.dataframe(df_detalles)
                except Exception as e:
                    st.error(f"❌ Error al cargar detalles: {str(e)}")

            # Registrar un nuevo detalle
            with col2:
                st.write("🧮 Agregar Detalle a una Factura")

                try:
                    # Obtener facturas disponibles
                    db.cursor.execute("SELECT factura_id FROM Factura")
                    facturas = [f[0] for f in db.cursor.fetchall()]

                    # Obtener productos disponibles
                    db.cursor.execute("SELECT producto_id, nombre, precio FROM Producto")
                    productos = db.cursor.fetchall()
                except Exception as e:
                    st.error(f"❌ Error al cargar facturas o productos: {str(e)}")
                    facturas = []
                    productos = []

                if facturas:
                    factura_id = st.selectbox("Seleccionar Factura", facturas)
                else:
                    st.warning("⚠️ No hay facturas registradas. Por favor registra una factura antes de agregar detalles.")
                    factura_id = None


                producto_opciones = {f"{p[1]} (S/ {p[2]})": (p[0], p[2]) for p in productos}
                producto_seleccionado = st.selectbox("Producto", list(producto_opciones.keys()))
                cantidad = st.number_input("Cantidad", min_value=1)

                if producto_seleccionado:
                    producto_id, precio_unitario = producto_opciones[producto_seleccionado]
                    subtotal = round(precio_unitario * cantidad, 2)
                    st.write(f"💰 Subtotal: **S/ {subtotal:.2f}**")

                if factura_id and st.button("Guardar Detalle"):
                    try:
                        # Obtener siguiente detalle_id
                        db.cursor.execute("SELECT IFNULL(MAX(detalle_id), 0) + 1 FROM Detalle_Factura")
                        next_id = db.cursor.fetchone()[0]

                        # Insertar detalle
                        db.cursor.execute("""
                            INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            next_id, factura_id, producto_id, cantidad, precio_unitario, subtotal
                        ))

                        # Actualizar stock
                        db.cursor.execute("""
                            UPDATE Producto SET stock = stock - %s WHERE producto_id = %s
                        """, (cantidad, producto_id))

                        # Recalcular total de la factura
                        db.cursor.execute("""
                            SELECT SUM(subtotal) FROM Detalle_Factura WHERE factura_id = %s
                        """, (factura_id,))
                        nuevo_total = db.cursor.fetchone()[0] or 0

                        db.cursor.execute("""
                            UPDATE Factura SET total = %s WHERE factura_id = %s
                        """, (nuevo_total, factura_id))

                        db.connection.commit()
                        st.success("✅ Detalle agregado correctamente.")
                    except Exception as e:
                        st.error(f"❌ Error al guardar detalle: {str(e)}")
        db.disconnect()
    
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
        "port": "3308",
        "username": "root",
        "password": "admin"
    }

if __name__ == "__main__":
    main()




