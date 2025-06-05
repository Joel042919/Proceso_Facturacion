import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from datetime import datetime
import json

# Configuración de la página
st.set_page_config(page_title="Sistema de Facturación con SQL Server", layout="wide")

# Diccionario para almacenar los tiempos de cada operación
performance_data = {
    'database': [],
    'operation': [],
    'time_ms': []
}

class SQLServerConnector:
    def __init__(self):
        self.db_type = "SQLServer"
        self.connection = None
        self.cursor = None

    def connect(self, server, database, username, password):
        try:
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            st.error(f"Error al conectar a SQL Server: {str(e)}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        start_time = time.time()
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            execution_time = (time.time() - start_time) * 1000  # ms
            return execution_time
        except Exception as e:
            self.connection.rollback()
            st.error(f"Error en la consulta: {str(e)}")
            return None

    def execute_sp(self, sp_name, params):
      start_time = time.time()
      try:
          placeholders = ','.join(['?'] * len(params))
          query = f"{{CALL {sp_name} ({placeholders})}}"
          self.cursor.execute(query, params)

          # Intentar obtener resultados solo si hay alguno
          try:
              result = self.cursor.fetchall()
              print("Resultado directo del SP:", result);
          except pyodbc.ProgrammingError:
              # No hay resultados para fetch
              result = None

          self.connection.commit()
          execution_time = (time.time() - start_time) * 1000  # ms
          return result, execution_time
      except Exception as e:
          self.connection.rollback()
          st.error(f"Error al ejecutar SP {sp_name}: {str(e)}")
          return None, None

    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = (time.time() - start_time) * 1000  # ms

        performance_data['database'].append(self.db_type)
        performance_data['operation'].append(operation_name)
        performance_data['time_ms'].append(execution_time)

        return result, execution_time

    def create_tables(self):
        try:
            # Verificar si las tablas ya existen
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Clientes' AND xtype='U')
                CREATE TABLE Clientes (
                    cliente_id INT PRIMARY KEY,
                    nombre VARCHAR(100),
                    email VARCHAR(100),
                    telefono VARCHAR(20),
                    direccion VARCHAR(200)
                )
            """)

            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Personal' AND xtype='U')
                CREATE TABLE Personal (
                    personal_id INT PRIMARY KEY,
                    nombre VARCHAR(100),
                    rol VARCHAR(50)
                )
            """)

            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Producto' AND xtype='U')
                CREATE TABLE Producto (
                    producto_id INT PRIMARY KEY,
                    nombre VARCHAR(100),
                    precio DECIMAL(10,2),
                    stock INT
                )
            """)

            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Factura' AND xtype='U')
                CREATE TABLE Factura (
                    factura_id INT PRIMARY KEY,
                    cliente_id INT FOREIGN KEY REFERENCES Clientes(cliente_id),
                    personal_id INT FOREIGN KEY REFERENCES Personal(personal_id),
                    fecha DATETIME,
                    total DECIMAL(10,2)
                )
            """)

            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Detalle_Factura' AND xtype='U')
                CREATE TABLE Detalle_Factura (
                    detalle_id INT PRIMARY KEY,
                    factura_id INT FOREIGN KEY REFERENCES Factura(factura_id),
                    producto_id INT FOREIGN KEY REFERENCES Producto(producto_id),
                    cantidad INT,
                    precio_unitario DECIMAL(10,2),
                    subtotal DECIMAL(10,2)
                )
            """)

            self.connection.commit()
            return True
        except Exception as e:
            self.connection.rollback()
            st.error(f"Error al crear tablas: {str(e)}")
            return False

    def create_stored_procedures(self):
        try:
            # Verificar si el stored procedure ya existe
            self.cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'sp_generar_factura')
                EXEC('
                CREATE PROCEDURE sp_generar_factura
                    @cliente_id INT,
                    @personal_id INT,
                    @productos_json NVARCHAR(MAX)
                AS
                BEGIN
                    BEGIN TRY
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
                            producto_id INT ''$.producto_id'',
                            cantidad INT ''$.cantidad''
                        ) j;

                        -- Insertar detalles y calcular total
                        INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
                        SELECT
                            ISNULL((SELECT MAX(detalle_id) FROM Detalle_Factura), 0) + ROW_NUMBER() OVER (ORDER BY producto_id),
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
                    END TRY
                    BEGIN CATCH
                        ROLLBACK TRANSACTION;
                        THROW;
                    END CATCH
                END
                ')
            """)

            self.connection.commit()
            return True
        except Exception as e:
            self.connection.rollback()
            st.error(f"Error al crear stored procedures: {str(e)}")
            return False

    def generate_test_data(self):
        try:
            # Insertar datos de prueba solo si las tablas están vacías
            self.cursor.execute("SELECT COUNT(*) FROM Clientes")
            if self.cursor.fetchone()[0] == 0:
                # Insertar clientes
                clientes = [
                    (1, 'Juan Pérez', 'juan@example.com', '555-1234', 'Calle 123'),
                    (2, 'María Gómez', 'maria@example.com', '555-5678', 'Avenida 456'),
                    (3, 'Carlos Ruiz', 'carlos@example.com', '555-9012', 'Boulevard 789')
                ]
                self.cursor.executemany("INSERT INTO Clientes VALUES (?, ?, ?, ?, ?)", clientes)

                # Insertar personal
                personal = [
                    (1, 'Ana López', 'Vendedor'),
                    (2, 'Pedro Martínez', 'Gerente'),
                    (3, 'Luisa Fernández', 'Cajero')
                ]
                self.cursor.executemany("INSERT INTO Personal VALUES (?, ?, ?)", personal)

                # Insertar productos
                productos = [
                    (1, 'Laptop', 1200.00, 50),
                    (2, 'Teléfono', 800.00, 100),
                    (3, 'Tablet', 400.00, 75),
                    (4, 'Monitor', 300.00, 40),
                    (5, 'Teclado', 50.00, 120)
                ]
                self.cursor.executemany("INSERT INTO Producto VALUES (?, ?, ?, ?)", productos)

                self.connection.commit()
                return True
            return False
        except Exception as e:
            self.connection.rollback()
            st.error(f"Error al generar datos de prueba: {str(e)}")
            return False

    def search_client(self, client_id=None, name=None):
        try:
            if client_id:
                query = "SELECT * FROM Clientes WHERE cliente_id = ?"
                self.cursor.execute(query, client_id)
            elif name:
                query = "SELECT * FROM Clientes WHERE nombre LIKE ?"
                self.cursor.execute(query, f'%{name}%')
            else:
                query = "SELECT * FROM Clientes"
                self.cursor.execute(query)

            columns = [column[0] for column in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return results
        except Exception as e:
            st.error(f"Error al buscar cliente: {str(e)}")
            return []

    def search_product(self, product_id=None, name=None):
        try:
            if product_id:
                query = "SELECT * FROM Producto WHERE producto_id = ?"
                self.cursor.execute(query, product_id)
            elif name:
                query = "SELECT * FROM Producto WHERE nombre LIKE ?"
                self.cursor.execute(query, f'%{name}%')
            else:
                query = "SELECT * FROM Producto"
                self.cursor.execute(query)

            columns = [column[0] for column in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return results
        except Exception as e:
            st.error(f"Error al buscar producto: {str(e)}")
            return []

    def generate_invoice(self, client_id, employee_id, products):
        try:
            # Convertir productos a JSON
            productos_json = json.dumps(products)

            # Ejecutar el stored procedure
            result, _ = self.execute_sp("sp_generar_factura", (client_id, employee_id, productos_json))

            if result:
                # El SP devuelve factura_id y total
                factura_id = result[0][0]
                total = result[0][1]

                # Ahora obtener los detalles completos de la factura
                invoice_details = self.query_invoice(factura_id)

                if invoice_details:
                    return {
                        'factura_id': factura_id,
                        'total': float(total),
                        'details': invoice_details
                    }

            return None
        except Exception as e:
            st.error(f"Error al generar factura: {str(e)}")
            return None

    def query_invoice(self, invoice_id):
        try:
            # Consultar la factura
            query = """
                SELECT f.factura_id, f.fecha, f.total,
                      c.nombre as cliente_nombre,
                      p.nombre as personal_nombre
                FROM Factura f
                JOIN Clientes c ON f.cliente_id = c.cliente_id
                JOIN Personal p ON f.personal_id = p.personal_id
                WHERE f.factura_id = ?
            """
            self.cursor.execute(query, invoice_id)
            factura_row = self.cursor.fetchone()

            if not factura_row:
                return None

            # Obtener nombres de columnas
            columns = [column[0] for column in self.cursor.description]
            factura_dict = dict(zip(columns, factura_row))

            # Consultar los detalles
            query = """
                SELECT df.detalle_id, pr.nombre as producto_nombre,
                      df.cantidad, df.precio_unitario, df.subtotal
                FROM Detalle_Factura df
                JOIN Producto pr ON df.producto_id = pr.producto_id
                WHERE df.factura_id = ?
                ORDER BY df.detalle_id
            """
            self.cursor.execute(query, invoice_id)
            detalles_rows = self.cursor.fetchall()

            # Formatear los detalles
            detalles_columns = ['detalle_id', 'producto_nombre', 'cantidad', 'precio_unitario', 'subtotal']
            detalles_list = [dict(zip(detalles_columns, row)) for row in detalles_rows]

            return {
                'factura': {
                    'factura_id': factura_dict['factura_id'],
                    'fecha': factura_dict['fecha'],
                    'total': factura_dict['total'],
                    'cliente_nombre': factura_dict['cliente_nombre'],
                    'personal_nombre': factura_dict['personal_nombre']
                },
                'detalles': detalles_list
            }
        except Exception as e:
            st.error(f"Error al consultar factura: {str(e)}")
            return None

    def sales_report(self, start_date=None, end_date=None):
        try:
            query = """
                SELECT
                    f.factura_id,
                    f.fecha,
                    c.nombre as cliente,
                    p.nombre as vendedor,
                    f.total,
                    COUNT(df.detalle_id) as items
                FROM Factura f
                JOIN Clientes c ON f.cliente_id = c.cliente_id
                JOIN Personal p ON f.personal_id = p.personal_id
                JOIN Detalle_Factura df ON f.factura_id = df.factura_id
            """

            params = []
            if start_date and end_date:
                query += " WHERE f.fecha BETWEEN ? AND ?"
                params.extend([start_date, end_date])
            elif start_date:
                query += " WHERE f.fecha >= ?"
                params.append(start_date)
            elif end_date:
                query += " WHERE f.fecha <= ?"
                params.append(end_date)

            query += " GROUP BY f.factura_id, f.fecha, c.nombre, p.nombre, f.total"

            self.cursor.execute(query, params)

            columns = [column[0] for column in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return results
        except Exception as e:
            st.error(f"Error al generar reporte de ventas: {str(e)}")
            return []

# Interfaz de usuario con Streamlit
def main():
    st.title("Sistema de Facturación con SQL Server")

    # Inicializar conexión a la base de datos
    if 'db_conn' not in st.session_state:
        st.session_state.db_conn = SQLServerConnector()

    # Menú lateral para configuración de conexión
    with st.sidebar:
        st.header("Configuración de Conexión")
        server = st.text_input("Servidor", "localhost")
        database = st.text_input("Base de Datos", "facturacion")
        username = st.text_input("Usuario", "sa")
        password = st.text_input("Contraseña", type="password")

        if st.button("Conectar a SQL Server"):
            with st.spinner("Conectando..."):
                if st.session_state.db_conn.connect(server, database, username, password):
                    st.success("Conexión exitosa!")
                    # Crear tablas y procedimientos si no existen
                    st.session_state.db_conn.create_tables()
                    st.session_state.db_conn.create_stored_procedures()
                    st.session_state.db_conn.generate_test_data()
                else:
                    st.error("Error al conectar")

        st.header("Menú Principal")
        menu_options = [
            "Mantenedores",
            "Proceso de Facturación",
            "Consultas y Reportes",
            "Rendimiento"
        ]
        choice = st.selectbox("Seleccione una opción", menu_options)

    # Conexión a la base de datos
    if not st.session_state.db_conn.connection:
        st.warning("Por favor, configure y establezca la conexión a la base de datos primero.")
        return

    if choice == "Mantenedores":
        st.header("Mantenedores de Datos")

        tab_options = ["Clientes", "Personal", "Productos"]
        tab_choice = st.selectbox("Seleccione tabla", tab_options)

        if tab_choice == "Clientes":
            st.subheader("Mantenedor de Clientes")

            # Opciones para clientes
            client_options = ["Listar Clientes", "Agregar Cliente", "Buscar Cliente"]
            client_action = st.radio("Acción", client_options)

            if client_action == "Listar Clientes":
                clients = st.session_state.db_conn.search_client()
                if clients:
                    st.dataframe(pd.DataFrame(clients))
                else:
                    st.info("No hay clientes registrados")

            elif client_action == "Agregar Cliente":
                with st.form("add_client_form"):
                    col1, col2 = st.columns(2)

                    with col1:
                        cliente_id = st.number_input("ID Cliente", min_value=1)
                        nombre = st.text_input("Nombre")

                    with col2:
                        email = st.text_input("Email")
                        telefono = st.text_input("Teléfono")

                    direccion = st.text_input("Dirección")

                    if st.form_submit_button("Guardar Cliente"):
                        try:
                            query = """
                                INSERT INTO Clientes (cliente_id, nombre, email, telefono, direccion)
                                VALUES (?, ?, ?, ?, ?)
                            """
                            exec_time = st.session_state.db_conn.execute_query(
                                query, (cliente_id, nombre, email, telefono, direccion))

                            if exec_time is not None:
                                st.success(f"Cliente agregado correctamente en {exec_time:.2f} ms")
                        except Exception as e:
                            st.error(f"Error al agregar cliente: {str(e)}")

            elif client_action == "Buscar Cliente":
                search_term = st.text_input("Ingrese nombre o ID del cliente")
                if search_term:
                    if search_term.isdigit():
                        clients = st.session_state.db_conn.search_client(client_id=int(search_term))
                    else:
                        clients = st.session_state.db_conn.search_client(name=search_term)

                    if clients:
                        st.dataframe(pd.DataFrame(clients))
                    else:
                        st.info("No se encontraron clientes")

        elif tab_choice == "Personal":
            st.subheader("Mantenedor de Personal")

            # Mostrar personal existente
            st.session_state.db_conn.cursor.execute("SELECT * FROM Personal")
            personal = st.session_state.db_conn.cursor.fetchall()

            if personal:
                st.dataframe(pd.DataFrame.from_records(
                    personal,
                    columns=['ID', 'Nombre', 'Rol']
                ))
            else:
                st.info("No hay personal registrado")

            # Formulario para agregar nuevo personal
            with st.form("add_personal_form"):
                col1, col2 = st.columns(2)

                with col1:
                    personal_id = st.number_input("ID Personal", min_value=1)
                    nombre = st.text_input("Nombre")

                with col2:
                    rol = st.selectbox("Rol", ["Vendedor", "Gerente", "Cajero", "Administrativo"])

                if st.form_submit_button("Agregar Personal"):
                    try:
                        query = """
                            INSERT INTO Personal (personal_id, nombre, rol)
                            VALUES (?, ?, ?)
                        """
                        exec_time = st.session_state.db_conn.execute_query(
                            query, (personal_id, nombre, rol))

                        if exec_time is not None:
                            st.success(f"Personal agregado correctamente en {exec_time:.2f} ms")
                            st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error al agregar personal: {str(e)}")

        elif tab_choice == "Productos":
            st.subheader("Mantenedor de Productos")

            # Opciones para productos
            product_options = ["Listar Productos", "Agregar Producto", "Buscar Producto"]
            product_action = st.radio("Acción", product_options)

            if product_action == "Listar Productos":
                products = st.session_state.db_conn.search_product()
                if products:
                    st.dataframe(pd.DataFrame(products))
                else:
                    st.info("No hay productos registrados")

            elif product_action == "Agregar Producto":
                with st.form("add_product_form"):
                    col1, col2 = st.columns(2)

                    with col1:
                        producto_id = st.number_input("ID Producto", min_value=1)
                        nombre = st.text_input("Nombre del Producto")

                    with col2:
                        precio = st.number_input("Precio", min_value=0.0, step=0.01)
                        stock = st.number_input("Stock", min_value=0)

                    if st.form_submit_button("Guardar Producto"):
                        try:
                            query = """
                                INSERT INTO Producto (producto_id, nombre, precio, stock)
                                VALUES (?, ?, ?, ?)
                            """
                            exec_time = st.session_state.db_conn.execute_query(
                                query, (producto_id, nombre, precio, stock))

                            if exec_time is not None:
                                st.success(f"Producto agregado correctamente en {exec_time:.2f} ms")
                        except Exception as e:
                            st.error(f"Error al agregar producto: {str(e)}")

            elif product_action == "Buscar Producto":
                search_term = st.text_input("Ingrese nombre o ID del producto")
                if search_term:
                    if search_term.isdigit():
                        products = st.session_state.db_conn.search_product(product_id=int(search_term))
                    else:
                        products = st.session_state.db_conn.search_product(name=search_term)

                    if products:
                        st.dataframe(pd.DataFrame(products))
                    else:
                        st.info("No se encontraron productos")

    elif choice == "Proceso de Facturación":
        st.header("Proceso de Facturación")

        # Obtener clientes y personal para selección
        clients = st.session_state.db_conn.search_client()
        employees = st.session_state.db_conn.search_product()
        products = st.session_state.db_conn.search_product()

        if not clients or not employees or not products:
            st.warning("No hay suficientes datos para generar una factura. Verifique que existan clientes, personal y productos.")
            return

        with st.form("invoice_form"):
            col1, col2 = st.columns(2)

            with col1:
                # Seleccionar cliente
                cliente_options = {f"{c['cliente_id']} - {c['nombre']}": c['cliente_id'] for c in clients}
                cliente_selected = st.selectbox("Cliente", options=list(cliente_options.keys()))
                cliente_id = cliente_options[cliente_selected]

                # Seleccionar vendedor
                personal_options = {f"{e['producto_id']} - {e['nombre']}": e['producto_id'] for e in employees}
                personal_selected = st.selectbox("Vendedor", options=list(personal_options.keys()))
                personal_id = personal_options[personal_selected]

            with col2:
                st.write("**Productos Disponibles**")
                product_list = [f"{p['producto_id']} - {p['nombre']} (${p['precio']})" for p in products]
                selected_products = st.multiselect("Seleccione productos", options=product_list)

                # Cantidades para cada producto seleccionado
                product_quantities = {}
                for product in selected_products:
                    product_id = int(product.split(' - ')[0])
                    max_stock = next(p['stock'] for p in products if p['producto_id'] == product_id)
                    quantity = st.number_input(
                        f"Cantidad para {product}",
                        min_value=1,
                        max_value=max_stock,
                        value=1,
                        key=f"qty_{product_id}"
                    )
                    product_quantities[product_id] = quantity

            if st.form_submit_button("Generar Factura"):
                if not selected_products:
                    st.warning("Seleccione al menos un producto")
                else:
                    # Preparar datos para el stored procedure
                    productos_json = [
                        {"producto_id": pid, "cantidad": qty}
                        for pid, qty in product_quantities.items()
                    ]

                    with st.spinner("Generando factura..."):
                        result = st.session_state.db_conn.generate_invoice(
                            cliente_id,
                            personal_id,
                            productos_json
                        )

                        if result:
                            st.success(f"Factura #{result['factura_id']} generada con éxito! Total: ${result['total']:.2f}")

                            # Mostrar detalles de la factura
                            invoice_details = st.session_state.db_conn.query_invoice(result['factura_id'])
                            if invoice_details:
                                st.subheader("Detalles de la Factura")

                                col1, col2 = st.columns(2)

                                with col1:
                                    st.write("**Información General**")
                                    st.write(f"**Número:** {invoice_details['factura']['factura_id']}")
                                    st.write(f"**Fecha:** {invoice_details['factura']['fecha'].strftime('%Y-%m-%d %H:%M')}")
                                    st.write(f"**Cliente:** {invoice_details['factura']['cliente_nombre']}")
                                    st.write(f"**Vendedor:** {invoice_details['factura']['personal_nombre']}")
                                    st.write(f"**Total:** ${invoice_details['factura']['total']:.2f}")

                                with col2:
                                    st.write("**Productos**")
                                    detalles_df = pd.DataFrame(invoice_details['detalles'])
                                    st.dataframe(detalles_df)

    elif choice == "Consultas y Reportes":
        st.header("Consultas y Reportes")

        report_options = ["Consulta de Factura", "Reporte de Ventas"]
        report_choice = st.selectbox("Seleccione reporte", report_options)

        if report_choice == "Consulta de Factura":
            st.subheader("Consulta de Factura por Número")

            invoice_id = st.number_input("Ingrese el número de factura", min_value=1)

            if st.button("Buscar Factura"):
                if invoice_id:
                    with st.spinner("Buscando factura..."):
                        invoice_details = st.session_state.db_conn.query_invoice(invoice_id)

                        if invoice_details:
                            st.success("Factura encontrada!")

                            col1, col2 = st.columns(2)

                            with col1:
                                st.write("**Información General**")
                                st.write(f"**Número:** {invoice_details['factura']['factura_id']}")
                                st.write(f"**Fecha:** {invoice_details['factura']['fecha'].strftime('%Y-%m-%d %H:%M')}")
                                st.write(f"**Cliente:** {invoice_details['factura']['cliente_nombre']}")
                                st.write(f"**Vendedor:** {invoice_details['factura']['personal_nombre']}")
                                st.write(f"**Total:** ${invoice_details['factura']['total']:.2f}")

                            with col2:
                                st.write("**Productos**")
                                detalles_df = pd.DataFrame(invoice_details['detalles'])
                                st.dataframe(detalles_df)
                        else:
                            st.warning("No se encontró la factura")

        elif report_choice == "Reporte de Ventas":
            st.subheader("Reporte de Ventas por Período")

            col1, col2 = st.columns(2)

            with col1:
                start_date = st.date_input("Fecha de inicio")

            with col2:
                end_date = st.date_input("Fecha de fin")

            if st.button("Generar Reporte"):
                if start_date and end_date and start_date <= end_date:
                    with st.spinner("Generando reporte..."):
                        sales_data = st.session_state.db_conn.sales_report(
                            start_date.strftime('%Y-%m-%d'),
                            end_date.strftime('%Y-%m-%d')
                        )

                        if sales_data:
                            st.success(f"Reporte generado con {len(sales_data)} ventas")

                            # Convertir a DataFrame para mejor visualización
                            df = pd.DataFrame(sales_data)
                            df['fecha'] = pd.to_datetime(df['fecha']).dt.strftime('%Y-%m-%d %H:%M')
                            df['total'] = df['total'].apply(lambda x: f"${x:.2f}")

                            st.dataframe(df)

                            # Gráfico de ventas por día
                            st.subheader("Ventas por Día")
                            df_daily = df.copy()
                            df_daily['fecha'] = pd.to_datetime(df_daily['fecha']).dt.date
                            daily_sales = df_daily.groupby('fecha')['total'].sum().reset_index()

                            fig, ax = plt.subplots(figsize=(10, 5))
                            ax.bar(
                                daily_sales['fecha'].astype(str),
                                daily_sales['total'].str.replace('$', '').astype(float),
                                color='skyblue'
                            )
                            ax.set_xlabel("Fecha")
                            ax.set_ylabel("Total Ventas ($)")
                            ax.set_title("Ventas por Día")
                            plt.xticks(rotation=45)
                            st.pyplot(fig)
                        else:
                            st.warning("No hay ventas en el período seleccionado")
                else:
                    st.warning("Seleccione un rango de fechas válido")

    elif choice == "Rendimiento":
        st.header("Pruebas de Rendimiento")

        if st.button("Ejecutar Pruebas de Rendimiento"):
            with st.spinner("Ejecutando pruebas..."):
                # Limpiar datos anteriores
                performance_data['database'].clear()
                performance_data['operation'].clear()
                performance_data['time_ms'].clear()

                # Prueba 1: Búsqueda de cliente
                _, exec_time = st.session_state.db_conn.measure_time(
                    "Búsqueda de cliente",
                    st.session_state.db_conn.search_client,
                    name="Juan"
                )
                st.write(f"Búsqueda de cliente: {exec_time:.2f} ms")

                # Prueba 2: Búsqueda de producto
                _, exec_time = st.session_state.db_conn.measure_time(
                    "Búsqueda de producto",
                    st.session_state.db_conn.search_product,
                    name="Laptop"
                )
                st.write(f"Búsqueda de producto: {exec_time:.2f} ms")

                # Prueba 3: Generación de factura
                test_products = [{"producto_id": 1, "cantidad": 1}, {"producto_id": 2, "cantidad": 2}]
                _, exec_time = st.session_state.db_conn.measure_time(
                    "Generación de factura",
                    st.session_state.db_conn.generate_invoice,
                    1, 1, test_products
                )
                if exec_time:
                    st.write(f"Generación de factura: {exec_time:.2f} ms")

                # Prueba 4: Consulta de factura
                _, exec_time = st.session_state.db_conn.measure_time(
                    "Consulta de factura",
                    st.session_state.db_conn.query_invoice,
                    1
                )
                if exec_time:
                    st.write(f"Consulta de factura: {exec_time:.2f} ms")

                # Prueba 5: Reporte de ventas
                _, exec_time = st.session_state.db_conn.measure_time(
                    "Reporte de ventas",
                    st.session_state.db_conn.sales_report
                )
                if exec_time:
                    st.write(f"Reporte de ventas: {exec_time:.2f} ms")

                st.success("Pruebas completadas!")

                # Mostrar resultados
                st.subheader("Resultados de Rendimiento")

                if performance_data['database']:
                    df = pd.DataFrame(performance_data)

                    st.dataframe(df)

                    # Gráfico de resultados
                    fig, ax = plt.subplots(figsize=(10, 6))
                    df.groupby('operation')['time_ms'].mean().plot(kind='bar', ax=ax)
                    ax.set_title("Tiempo de Ejecución por Operación")
                    ax.set_ylabel("Tiempo (ms)")
                    ax.set_xlabel("Operación")
                    plt.xticks(rotation=45)
                    st.pyplot(fig)
                else:
                    st.warning("No se recopilaron datos de rendimiento")

if __name__ == "__main__":
    main()