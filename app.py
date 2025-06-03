import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
#import ibm_db
import uuid
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
        self.cluster = None 
        self.session = None  #para casandra es como el cursor
        
    def connect(self, **kwargs):
        raise NotImplementedError
        
    def disconnect(self):
        if hasattr(self, 'connection') and self.connection: # Para conectores tipo SQL
            self.connection.close()
        if hasattr(self, 'session') and self.session: # Para Cassandra
            self.session.shutdown()
        if hasattr(self, 'cluster') and self.cluster: # Para Cassandra
            self.cluster.shutdown()
            
    def execute_query(self, query, params=None):
        start_time = time.time()

        if self.session: #cassandra
            statement = SimpleStatement(query)
            self.session.execute(statement,params if params else ())
        elif hasattr(self, 'cursor') and self.cursor: #sql
            self.cursor.execute(query, params if params else())

        execution_time = (time.time() - start_time) * 1000  # ms
        return execution_time
    
    def select_query(self, query, params=None): # Nuevo método para SELECTs que devuelven datos
        start_time = time.time()
        if self.session: # Cassandra
            statement = SimpleStatement(query)
            rows = self.session.execute(statement, params if params else ())
        elif hasattr(self, 'cursor') and self.cursor: # SQL
            self.cursor.execute(query, params if params else ())
            rows = self.cursor.fetchall()
        else:
            rows = []
        execution_time = (time.time() - start_time) * 1000  # ms
        return rows, execution_time
    
    def execute_sp(self, sp_name, params):
        start_time = time.time()
        # Implementación específica para cada base de datos
        execution_time = (time.time() - start_time) * 1000  # ms
        return execution_time
    
    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs) #self.select_query => result, time
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
                cliente_id INT not null,
                personal_id INT not null,
                fecha DATETIME,
                total DECIMAL(10,2),
                FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id),
                FOREIGN KEY (personal_id) REFERENCES Personal(personal_id))""",
            """CREATE TABLE IF NOT EXISTS Detalle_Factura (
                detalle_id INT PRIMARY KEY,
                factura_id INT not null,
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                FOREIGN KEY (factura_id) REFERENCES Factura(factura_id),
                FOREIGN KEY (producto_id) REFERENCES Producto(producto_id))"""
        ]
        
        for query in queries:
            self.execute_query(query)
        
        self.create_stored_procedures()
            
    def create_stored_procedures(self):
        drop_sp_query = "DROP PROCEDURE IF EXISTS sp_generar_factura_mysql;"
        try:
            self.execute_query(drop_sp_query)
            print("Stored procedure sp_generar_factura_mysql (si existía) eliminado.")
        except Exception as e:
            st.warning(f"Advertencia al intentar eliminar SP: {e} (puede que no existiera).")

        sp_query_mysql = """
        CREATE PROCEDURE sp_generar_factura_mysql (
            IN p_cliente_id INT,
            IN p_personal_id INT,
            IN p_productos_json JSON,  -- MySQL usa el tipo JSON
            OUT p_factura_id_out INT,
            OUT p_total_out DECIMAL(10,2)
        )
        BEGIN
            -- Declaración de variables locales
            DECLARE v_factura_id INT;
            DECLARE v_total_factura DECIMAL(10,2) DEFAULT 0.00;
            DECLARE v_max_detalle_id INT DEFAULT 0;
            -- Variables para iterar o procesar JSON si fuera necesario (no se usa en este enfoque con JSON_TABLE)
            -- DECLARE i INT DEFAULT 0;
            -- DECLARE num_productos INT DEFAULT 0;
            -- DECLARE current_producto_id INT;
            -- DECLARE current_cantidad INT;
            -- DECLARE current_precio_unitario DECIMAL(10,2);

            -- Manejador de errores para hacer ROLLBACK en caso de excepción
            DECLARE EXIT HANDLER FOR SQLEXCEPTION
            BEGIN
                ROLLBACK;
                -- Puedes también registrar el error o propagarlo si es necesario
                -- RESIGNAL; -- Esto relanzaría la excepción
                SET p_factura_id_out = NULL; -- Indicar fallo
                SET p_total_out = NULL;
            END;

            START TRANSACTION;

            -- 1. Obtener el próximo ID de factura
            SELECT IFNULL(MAX(factura_id), 0) + 1 INTO v_factura_id FROM Factura;

            -- 2. Insertar la cabecera de la factura
            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
            VALUES (v_factura_id, p_cliente_id, p_personal_id, NOW(), 0.00); -- NOW() para la fecha actual

            -- 3. Crear y poblar una tabla temporal con los productos del JSON
            -- Esto requiere MySQL 5.7.8+ para JSON_TABLE
            CREATE TEMPORARY TABLE temp_productos_factura (
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2)
            );

            INSERT INTO temp_productos_factura (producto_id, cantidad, precio_unitario)
            SELECT
                jt.producto_id_json,
                jt.cantidad_json,
                p.precio -- Obtener el precio directamente de la tabla Producto
            FROM
                JSON_TABLE(
                    p_productos_json,
                    '$[*]' -- Asume que p_productos_json es un array de objetos: '[{"producto_id":1, "cantidad":2}, ...]'
                    COLUMNS (
                        producto_id_json INT PATH '$.producto_id',
                        cantidad_json INT PATH '$.cantidad'
                    )
                ) AS jt
            JOIN Producto p ON p.producto_id = jt.producto_id_json;
            -- Aquí se podría añadir validación de stock si es necesario antes de continuar

            -- 4. Obtener el máximo detalle_id actual para la generación secuencial
            SELECT IFNULL(MAX(detalle_id), 0) INTO v_max_detalle_id FROM Detalle_Factura;

            -- 5. Insertar detalles de la factura desde la tabla temporal
            -- Se usa ROW_NUMBER() (MySQL 8.0+) para generar IDs de detalle secuenciales
            INSERT INTO Detalle_Factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal)
            SELECT 
                v_max_detalle_id + ROW_NUMBER() OVER (ORDER BY tp.producto_id), -- Genera IDs secuenciales para los detalles
                v_factura_id,
                tp.producto_id,
                tp.cantidad,
                tp.precio_unitario,
                (tp.cantidad * tp.precio_unitario) -- Calcular subtotal
            FROM temp_productos_factura tp;

            -- 6. Actualizar stock de productos
            UPDATE Producto p
            JOIN temp_productos_factura tp ON p.producto_id = tp.producto_id
            SET p.stock = p.stock - tp.cantidad;
            -- Considerar añadir una verificación aquí para asegurar que el stock no sea negativo.

            -- 7. Calcular el total de la factura sumando los subtotales de los detalles
            SELECT SUM(subtotal) INTO v_total_factura FROM Detalle_Factura WHERE factura_id = v_factura_id;
            
            -- Asegurarse de que el total no sea NULL si no hay detalles (aunque debería haber al menos uno)
            IF v_total_factura IS NULL THEN
                SET v_total_factura = 0.00;
            END IF;

            -- 8. Actualizar el total en la tabla Factura
            UPDATE Factura SET total = v_total_factura WHERE factura_id = v_factura_id;

            -- 9. Eliminar la tabla temporal
            DROP TEMPORARY TABLE IF EXISTS temp_productos_factura;

            COMMIT;

            -- 10. Asignar los valores a los parámetros de salida
            SET p_factura_id_out = v_factura_id;
            SET p_total_out = v_total_factura;
            
            -- Opcional: Si el código que llama al SP espera un result set además de los OUT params,
            -- podrías añadir un SELECT final, pero los OUT params son la forma estándar de devolver valores escalares.
            -- SELECT v_factura_id AS factura_creada_id, v_total_factura AS total_calculado;

        END;
        """
        try:
            self.execute_query(sp_query_mysql)
            # Generalmente, CREATE PROCEDURE es una DDL y se auto-confirma (auto-commit).
            # self.connection.commit() # Podría no ser necesario o incluso causar error dependiendo del driver/configuración.
            st.success("Stored Procedure 'sp_generar_factura_mysql' creado/actualizado en MySQL.")
        except Exception as e:
            st.error(f"Error al crear Stored Procedure en MySQL: {e}")

    def generate_test_data(self):
        tables_to_clear = ["Detalle_Factura", "Factura", "Producto", "Clientes", "Personal"]
        try:
            # Eliminar datos en el orden correcto para evitar problemas de FK
            # (Las tablas que son referenciadas por otras, se limpian después)
            delete_order = ["Detalle_Factura", "Factura", "Producto", "Clientes", "Personal"]
            for table in delete_order:
                try:
                    self.execute_query(f"DELETE FROM {table};")
                except Exception as e:
                    st.warning(f"No se pudieron eliminar datos de {table} (puede que no exista o ya esté vacía): {e}")
            
            # self.execute_query("SET FOREIGN_KEY_CHECKS=1;") # Reactivar FKs
            self.connection.commit() # Commit de las eliminaciones
        except Exception as e:
            st.error(f"Error al limpiar tablas en MySQL: {e}")

        nombres_clientes = ["Carlos", "María", "José", "Ana", "Luis", "Carmen", "Pedro", "Lucía", "Juan", "Diana", "Enrique", "Andres", "Camila","Luciana","Ezequiel","Herodes","Daniel","Pablo","Sebastian","Hilario","Fabricio","Jamil","Hasan","Miguel","Gabriel","Abel"]
        apellidos = ["Pérez", "González", "Rodríguez", "López", "Fernández","Sanchez","Muñoz","Castillo","Takemoto","Chiclayo","Silva","Vega","Vera","Ortega","Lulichac","Villanueva","Oro","Julca","Choquehuanca","Akenmy","Carrasco","Castro","Vasquez","Caffo","Villalobos"]
        roles = ["Vendedor", "Cajero", "Supervisor"]
        productos_ejemplo = [
            ("Laptop HP", 3200.50), ("Impresora Epson", 560.99), ("Mouse Logitech", 75.90),
            ("Teclado Mecánico", 180.00), ("Monitor 24'' Samsung", 899.99), ("Camera Webcam", 120.00),("Altavoces JBL", 200.50)
        ]

        cliente_ids_generated = []
        for i in range(1, 51):
            cliente_id = i
            cliente_ids_generated.append(cliente_id)
            nombre_base = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            email = f"{nombre_base.replace(' ', '.').lower().replace('ñ','n').replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')}{random.randint(1,100)}@example.com"
            telefono = f"9{random.randint(10000000,99999999)}"
            direccion = "Calle Falsa 123, Springfield"
            
            self.execute_query(
                "INSERT INTO Clientes (cliente_id, nombre, email, telefono, direccion) VALUES (%s, %s, %s, %s, %s)",
                (cliente_id, nombre_base, email, telefono, direccion)
            )

        personal_ids_generated = []
        for i in range(1, 51): 
            personal_id = i
            personal_ids_generated.append(personal_id)
            nombre_base = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            rol = random.choice(roles)
            self.execute_query(
                "INSERT INTO Personal (personal_id, nombre, rol) VALUES (%s, %s, %s)",
                (personal_id, nombre_base, rol)
            )

        producto_data_map_generated = {} 
        for i, (nombre_producto, precio) in enumerate(productos_ejemplo):
            producto_id = i + 1 # IDs de 1 hasta N productos
            producto_data_map_generated[nombre_producto] = {'id': producto_id, 'precio': precio}
            stock = random.randint(20, 100)
            self.execute_query(
                "INSERT INTO Producto (producto_id, nombre, precio, stock) VALUES (%s, %s, %s, %s)",
                (producto_id, nombre_producto, precio, stock)
            )
        
        try:
            self.connection.commit() # Commit final de todas las inserciones
            st.success("Datos de prueba generados para MySQL.")
        except Exception as e:
            st.error(f"Error al hacer commit de los datos de prueba en MySQL: {e}")
            try:
                self.connection.rollback() # Intentar rollback si el commit falla
            except Exception as rb_err:
                st.error(f"Error al hacer rollback: {rb_err}")

    def search_client(self):
        try:
            data, _ = self.select_query("SELECT * FROM Clientes WHERE cliente_id = %s LIMIT 1", (1,))
            if data:
                print(f"MySQL search_client: Encontrado cliente con ID 1.")
            else:
                print(f"MySQL search_client: No se encontró cliente con ID 1.")
            return data
        except Exception as e:
            st.error(f"Error en MySQL search_client (placeholder): {e}")
            return None

    def search_product(self):
        try:
            data, _ = self.select_query("SELECT * FROM Producto WHERE producto_id = %s LIMIT 1", (1,))
            if data:
                print(f"MySQL search_product: Encontrado producto con ID 1.")
            else:
                print(f"MySQL search_product: No se encontró producto con ID 1.")
            return data
        except Exception as e:
            st.error(f"Error en MySQL search_product (placeholder): {e}")
            return None

    def generate_invoice(self):
        
        test_cliente_id = 1
        test_personal_id = 1
        
        test_productos_json = '[{"producto_id": 1, "cantidad": 2}, {"producto_id": 2, "cantidad": 1}]'

        args = [
            test_cliente_id,        # p_cliente_id
            test_personal_id,       # p_personal_id
            test_productos_json,    # p_productos_json
            0,                      # Placeholder para p_factura_id_out (INT)
            0.0                     # Placeholder para p_total_out (DECIMAL)
        ]
        
        try:
            self.cursor.callproc('sp_generar_factura_mysql', args)
            generated_factura_id = args[3]  # El cuarto argumento era p_factura_id_out
            generated_total = args[4]       # El quinto argumento era p_total_out
            
            if generated_factura_id is not None and generated_factura_id != 0: # El SP pone NULL o podríamos usar 0 para indicar error en OUT params
                success_msg = (f"MySQL generate_invoice: Factura generada exitosamente. "
                               f"ID Factura: {generated_factura_id}, Total: {generated_total:.2f}")
                print(success_msg)
                return {"factura_id": generated_factura_id, "total": generated_total} # Devolvemos un dict con los resultados
            else:
                # Esto podría ocurrir si el EXIT HANDLER del SP se activó y puso los OUT params a NULL,
                # o si el SP no asignó los OUT params por alguna razón.
                warn_msg = ("MySQL generate_invoice: El SP se ejecutó pero no devolvió un ID de factura válido. "
                            f"ID OUT: {generated_factura_id}, Total OUT: {generated_total}")
                print(warn_msg)
                # st.warning(warn_msg)
                return None # O un dict indicando el problema
            
        except Exception as e:
            st.error(f"Error en MySQL generate_invoice (placeholder): {e}")
            return None

    def query_invoice(self):
        try:
            data, _ = self.select_query("SELECT * FROM Factura WHERE factura_id = %s LIMIT 1", (1,))
            
            if data:
                print(f"MySQL query_invoice: Encontrada factura con ID 1.")
            else:
                print(f"MySQL query_invoice: No se encontró factura con ID 1.")
            return data
        except Exception as e:
            st.error(f"Error en MySQL query_invoice (placeholder): {e}")
            return None

    def sales_report(self):
        try:
            
            data, _ = self.select_query("SELECT SUM(total) as total_ventas FROM Factura")
            if data:
                print(f"MySQL sales_report: Suma de ventas calculada.")
            return data
        except Exception as e:
            st.error(f"Error en MySQL sales_report (placeholder): {e}")
            return None   


class CassandraConnector(DatabaseConnector):
    def connect(self, contact_points=['127.0.0.1'], keyspace=None):
        try:
            self.cluster = Cluster(contact_points)
            self.session = self.cluster.connect(keyspace)

            if keyspace:
                print(f"Conectado a Cassandra, keyspace: {keyspace}")
            else:
                print("Conectado a Cassandra (Sin keyspace por defecto)")
            
            self.session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        except Exception as e:
            st.error(f"Error al conectar Cassandra: {e}")
            raise

    def create_tables(self):
        #UUID para IDs
        #Cassandra no tiene autoincremente para IDS como en SQL
        #Usamos UUID generados por la app

        table_queries = [
            """CREATE TABLE IF NOT EXISTS facturacion.Clientes (
                cliente_id UUID PRIMARY KEY,
                nombre TEXT,
                email TEXT,
                telefono TEXT,
                direccion TEXT
            );""",
            """CREATE TABLE IF NOT EXISTS facturacion.Personal (
                personal_id UUID PRIMARY KEY,
                nombre TEXT,
                rol TEXT
            );""",
            """CREATE TABLE IF NOT EXISTS facturacion.Producto (
                producto_id UUID PRIMARY KEY,
                nombre TEXT,
                precio DECIMAL,
                stock INT
            );""",
            """CREATE TABLE IF NOT EXISTS facturacion.Factura (
                factura_id UUID PRIMARY KEY,
                cliente_id UUID,     // Almacenamos el ID, la 'relación' se maneja en la app
                personal_id UUID,    // Almacenamos el ID
                fecha TIMESTAMP,
                total DECIMAL
                // Los detalles de la factura podrían ser una colección aquí (UDT)
                // o una tabla separada consultada por factura_id.
            );""",
            """CREATE TABLE IF NOT EXISTS facturacion.Detalle_Factura (
                factura_id UUID,         // Parte de la clave primaria para agrupar por factura
                detalle_id UUID,         // ID único para el detalle
                producto_id UUID,        // Almacenamos el ID
                nombre_producto TEXT,    // Denormalizado para evitar otra búsqueda solo para el nombre
                cantidad INT,
                precio_unitario DECIMAL,
                subtotal DECIMAL,
                PRIMARY KEY (factura_id, detalle_id) // Clave de partición por factura_id
            );"""
        ]

        for query in table_queries:
            try:
                self.execute_query(query)
            except Exception as e:
                st.error(f"Error creando tabla en Cassandra: {query[:30]}... -> {e}")
        st.success("Tablas de Cassandra verificadas/creadas en keyspace 'facturacion'.")

    def generate_test_data(self):
        import uuid
        # Para Cassandra, TRUNCATE es más eficiente que DELETE FROM para tablas enteras.
        # Considera que TRUNCATE es DDL y puede no ser transaccional como en SQL.
        tables_to_truncate = ["Detalle_Factura", "Factura", "Producto", "Clientes", "Personal"]
        for table in tables_to_truncate:
            try:
                self.execute_query(f"TRUNCATE facturacion.{table};")
                print(f"Truncada tabla facturacion.{table}")
            except Exception as e:
                print(f"Error truncando {table}: {e} (puede que no exista aún o ya esté vacía)")
        
        nombres_clientes = ["Carlos", "María", "José", "Ana", "Luis", "Carmen", "Pedro", "Lucía", "Juan", "Diana", "Enrique", "Andres", "Camila","Luciana","Ezequiel","Herodes","Daniel","Pablo","Sebastian","Hilario","Fabricio","Jamil","Hasan","Miguel","Gabriel","Abel"]
        apellidos = ["Pérez", "González", "Rodríguez", "López", "Fernández","Sanchez","Muñoz","Castillo","Takemoto","Chiclayo","Silva","Vega","Vera","Ortega","Lulichac","Villanueva","Oro","Julca","Choquehuanca","Akenmy","Carrasco","Castro","Vasquez","Caffo","Villalobos"]
        roles = ["Vendedor", "Cajero", "Supervisor"]
        productos_ejemplo = [
            ("Laptop HP", 3200.50), ("Impresora Epson", 560.99), ("Mouse Logitech", 75.90),
            ("Teclado Mecánico", 180.00), ("Monitor 24'' Samsung", 899.99), ("Camera Webcam", 120.00),("Altavoces JBL", 200.50)
        ]

        # Insertar Clientes
        cliente_ids = []
        for i in range(100):
            cliente_id = uuid.uuid4()
            cliente_ids.append(cliente_id)
            nombre = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            email = f"{nombre.replace(' ', '.').lower()}@gmail.com"
            
            self.execute_query(
                "INSERT INTO facturacion.Clientes (cliente_id, nombre, email, telefono, direccion) VALUES (%s, %s, %s, %s, %s)",
                (cliente_id, nombre, email, f"9{random.randint(10000000,99999999)}", "Francisco de zela 462")
            )

        # Insertar Personal
        personal_ids = []
        for i in range(20):
            personal_id = uuid.uuid4()
            personal_ids.append(personal_id)
            nombre = f"{random.choice(nombres_clientes)} {random.choice(apellidos)}"
            self.execute_query(
                "INSERT INTO facturacion.Personal (personal_id, nombre, rol) VALUES (%s, %s, %s)",
                (personal_id, nombre, random.choice(roles))
            )

        # Insertar Productos
        producto_data_map = {} # Para fácil acceso a precio e ID
        for i, (nombre_producto, precio) in enumerate(productos_ejemplo):
            producto_id = uuid.uuid4()
            producto_data_map[nombre_producto] = {'id': producto_id, 'precio': precio}
            stock = random.randint(20, 100)
            self.execute_query(
                "INSERT INTO facturacion.Producto (producto_id, nombre, precio, stock) VALUES (%s, %s, %s, %s)",
                (producto_id, nombre_producto, precio, stock)
            )
        
        st.success("Datos generados para Cassandra.")
    
    def search_client_cassandra(self, nombre_buscar=None):
        query = "SELECT cliente_id, nombre, email, telefono, direccion FROM facturacion.Clientes"
        # Cassandra no tiene LIKE '%...%' de forma eficiente sin integraciones (ej. Solr).
        # Para búsquedas flexibles, se usan otras estrategias o se trae todo y se filtra en la app (no ideal para grandes datasets).
        # O se modela para soportar este tipo de queries (ej. usando índices secundarios o tablas desnormalizadas).
        if nombre_buscar:
            # Esto sería una búsqueda ineficiente si no hay índice secundario en nombre y la tabla es grande.
            query += f" WHERE nombre = '{nombre_buscar}' ALLOW FILTERING"
        
        rows, exec_time = self.measure_time("Búsqueda Cliente (Cassandra)", self.select_query, query)
        data,timeInter = rows 
        return pd.DataFrame(list(data)), exec_time

    def generate_invoice_cassandra(self, cliente_id, personal_id, productos_pedido):
        # Esta función simularía la lógica del Stored Procedure en la aplicación
        # productos_pedido: lista de tuplas (producto_id, cantidad)
        
        factura_id = uuid.uuid4()
        fecha_actual = datetime.utcnow() # Usar UTC para timestamps
        total_factura = 0
        
        # 1. Calcular subtotal y total, preparar detalles
        detalles_para_insertar = []
        for prod_id_pedido, cantidad_pedida in productos_pedido:
            # Obtener info del producto (nombre, precio)
            # En una app real, podrías tener esta info ya cargada o hacer una query eficiente
            producto_info_rows, _ = self.select_query("SELECT nombre, precio, stock FROM facturacion.Producto WHERE producto_id = %s", (prod_id_pedido,))
            if not producto_info_rows:
                st.error(f"Producto con ID {prod_id_pedido} no encontrado.")
                return None, 0 # O manejar el error de otra forma
            
            producto_info = producto_info_rows[0] # Asumimos que producto_id es único
            nombre_prod = producto_info.nombre
            precio_unit = producto_info.precio
            stock_actual = producto_info.stock

            if cantidad_pedida > stock_actual:
                st.error(f"No hay suficiente stock para {nombre_prod}. Solicitado: {cantidad_pedida}, Disponible: {stock_actual}")
                return None, 0

            subtotal_item = precio_unit * cantidad_pedida
            total_factura += subtotal_item
            detalles_para_insertar.append({
                'detalle_id': uuid.uuid4(),
                'producto_id': prod_id_pedido,
                'nombre_producto': nombre_prod,
                'cantidad': cantidad_pedida,
                'precio_unitario': precio_unit,
                'subtotal': subtotal_item
            })

            # Actualizar stock (esto debe ser más robusto en producción, ej. con LWT o ajustes)
            nuevo_stock = stock_actual - cantidad_pedida
            self.execute_query("UPDATE facturacion.Producto SET stock = %s WHERE producto_id = %s", (nuevo_stock, prod_id_pedido))

        # 2. Insertar la Factura
        self.execute_query(
            "INSERT INTO facturacion.Factura (factura_id, cliente_id, personal_id, fecha, total) VALUES (%s, %s, %s, %s, %s)",
            (factura_id, cliente_id, personal_id, fecha_actual, total_factura)
        )

        # 3. Insertar los Detalles de la Factura (como lote para eficiencia)
        # Aunque Cassandra no tiene transacciones ACID multi-fila como SQL, un BATCH puede agrupar operaciones.
        # Para inserciones en la misma partición, es útil. Aquí, cada detalle va a la misma factura_id (partición).
        from cassandra.query import BatchStatement, SimpleStatement
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for detalle in detalles_para_insertar:
            batch.add(SimpleStatement(
                """INSERT INTO facturacion.Detalle_Factura 
                   (factura_id, detalle_id, producto_id, nombre_producto, cantidad, precio_unitario, subtotal) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                ),(factura_id, detalle['detalle_id'], detalle['producto_id'], detalle['nombre_producto'],
                   detalle['cantidad'], detalle['precio_unitario'], detalle['subtotal'])
            )
        self.session.execute(batch)
        
        return {"factura_id": factura_id, "total": total_factura} 

    def search_client(self):
        
        try:
            # Ejemplo: obtener hasta 5 clientes para la prueba.
            # Para una prueba más específica, podrías obtener un UUID de cliente de
            # los datos generados y buscar por esa clave primaria.
            query = "SELECT cliente_id, nombre, email, telefono, direccion FROM facturacion.Clientes LIMIT 5"
            data_rows, internal_time = self.select_query(query)
            
            if data_rows:
                print(f"Cassandra search_client: Se recuperaron {len(list(data_rows))} clientes (límite 5).")
            else:
                print("Cassandra search_client: No se encontraron clientes.")
            # Devuelve lo que select_query devuelve, para ser consistente.
            return data_rows, internal_time 
        except Exception as e:
            print(f"Error en CassandraConnector.search_client: {str(e)}")
            return None, 0 # Devolver tupla para consistencia con el retorno esperado de select_query


    def search_product(self):
        try:
            query = "SELECT producto_id, nombre, precio, stock FROM facturacion.Producto LIMIT 5"
            data_rows, internal_time = self.select_query(query)

            if data_rows:
                print(f"Cassandra search_product: Se recuperaron {len(list(data_rows))} productos (límite 5).")
            else:
                print("Cassandra search_product: No se encontraron productos.")
            return data_rows, internal_time
        except Exception as e:
            print(f"Error en CassandraConnector.search_product: {str(e)}")
            return None, 0

    def generate_invoice(self):
        print("Executing CassandraConnector.generate_invoice (placeholder)...")
        try:
            # Obtener IDs existentes para la factura de prueba
            clientes_rs, _ = self.select_query("SELECT cliente_id FROM facturacion.Clientes LIMIT 1")
            personal_rs, _ = self.select_query("SELECT personal_id FROM facturacion.Personal LIMIT 1")
            productos_rs, _ = self.select_query("SELECT producto_id FROM facturacion.Producto LIMIT 2")

            if not (clientes_rs and clientes_rs[0] and \
                    personal_rs and personal_rs[0] and \
                    productos_rs and len(list(productos_rs)) >= 1): # Asegurar que hay al menos un producto
                print("Cassandra generate_invoice: Datos insuficientes (cliente/personal/producto) para generar factura de prueba.")
                return None # O un dict indicando error

            test_cliente_id = clientes_rs[0].cliente_id
            test_personal_id = personal_rs[0].personal_id
            
            productos_list = list(productos_rs) # Convertir ResultSet a lista para acceder por índice
            productos_pedido_test = []
            if len(productos_list) > 0:
                 productos_pedido_test.append((productos_list[0].producto_id, 1)) # (producto_id, cantidad)
            if len(productos_list) > 1: # Si hay al menos dos productos, añadir el segundo
                 productos_pedido_test.append((productos_list[1].producto_id, 2))
            
            if not productos_pedido_test:
                 print("Cassandra generate_invoice: No se pudieron obtener productos para los detalles de la factura.")
                 return None

            invoice_data = self.generate_invoice_cassandra(test_cliente_id, test_personal_id, productos_pedido_test)
            
            if invoice_data and isinstance(invoice_data, dict) and "factura_id" in invoice_data:
                 print(f"Cassandra generate_invoice: Factura de prueba generada con ID {invoice_data['factura_id']}.")
            elif invoice_data and invoice_data[0] is None: # Caso de error de generate_invoice_cassandra
                 print("Cassandra generate_invoice: Falló la generación de factura (controlado por generate_invoice_cassandra).")
            else:
                 print("Cassandra generate_invoice: La generación de factura no devolvió el resultado esperado.")
            return invoice_data
            
        except Exception as e:
            print(f"Error crítico en CassandraConnector.generate_invoice: {str(e)}")
            return None

    def query_invoice(self):
        print("Executing CassandraConnector.query_invoice (placeholder)...")
        try:
            facturas_rs, _ = self.select_query("SELECT factura_id FROM facturacion.Factura LIMIT 1")
            
            if not (facturas_rs and facturas_rs[0]):
                print("Cassandra query_invoice: No hay facturas para consultar.")
                return None, 0

            test_factura_id = facturas_rs[0].factura_id
            
            # Consultar la factura principal
            query_factura = "SELECT * FROM facturacion.Factura WHERE factura_id = %s"
            factura_data, t1 = self.select_query(query_factura, (test_factura_id,))
            
            # Consultar los detalles de esa factura
            query_detalles = "SELECT * FROM facturacion.Detalle_Factura WHERE factura_id = %s"
            detalles_data, t2 = self.select_query(query_detalles, (test_factura_id,))
            
            num_factura_rows = len(list(factura_data)) if factura_data else 0
            # Es importante volver a convertir a lista si se va a medir len de nuevo, ya que el ResultSet se consume
            detalles_list = list(detalles_data) if detalles_data else []
            num_detalles_rows = len(detalles_list)

            print(f"Cassandra query_invoice: Consultada factura {test_factura_id}. "
                  f"Cabecera: {num_factura_rows} fila(s), Detalles: {num_detalles_rows} fila(s).")
            
            # Devolvemos un diccionario con los resultados, y el tiempo total sumado (aproximado)
            return {"factura": factura_data, "detalles": detalles_list}, (t1 + t2) # t1 y t2 son tiempos internos
        except Exception as e:
            print(f"Error en CassandraConnector.query_invoice: {str(e)}")
            return None, 0

    def sales_report(self):
        print("Executing CassandraConnector.sales_report (placeholder - INEFICIENTE)...")
        try:
            query = "SELECT total FROM facturacion.Factura" # Sin WHERE, trae todos los totales
            all_invoices_totals_rs, internal_time = self.select_query(query)
            
            total_sales = 0
            invoice_count = 0
            if all_invoices_totals_rs:
                for row in all_invoices_totals_rs: # Iterar sobre el ResultSet
                    if row.total is not None:
                        total_sales += row.total
                    invoice_count += 1
            
            print(f"Cassandra sales_report: Ventas totales calculadas: {total_sales} de {invoice_count} facturas.")
            # Devolver el resultado y el tiempo interno de la consulta SELECT
            return {"total_sales": total_sales, "invoices_counted": invoice_count}, internal_time
        except Exception as e:
            print(f"Error en CassandraConnector.sales_report: {str(e)}")
            return None, 0 
        


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
        #"SQLServer": None,
        #"Oracle": None,  # Se implementaría similar a SQLServer
        #"DB2": DB2Connector("DB2"),
        #"PostgreSQL": None,
        "MySQL": MySQLConnector("MySQL"),
        "Cassandra": CassandraConnector("Cassandra"),
        #"MongoDB": None
    }
    
    if choice == "Mantenedores":
        st.header("Mantenedores de Tablas")
        
        db_choice_mant = st.sidebar.radio("Selecionar BD para mantenedores",list(databases.keys()))
        selected_db_connector = databases.get(db_choice_mant)

        if not selected_db_connector:
            st.warning(f"Conector para {db_choice_mant} no implementado/seleccionado")
            return
        
        try:
            creds = get_db_credentials(db_choice_mant)
            if not creds:
                st.error(f"Credenciales para {db_choice_mant} no definidas.")
                return
            selected_db_connector.connect(**creds)

            if st.sidebar.button(f"Verificar/Crear Tablas en {db_choice_mant}"):
                with st.spinner(f"Creando tablas en {db_choice_mant}..."):
                    selected_db_connector.create_tables()
        except Exception as e:
            st.error(f"Error de conexión o configuración en Mantenedores para {db_choice_mant}: {e}")
            if selected_db_connector: selected_db_connector.disconnect()
            return # No continuar si la conexión falla


        tab_options = ["Clientes", "Personal", "Producto", "Factura", "Detalle Factura"]
        tab_choice = st.selectbox("Seleccione tabla", tab_options)
        
        
        if db_choice_mant == "Cassandra" and tab_choice == "Clientes":
            st.subheader("Mantenedor de Clientes (Cassandra)")
            col1, col2 = st.columns(2)
            with col1:
                st.write("📋 Listado de Clientes")
                df_clientes, _ = selected_db_connector.search_client_cassandra()
                st.write(df_clientes)
                #st.dataframe(df_clientes)
            with col2:
                st.write("➕ Agregar Cliente (Cassandra)")
                with st.form("add_cliente_cassandra"):
                    # Usar TEXT para nombre, email etc. cliente_id será UUID
                    nombre_c = st.text_input("Nombre")
                    email_c = st.text_input("Email")
                    telefono_c = st.text_input("Teléfono")
                    direccion_c = st.text_input("Dirección")
                    submitted_c = st.form_submit_button("Guardar Cliente")
                    if submitted_c:
                        cliente_id_c = uuid.uuid4()
                        try:
                            selected_db_connector.execute_query(
                                "INSERT INTO facturacion.Clientes (cliente_id, nombre, email, telefono, direccion) VALUES (%s, %s, %s, %s, %s)",
                                (cliente_id_c, nombre_c, email_c, telefono_c, direccion_c)
                            )
                            st.success(f"Cliente {nombre_c} agregado a Cassandra con ID: {cliente_id_c}")
                        except Exception as e:
                            st.error(f"Error agregando cliente a Cassandra: {e}") 
            #dewd
            st.subheader("🗑️ Eliminar Cliente de Cassandra")
            uuid_cliente_a_eliminar_str = st.text_input(
                "Ingrese el UUID del Cliente a eliminar", 
                key="del_cassandra_cliente_uuid_input",
                placeholder="Ej: 123e4567-e89b-12d3-a456-426614174000"
            )

            if st.button("Eliminar Cliente de Cassandra", key="del_cassandra_cliente_button"):
                if not uuid_cliente_a_eliminar_str:
                    st.warning("Por favor, ingrese el UUID del cliente que desea eliminar.")
                else:
                    try:
                        # 1. Convertir el string del input a un objeto UUID
                        import uuid 
                        cliente_uuid_obj = uuid.UUID(uuid_cliente_a_eliminar_str)

                        # 3. Preparar y ejecutar la consulta DELETE CQL
                        cql_query = "DELETE FROM facturacion.Clientes WHERE cliente_id = %s" 
                        
                        # Usamos el método execute_query de tu DatabaseConnector,
                        # que ya maneja la sesión de Cassandra.
                        # Pasamos el UUID como parámetro en una tupla.
                        tiempo_ejecucion = selected_db_connector.execute_query(cql_query, (cliente_uuid_obj,))
                        
                        # 4. Mostrar mensaje de éxito. No se necesita commit en Cassandra para esto.
                        st.success(f"🗑️ Cliente con UUID '{cliente_uuid_obj}' eliminado correctamente de Cassandra.")
                        st.caption(f"Tiempo de ejecución: {tiempo_ejecucion:.2f} ms")
                        st.rerun()
                    except ValueError:
                        st.error("❌ El UUID ingresado no es válido. Asegúrese de que tenga el formato correcto (ej: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).")
                    except Exception as e:
                        st.error(f"❌ Error al intentar eliminar el cliente de Cassandra: {str(e)}")

        elif db_choice_mant == "Cassandra" and tab_choice == "Personal":
            st.subheader("Mantenedor de Personal (Cassandra)")

            def mostrar_lista_personal_cassandra(conector):
                st.write("📋 Listado de Personal")
                try:
                    query_cql = "SELECT personal_id, nombre, rol FROM facturacion.Personal"
                    result_set, tiempo_carga = conector.select_query(query_cql)
                    if result_set:
                        df_personal = pd.DataFrame(list(result_set))
                        if not df_personal.empty:
                            st.dataframe(df_personal)
                        else:
                            st.info("No hay personal para mostrar.")
                    else:
                            st.info("No hay personal para mostrar o la tabla está vacía.")
                    st.caption(f"Lista cargada/refrescada. Tiempo de consulta: {tiempo_carga:.2f} ms")
                except Exception as e:
                    st.error(f"Error al cargar lista de personal: {e}")
            
            col1_per, col2_per = st.columns(2)
            with col1_per:
                mostrar_lista_personal_cassandra(selected_db_connector)
                if st.button("Refrescar Lista Personal", key="refresh_c_per"):
                    st.rerun()

            with col2_per:
                st.write("➕ Agregar Personal")
                with st.form("add_personal_c_form", clear_on_submit=True):
                    nombre_p_add = st.text_input("Nombre del Empleado", key="add_p_nombre")
                    rol_p_add = st.text_input("Rol del Empleado", key="add_p_rol") # Podría ser un st.selectbox si los roles son fijos
                    submitted_add_p = st.form_submit_button("Guardar Personal")
                    if submitted_add_p:
                        if nombre_p_add and rol_p_add:
                            import uuid
                            personal_id_p = uuid.uuid4()
                            try:
                                selected_db_connector.execute_query(
                                    "INSERT INTO facturacion.Personal (personal_id, nombre, rol) VALUES (%s, %s, %s)",
                                    (personal_id_p, nombre_p_add, rol_p_add)
                                )
                                st.success(f"Personal '{nombre_p_add}' agregado con UUID: {personal_id_p}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error agregando personal: {e}")
                        else:
                            st.warning("Nombre y Rol son obligatorios.")
                
                st.markdown("---")
                st.write("✏️ Editar Personal")
                with st.form("edit_personal_c_form", clear_on_submit=True):
                    import uuid
                    personal_id_edit_str = st.text_input("UUID del Personal a editar")
                    nombre_p_edit = st.text_input("Nuevo Nombre")
                    rol_p_edit = st.text_input("Nuevo Rol")
                    submitted_edit_p = st.form_submit_button("Actualizar Personal")
                    if submitted_edit_p:
                        if personal_id_edit_str and (nombre_p_edit or rol_p_edit):
                            try:
                                personal_id_edit_obj = uuid.UUID(personal_id_edit_str)
                                selected_db_connector.execute_query(
                                    "UPDATE facturacion.Personal SET nombre = %s, rol = %s WHERE personal_id = %s",
                                    (nombre_p_edit, rol_p_edit, personal_id_edit_obj)
                                )
                                st.success(f"Personal {personal_id_edit_obj} actualizado.")
                                st.rerun()
                            except ValueError:
                                st.error("UUID de personal para editar no válido.")
                            except Exception as e:
                                st.error(f"Error actualizando personal: {e}")
                        else:
                            st.warning("Se requiere UUID del personal y al menos un campo nuevo.")

                st.markdown("---")
                st.write("🗑️ Eliminar Personal")
                uuid_personal_del_str = st.text_input("UUID del Personal a eliminar", key="del_c_pers_uuid")
                if st.button("Eliminar Personal", key="del_c_pers_btn"):
                    if uuid_personal_del_str:
                        try:
                            personal_uuid_obj = uuid.UUID(uuid_personal_del_str)
                            selected_db_connector.execute_query(
                                "DELETE FROM facturacion.Personal WHERE personal_id = %s", (personal_uuid_obj,)
                            )
                            st.success(f"Personal {personal_uuid_obj} eliminado.")
                            st.rerun()
                        except ValueError:
                            st.error("UUID para eliminar no es válido.")
                        except Exception as e:
                            st.error(f"Error eliminando personal: {e}")
                    else:
                        st.warning("Ingrese un UUID para eliminar.")

        elif db_choice_mant == "Cassandra" and tab_choice == "Producto":
            st.subheader("Mantenedor de Productos (Cassandra)")

            def mostrar_lista_productos_cassandra(conector):
                st.write("📦 Listado de Productos")
                try:
                    query_cql = "SELECT producto_id, nombre, precio, stock FROM facturacion.Producto"
                    result_set, tiempo_carga = conector.select_query(query_cql)
                    if result_set:
                        df_productos = pd.DataFrame(list(result_set))
                        if not df_productos.empty:
                            st.dataframe(df_productos)
                        else:
                            st.info("No hay productos para mostrar.")
                    else:
                        st.info("No hay productos para mostrar o la tabla está vacía.")
                    st.caption(f"Lista cargada/refrescada. Tiempo de consulta: {tiempo_carga:.2f} ms")
                except Exception as e:
                    st.error(f"Error al cargar lista de productos: {e}")

            col1_prod, col2_prod = st.columns(2)
            with col1_prod:
                mostrar_lista_productos_cassandra(selected_db_connector)
                if st.button("Refrescar Lista Productos", key="refresh_c_prod"):
                    st.rerun()
            
            with col2_prod:
                st.write("➕ Agregar Producto")
                with st.form("add_producto_c_form", clear_on_submit=True):
                    nombre_prod_add = st.text_input("Nombre del Producto", key="add_prod_nombre")
                    precio_prod_add = st.number_input("Precio", min_value=0.0, format="%.2f", key="add_prod_precio")
                    stock_prod_add = st.number_input("Stock Inicial", min_value=0, step=1, key="add_prod_stock")
                    submitted_add_prod = st.form_submit_button("Guardar Producto")
                    if submitted_add_prod:
                        if nombre_prod_add and precio_prod_add >= 0:
                            import uuid
                            producto_id_add = uuid.uuid4()
                            try:
                                selected_db_connector.execute_query(
                                    "INSERT INTO facturacion.Producto (producto_id, nombre, precio, stock) VALUES (%s, %s, %s, %s)",
                                    (producto_id_add, nombre_prod_add, precio_prod_add, stock_prod_add)
                                )
                                st.success(f"Producto '{nombre_prod_add}' agregado con UUID: {producto_id_add}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error agregando producto: {e}")
                        else:
                            st.warning("Nombre y Precio válido son obligatorios.")
                
                st.markdown("---")
                st.write("✏️ Editar Producto")
                with st.form("edit_producto_c_form", clear_on_submit=True):
                    import uuid
                    producto_id_edit_str = st.text_input("UUID del Producto a editar")
                    nombre_prod_edit = st.text_input("Nuevo Nombre del Producto")
                    precio_prod_edit = st.number_input("Nuevo Precio", min_value=0.0, format="%.2f")
                    stock_prod_edit = st.number_input("Nuevo Stock", min_value=0, step=1)
                    submitted_edit_prod = st.form_submit_button("Actualizar Producto")
                    if submitted_edit_prod:
                        if producto_id_edit_str:
                            try:
                                producto_id_edit_obj = uuid.UUID(producto_id_edit_str)
                                selected_db_connector.execute_query(
                                    """UPDATE facturacion.Producto 
                                        SET nombre = %s, precio = %s, stock = %s 
                                        WHERE producto_id = %s""",
                                    (nombre_prod_edit, precio_prod_edit, stock_prod_edit, producto_id_edit_obj)
                                )
                                st.success(f"Producto {producto_id_edit_obj} actualizado.")
                                st.rerun()
                            except ValueError:
                                st.error("UUID de producto para editar no válido.")
                            except Exception as e:
                                st.error(f"Error actualizando producto: {e}")
                        else:
                            st.warning("Se requiere UUID del producto.")


                st.markdown("---")
                st.write("🗑️ Eliminar Producto")
                uuid_prod_del_str = st.text_input("UUID del Producto a eliminar", key="del_c_prod_uuid")
                if st.button("Eliminar Producto", key="del_c_prod_btn"):
                    if uuid_prod_del_str:
                        import uuid
                        try:
                            producto_uuid_obj = uuid.UUID(uuid_prod_del_str)
                            # Consideración: Si el producto está en Detalle_Factura, eliminarlo aquí
                            # no actualiza esos registros (problema de integridad referencial que Cassandra no maneja).
                            selected_db_connector.execute_query(
                                "DELETE FROM facturacion.Producto WHERE producto_id = %s", (producto_uuid_obj,)
                            )
                            st.success(f"Producto {producto_uuid_obj} eliminado.")
                            st.rerun()
                        except ValueError:
                            st.error("UUID para eliminar no es válido.")
                        except Exception as e:
                            st.error(f"Error eliminando producto: {e}")
                    else:
                        st.warning("Ingrese un UUID para eliminar.")
        
        elif db_choice_mant == "Cassandra" and tab_choice == "Factura":
            st.subheader("Mantenedor de Facturas (Cassandra)")
            
            def mostrar_lista_facturas_cassandra(conector):
                st.write("🧾 Listado de Facturas")
                try:
                    query_cql = "SELECT factura_id, cliente_id, personal_id, fecha, total FROM facturacion.Factura"
                    result_set, tiempo_carga = conector.select_query(query_cql)
                    if result_set:
                        df_facturas = pd.DataFrame(list(result_set))
                        if not df_facturas.empty:
                            st.dataframe(df_facturas)
                        else:
                            st.info("No hay facturas para mostrar.")
                    else:
                        st.info("No hay facturas para mostrar o la tabla está vacía.")
                    st.caption(f"Lista cargada/refrescada. Tiempo de consulta: {tiempo_carga:.2f} ms")
                except Exception as e:
                    st.error(f"Error al cargar lista de facturas: {e}")

            # Helper para obtener listas de Clientes y Personal para selectbox
            def get_clientes_for_select(conector):
                try:
                    rows, _ = conector.select_query("SELECT cliente_id, nombre FROM facturacion.Clientes")
                    return {f"{row.nombre} ({row.cliente_id})": row.cliente_id for row in rows}
                except: return {}
            
            def get_personal_for_select(conector):
                try:
                    rows, _ = conector.select_query("SELECT personal_id, nombre FROM facturacion.Personal")
                    return {f"{row.nombre} ({row.personal_id})": row.personal_id for row in rows}
                except: return {}

            col1_fact, col2_fact = st.columns(2)
            with col1_fact:
                mostrar_lista_facturas_cassandra(selected_db_connector)
                if st.button("Refrescar Lista Facturas", key="refresh_c_fact"):
                    st.rerun()
            
            with col2_fact:
                st.write("➕ Registrar Nueva Factura")
                clientes_dict = get_clientes_for_select(selected_db_connector)
                personal_dict = get_personal_for_select(selected_db_connector)

                with st.form("add_factura_c_form", clear_on_submit=True):
                    cliente_display = st.selectbox("Cliente", options=list(clientes_dict.keys()), key="add_f_cliente")
                    personal_display = st.selectbox("Personal", options=list(personal_dict.keys()), key="add_f_personal")
                    fecha_f_add = st.date_input("Fecha de Factura", value=datetime.now(), key="add_f_fecha")
                    # El total se calcularía usualmente a partir de los detalles. Para un mantenedor simple, se puede ingresar manualmente.
                    # O dejarlo en 0 y que se actualice al agregar detalles. Por simplicidad, manual aquí.
                    total_f_add = st.number_input("Total Factura", min_value=0.0, format="%.2f", key="add_f_total", value=0.0)
                    
                    submitted_add_f = st.form_submit_button("Guardar Factura")
                    if submitted_add_f:
                        if cliente_display and personal_display:
                            import uuid
                            cliente_id_f = clientes_dict[cliente_display]
                            personal_id_f = personal_dict[personal_display]
                            factura_id_f = uuid.uuid4()
                            # Convertir st.date_input a datetime para Cassandra TIMESTAMP
                            fecha_f_dt = datetime(fecha_f_add.year, fecha_f_add.month, fecha_f_add.day)
                            try:
                                selected_db_connector.execute_query(
                                    "INSERT INTO facturacion.Factura (factura_id, cliente_id, personal_id, fecha, total) VALUES (%s, %s, %s, %s, %s)",
                                    (factura_id_f, cliente_id_f, personal_id_f, fecha_f_dt, total_f_add)
                                )
                                st.success(f"Factura registrada con UUID: {factura_id_f}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error registrando factura: {e}")
                        else:
                            st.warning("Cliente y Personal son obligatorios.")
                
                st.markdown("---")
                st.write("✏️ Editar Factura (Simplificado - se actualiza fecha y total)")
                # Editar cliente_id o personal_id es más complejo por la selección.
                with st.form("edit_factura_c_form", clear_on_submit=True):
                    import uuid
                    factura_id_edit_str = st.text_input("UUID de la Factura a editar")
                    nueva_fecha_f = st.date_input("Nueva Fecha", value=datetime.now())
                    nuevo_total_f = st.number_input("Nuevo Total", min_value=0.0, format="%.2f")
                    submitted_edit_f = st.form_submit_button("Actualizar Factura")

                    if submitted_edit_f:
                        if factura_id_edit_str:
                            try:
                                factura_id_edit_obj = uuid.UUID(factura_id_edit_str)
                                nueva_fecha_dt = datetime(nueva_fecha_f.year, nueva_fecha_f.month, nueva_fecha_f.day)
                                selected_db_connector.execute_query(
                                    "UPDATE facturacion.Factura SET fecha = %s, total = %s WHERE factura_id = %s",
                                    (nueva_fecha_dt, nuevo_total_f, factura_id_edit_obj)
                                )
                                st.success(f"Factura {factura_id_edit_obj} actualizada.")
                                st.rerun()
                            except ValueError:
                                st.error("UUID de factura para editar no válido.")
                            except Exception as e:
                                st.error(f"Error actualizando factura: {e}")
                        else:
                            st.warning("Se requiere UUID de la factura.")


                st.markdown("---")
                st.write("🗑️ Eliminar Factura")
                uuid_fact_del_str = st.text_input("UUID de la Factura a eliminar", key="del_c_fact_uuid")
                if st.button("Eliminar Factura", key="del_c_fact_btn"):
                    if uuid_fact_del_str:
                        import uuid
                        try:
                            factura_uuid_obj = uuid.UUID(uuid_fact_del_str)
                            # ¡ADVERTENCIA! Esto no elimina los Detalle_Factura asociados.
                            # Se necesitaría lógica adicional para eliminarlos primero (por factura_id).
                            selected_db_connector.execute_query(
                                "DELETE FROM facturacion.Factura WHERE factura_id = %s", (factura_uuid_obj,)
                            )
                            st.success(f"Factura {factura_uuid_obj} eliminada. (Detalles no se eliminan automáticamente).")
                            st.rerun()
                        except ValueError:
                            st.error("UUID para eliminar no es válido.")
                        except Exception as e:
                            st.error(f"Error eliminando factura: {e}")
                    else:
                        st.warning("Ingrese un UUID para eliminar.")

        elif db_choice_mant == "Cassandra" and tab_choice == "Detalle Factura":
            st.subheader("Mantenedor de Detalles de Factura (Cassandra)")
            # Clave Primaria: (factura_id, detalle_id)
            # Listar: Idealmente filtrado por factura_id.
            # Agregar: Requiere factura_id, producto_id. Genera detalle_id. Guarda nombre_producto, precio_unitario (denormalizado). Calcula subtotal.
            # Editar: Por (factura_id, detalle_id). Podría permitir cambiar cantidad y recalcular subtotal.
            # Eliminar: Por (factura_id, detalle_id).
            # Consideración: Agregar/Editar/Eliminar detalles debería idealmente recalcular y actualizar el 'total' en la tabla Factura.
            # Esto es complejo para un mantenedor simple y a menudo se maneja a nivel de aplicación/servicio.

            def get_facturas_for_select(conector): # Para seleccionar a qué factura agregar/ver detalles
                try:
                    rows, _ = conector.select_query("SELECT factura_id, fecha, total FROM facturacion.Factura")
                    # Mostrar fecha y total para ayudar a identificar la factura
                    return {f"ID: {row.factura_id} (Fecha: {row.fecha.strftime('%Y-%m-%d') if row.fecha else 'N/A'}, Total: {row.total if row.total is not None else 'N/A'})": row.factura_id for row in rows}
                except: return {}

            def get_productos_for_select_details(conector): # Para seleccionar producto al agregar detalle
                try:
                    rows, _ = conector.select_query("SELECT producto_id, nombre, precio FROM facturacion.Producto")
                    return {f"{row.nombre} (ID: {row.producto_id}, Precio: {row.precio})": (row.producto_id, row.nombre, row.precio) for row in rows}
                except: return {}

            st.info("""
            **Nota sobre Detalles de Factura:**
            - Agregar/Editar/Eliminar detalles aquí **NO** actualizará automáticamente el `total` de la factura principal en la tabla `Factura`.
            - Tampoco ajustará el `stock` de los productos. Estas operaciones suelen ser parte de la lógica de negocio más compleja (como en 'Proceso de Facturación').
            """)

            col1_df, col2_df = st.columns(2)

            with col1_df:
                st.write("🔍 Ver Detalles de una Factura Específica")
                facturas_dict_view = get_facturas_for_select(selected_db_connector)
                if facturas_dict_view:
                    factura_display_view = st.selectbox("Seleccione Factura para ver sus detalles", options=list(facturas_dict_view.keys()), key="view_df_factura")
                    selected_factura_id_view = facturas_dict_view[factura_display_view]

                    if st.button("Cargar Detalles de Factura Seleccionada", key="load_df_details"):
                        st.write(f"📋 Detalles para Factura UUID: {selected_factura_id_view}")
                        try:
                            query_df = "SELECT detalle_id, producto_id, nombre_producto, cantidad, precio_unitario, subtotal FROM facturacion.Detalle_Factura WHERE factura_id = %s"
                            result_set_df, tiempo_df = selected_db_connector.select_query(query_df, (selected_factura_id_view,))
                            if result_set_df:
                                df_detalles = pd.DataFrame(list(result_set_df))
                                if not df_detalles.empty:
                                    st.dataframe(df_detalles)
                                else:
                                    st.info("No hay detalles para esta factura.")
                            else:
                                st.info("No hay detalles para esta factura o la tabla está vacía.")
                            st.caption(f"Tiempo de consulta: {tiempo_df:.2f} ms")
                        except Exception as e:
                            st.error(f"Error cargando detalles: {e}")
                else:
                    st.warning("No hay facturas registradas para seleccionar.")

            with col2_df:
                st.write("➕ Agregar Detalle a Factura")
                facturas_dict_add = get_facturas_for_select(selected_db_connector)
                productos_dict_add = get_productos_for_select_details(selected_db_connector)

                if not facturas_dict_add or not productos_dict_add:
                    st.warning("Se necesitan Facturas y Productos registrados para agregar un detalle.")
                else:
                    with st.form("add_detalle_c_form", clear_on_submit=True):
                        factura_display_add = st.selectbox("Factura a la que pertenece el detalle", options=list(facturas_dict_add.keys()), key="add_df_factura_id")
                        producto_display_add = st.selectbox("Producto del detalle", options=list(productos_dict_add.keys()), key="add_df_producto_id")
                        cantidad_df_add = st.number_input("Cantidad", min_value=1, step=1, key="add_df_cantidad")
                        
                        submitted_add_df = st.form_submit_button("Guardar Detalle")
                        if submitted_add_df:
                            import uuid
                            if factura_display_add and producto_display_add and cantidad_df_add > 0:
                                import uuid
                                selected_factura_id_add = facturas_dict_add[factura_display_add]
                                selected_producto_data = productos_dict_add[producto_display_add]
                                
                                producto_id_df = selected_producto_data[0]
                                nombre_producto_df = selected_producto_data[1] # Denormalizado
                                precio_unitario_df = selected_producto_data[2] # Denormalizado
                                
                                detalle_id_df = uuid.uuid4()
                                subtotal_df = cantidad_df_add * precio_unitario_df

                                try:
                                    query_insert_df = """
                                    INSERT INTO facturacion.Detalle_Factura 
                                    (factura_id, detalle_id, producto_id, nombre_producto, cantidad, precio_unitario, subtotal) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """
                                    selected_db_connector.execute_query(query_insert_df, (
                                        selected_factura_id_add, detalle_id_df, producto_id_df,
                                        nombre_producto_df, cantidad_df_add, precio_unitario_df, subtotal_df
                                    ))
                                    st.success(f"Detalle agregado a factura {selected_factura_id_add} con UUID de detalle: {detalle_id_df}")
                                    st.rerun() # Para refrescar la vista si se estaban mostrando detalles de esta factura
                                except Exception as e:
                                    st.error(f"Error agregando detalle de factura: {e}")
                            else:
                                st.warning("Todos los campos son obligatorios y cantidad debe ser mayor a 0.")
                
                st.markdown("---")
                st.write("🗑️ Eliminar Detalle de Factura")
                st.caption("Necesitarás el UUID de la Factura y el UUID del Detalle específico.")
                factura_id_del_df_str = st.text_input("UUID de la Factura del detalle a eliminar", key="del_df_fact_uuid")
                detalle_id_del_df_str = st.text_input("UUID del Detalle específico a eliminar", key="del_df_det_uuid")

                if st.button("Eliminar Detalle", key="del_df_btn"):
                    if factura_id_del_df_str and detalle_id_del_df_str:
                        import uuid
                        try:
                            factura_id_obj_del = uuid.UUID(factura_id_del_df_str)
                            detalle_id_obj_del = uuid.UUID(detalle_id_del_df_str)

                            query_del_df = "DELETE FROM facturacion.Detalle_Factura WHERE factura_id = %s AND detalle_id = %s"
                            selected_db_connector.execute_query(query_del_df, (factura_id_obj_del, detalle_id_obj_del))
                            st.success(f"Detalle {detalle_id_obj_del} de factura {factura_id_obj_del} eliminado.")
                            st.rerun()
                        except ValueError:
                            st.error("Uno o ambos UUIDs no son válidos.")
                        except Exception as e:
                            st.error(f"Error eliminando detalle: {e}")
                    else:
                        st.warning("Se requieren ambos UUIDs (Factura y Detalle) para eliminar.")
                
                st.markdown("---")
                st.write("✏️ Editar Detalle de Factura (Simplificado - solo cantidad)")
                st.caption("Necesitarás el UUID de la Factura y el UUID del Detalle específico.")
                factura_id_edit_df_str = st.text_input("UUID de la Factura del detalle a editar", key="edit_df_fact_uuid")
                detalle_id_edit_df_str = st.text_input("UUID del Detalle específico a editar", key="edit_df_det_uuid")
                nueva_cantidad_df = st.number_input("Nueva Cantidad", min_value=0, step=1, key="edit_df_new_qty")

                if st.button("Actualizar Cantidad del Detalle", key="edit_df_btn"):
                    if factura_id_edit_df_str and detalle_id_edit_df_str and nueva_cantidad_df >=0:
                        import uuid
                        try:
                            factura_id_obj_edit = uuid.UUID(factura_id_edit_df_str)
                            detalle_id_obj_edit = uuid.UUID(detalle_id_edit_df_str)

                            # Para actualizar, necesitamos el precio_unitario para recalcular el subtotal.
                            # Lo ideal sería leer el detalle primero.
                            # SELECT precio_unitario FROM facturacion.Detalle_Factura WHERE factura_id = %s AND detalle_id = %s
                            # Por simplicidad aquí, si solo se cambia cantidad, el subtotal se recalcula si se conoce el precio_unitario.
                            # Esta edición es la más compleja de hacer bien en un mantenedor simple sin leer primero.
                            # Una forma sería requerir también el precio unitario o buscarlo.
                            # Vamos a asumir que lo buscamos para recalcular.
                            
                            detalle_actual_rows, _ = selected_db_connector.select_query(
                                "SELECT precio_unitario FROM facturacion.Detalle_Factura WHERE factura_id = %s AND detalle_id = %s",
                                (factura_id_obj_edit, detalle_id_obj_edit)
                            )
                            if detalle_actual_rows:
                                precio_unitario_actual = detalle_actual_rows[0].precio_unitario
                                nuevo_subtotal_df = nueva_cantidad_df * precio_unitario_actual

                                query_edit_df = """UPDATE facturacion.Detalle_Factura 
                                                    SET cantidad = %s, subtotal = %s 
                                                    WHERE factura_id = %s AND detalle_id = %s"""
                                selected_db_connector.execute_query(query_edit_df, (
                                    nueva_cantidad_df, nuevo_subtotal_df, factura_id_obj_edit, detalle_id_obj_edit
                                ))
                                st.success(f"Detalle {detalle_id_obj_edit} actualizado en factura {factura_id_obj_edit}.")
                                st.rerun()
                            else:
                                st.error("No se encontró el detalle para obtener el precio unitario y actualizar.")

                        except ValueError:
                            st.error("Uno o ambos UUIDs no son válidos.")
                        except Exception as e:
                            st.error(f"Error actualizando detalle: {e}")
                    else:
                        st.warning("Se requieren UUIDs (Factura y Detalle) y una nueva cantidad válida.")


        elif db_choice_mant == "MySQL" and tab_choice == "Clientes":
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
        elif db_choice_mant == "MySQL" and tab_choice=="Personal":
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
        elif db_choice_mant == "MySQL" and tab_choice=="Producto":
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
        elif db_choice_mant == "MySQL" and tab_choice == "Factura":
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
        elif db_choice_mant == "MySQL" and tab_choice == "Detalle Factura":
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
        
        if selected_db_connector: selected_db_connector.disconnect()
    
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
            try:
                # Usar pivot_table en lugar de pivot
                # np.mean tomará el promedio si hay múltiples mediciones para la misma operación y BD
                pivot_df = df.pivot_table(index='database', columns='operation', values='time_ms', aggfunc=np.mean)
                pivot_df.plot(kind='bar', ax=ax)
                ax.set_title("Tiempo de Ejecución Promedio por Operación y Base de Datos")
                ax.set_ylabel("Tiempo Promedio (ms)")
                ax.set_xlabel("Base de Datos")
                plt.xticks(rotation=45, ha="right") # Mejorar legibilidad de etiquetas en eje X
                plt.tight_layout() # Ajustar layout
                st.pyplot(fig)
            except Exception as e:
                st.error(f"Error al generar el gráfico de barras: {e}")
                st.caption("Esto puede ocurrir si no hay suficientes datos o si hay problemas con los nombres de las operaciones/DBs.")


            # Gráfico de líneas para comparación
            # Para el gráfico de líneas, si quieres mostrar el promedio de múltiples ejecuciones:
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            try:
                # Agrupar por base de datos y operación, y tomar el promedio del tiempo
                df_mean_times = df.groupby(['database', 'operation'])['time_ms'].mean().reset_index()

                for db_name in df_mean_times['database'].unique():
                    db_data = df_mean_times[df_mean_times['database'] == db_name]
                    # Asegurarse de que las operaciones estén en un orden consistente para el plot
                    # Puedes definir un orden explícito si es necesario
                    # operations_order = ["Carga inicial", "Búsqueda de cliente", ...]
                    # db_data = db_data.set_index('operation').reindex(operations_order).reset_index()

                    ax2.plot(db_data['operation'], db_data['time_ms'], label=db_name, marker='o')

                ax2.set_title("Comparación de Rendimiento Promedio entre Bases de Datos")
                ax2.set_ylabel("Tiempo Promedio (ms)")
                ax2.set_xlabel("Operación")
                plt.xticks(rotation=45, ha="right") # Mejorar legibilidad de etiquetas en eje X
                ax2.legend()
                ax2.grid(True)
                plt.tight_layout() # Ajustar layout
                st.pyplot(fig2)
            except Exception as e:
                st.error(f"Error al generar el gráfico de líneas: {e}")
    
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
    if db_name=="MySQL":
        return {
            "server": "localhost",
            "database": "facturacion",
            "port": "3306",
            "username": "root",
            "password": "toor19"
        }
    elif db_name=="Cassandra":
        return{
            "contact_points":['127.0.0.1'],
            "keyspace":"facturacion"
        }
    """elif db_name=="DB2":
        return {
            "database": "facturacion",
            "username": "db2inst1",
            "password": "password",
            "server": "localhost",
            "port": 50000
        }"""


if __name__ == "__main__":
    main()




