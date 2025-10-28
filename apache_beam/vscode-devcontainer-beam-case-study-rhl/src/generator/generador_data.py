import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# --- 1. Definición de Parámetros y Listas Globales ---
NUM_PRODUCTOS = 15
NUM_TRANSACCIONES = 300
FECHA_INICIO = datetime(2024, 1, 1)
FECHA_FIN = datetime(2024, 9, 30)

paises = ['Chile', 'Colombia', 'México', 'Argentina', 'España', 'Estados Unidos', 'Alemania', 'Japón']

ciudades_por_pais = {
    'Chile': ['Santiago', 'Concepción', 'Valparaíso'],
    'Colombia': ['Bogotá', 'Medellín', 'Cali'],
    'México': ['Ciudad de México', 'Guadalajara', 'Monterrey'],
    'Argentina': ['Buenos Aires', 'Córdoba', 'Rosario'],
    'España': ['Madrid', 'Barcelona', 'Valencia'],
    'Estados Unidos': ['Nueva York', 'Los Ángeles', 'Chicago'],
    'Alemania': ['Berlín', 'Múnich', 'Hamburgo'],
    'Japón': ['Tokio', 'Osaka', 'Kioto']
}

# --- 2. Creación del Archivo de Dimensión: Productos_Metadata ---
print("Generando Productos_Metadata.csv...")

# Datos de muestra para productos
categorias = ['Electrónica', 'Ropa', 'Hogar', 'Libros', 'Juguetes', 'Deportes']
nombres = [f'Producto {i:02d}' for i in range(1, NUM_PRODUCTOS + 1)]
id_productos = [f'P-{i:04d}' for i in range(1, NUM_PRODUCTOS + 1)]
costos_adquisicion = np.round(np.random.uniform(5.0, 150.0, NUM_PRODUCTOS), 2)
margenes_objetivo = np.round(np.random.choice([0.25, 0.30, 0.35, 0.40], NUM_PRODUCTOS, p=[0.4, 0.3, 0.2, 0.1]), 2)

data_productos = {
    'ID_Producto': id_productos,
    'Nombre_Producto': nombres,
    'Categoria': np.random.choice(categorias, NUM_PRODUCTOS),
    'Costo_Adquisicion': costos_adquisicion,
    'Margen_Objetivo': margenes_objetivo
}

df_productos = pd.DataFrame(data_productos)

# ⭐ CAMBIO CLAVE: Usar decimal=',' al guardar
df_productos.to_csv('Productos_Metadata.csv', index=False, encoding='utf-8', decimal=',') 
print("✅ Productos_Metadata.csv creado (separador decimal: coma).")


# --- 3. Creación del Archivo de Hechos: Ventas_Transacciones ---
print("Generando Ventas_Transacciones.csv...")

data_transacciones = {}

# 3.1. IDs y Fechas
data_transacciones['ID_Transaccion'] = [f'T-{i:06d}' for i in range(1, NUM_TRANSACCIONES + 1)]
rango_dias = (FECHA_FIN - FECHA_INICIO).days
data_transacciones['Fecha_Venta'] = [
    FECHA_INICIO + timedelta(days=random.randint(0, rango_dias), seconds=random.randint(0, 86400))
    for _ in range(NUM_TRANSACCIONES)
]

# 3.2. Claves de Producto y Cantidades
data_transacciones['ID_Producto'] = np.random.choice(id_productos, NUM_TRANSACCIONES)
data_transacciones['Cantidad'] = np.random.randint(1, 6, NUM_TRANSACCIONES)

# 3.3. Precios y Costos de Envío (Lógica de la Versión Anterior)
precios_unitarios = []
costos_envio = []
for prod_id in data_transacciones['ID_Producto']:
    costo_base = df_productos[df_productos['ID_Producto'] == prod_id]['Costo_Adquisicion'].iloc[0]
    margen_base = df_productos[df_productos['ID_Producto'] == prod_id]['Margen_Objetivo'].iloc[0]
    precio = costo_base * (1 + margen_base) * np.random.uniform(0.95, 1.1)
    precios_unitarios.append(np.round(precio, 2))
    costos_envio.append(np.round(np.random.choice([0.00, 5.00, 8.50, 12.00], 1, p=[0.2, 0.5, 0.2, 0.1])[0], 2))

data_transacciones['Precio_Unitario'] = precios_unitarios
data_transacciones['Costo_Envio'] = costos_envio

# 3.4. Columnas Geográficas (PAÍS y CIUDAD)
paises_clientes = np.random.choice(paises, NUM_TRANSACCIONES, p=[0.2, 0.15, 0.15, 0.1, 0.1, 0.1, 0.1, 0.1])
data_transacciones['Pais_Cliente'] = paises_clientes

ciudades_clientes = []
for pais in data_transacciones['Pais_Cliente']:
    ciudades_clientes.append(random.choice(ciudades_por_pais[pais]))
data_transacciones['Ciudad_Cliente'] = ciudades_clientes

# 3.5. Columna Metodo_Pago con Inconsistencias (Para Limpieza)
metodos = ['Tarjeta', 'Transferencia', 'Efectivo', 'PayPal']
metodos_sucios = []
for _ in range(NUM_TRANSACCIONES):
    metodo = random.choice(metodos)
    if random.random() < 0.2:
        if metodo == 'Tarjeta': metodo = 'tarjeta'
        elif metodo == 'Efectivo': metodo = 'EFECTIVO'
    metodos_sucios.append(metodo)
    
data_transacciones['Metodo_Pago'] = metodos_sucios

df_transacciones = pd.DataFrame(data_transacciones)

# ⭐ CAMBIO CLAVE: Usar decimal=',' al guardar
df_transacciones.to_csv('Ventas_Transacciones.csv', index=False, encoding='utf-8', decimal=',')
print("✅ Ventas_Transacciones.csv creado (separador decimal: coma).")

print("\n¡Script de creación de datasets completado! Los decimales ahora usan coma.")