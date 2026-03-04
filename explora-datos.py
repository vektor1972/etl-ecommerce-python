import pandas as pd
import glob
import os

# Verificar que existen los archivos CSV descargados
archivos = glob.glob('data/ecommerce_*.csv')
if not archivos:
    print("? No se encontraron los archivos. Asegurate de descargarlos en la carpeta data/")
    print("   Deber�as tener: ecommerce_orders.csv, ecommerce_customers.csv, etc.")
else:
    print(f"?? Archivos encontrados: {len(archivos)}")
    for f in sorted(archivos):
        print(f"  - {os.path.basename(f)}")
 
# Cargar los CSVs principales
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

# Explorar
print(f"\n?? Resumen:")
print(f"Orders: {len(df_orders)} filas, {len(df_orders.columns)} columnas")
print(f"Order Items: {len(df_order_items)} filas")
print(f"Customers: {len(df_customers)} filas")
print(f"Products: {len(df_products)} filas")

print("\n?? Primeras filas de orders:")
print(df_orders.head())
print("\n?? Info de orders:")
print(df_orders.info())