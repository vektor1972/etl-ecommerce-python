# etl.py - Pipeline ETL Completo
import pandas as pd
import os
from datetime import datetime

def extract(data_dir: str = 'data') -> dict:
    """Extrae datos de los archivos CSV."""
    print("?? EXTRACT: Cargando datos...")
    
    tables = {}
    csv_files = {
        'orders': 'ecommerce_orders.csv',
        'order_items': 'ecommerce_order_items.csv',
        'customers': 'ecommerce_customers.csv',
        'products': 'ecommerce_products.csv',
        'categories': 'ecommerce_categories.csv',
    }
    
    for table_name, filename in csv_files.items():
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            tables[table_name] = pd.read_csv(filepath)
            print(f"   {table_name}: {len(tables[table_name])} filas")
        else:
            print(f"   ?? {filename} no encontrado")
    
    return tables

def transform(tables: dict) -> pd.DataFrame:
    """Limpia y transforma los datos."""
    print("\n?? TRANSFORM: Limpiando datos...")
    df = tables['orders'].copy()
    
    # 1. Manejar nulos
    antes = len(df)
    df = df.dropna(subset=['customer_id', 'total_amount'])
    print(f"   Filas eliminadas por nulos: {antes - len(df)}")
    
    # 2. Eliminar duplicados
    antes = len(df)
    df = df.drop_duplicates(subset=['order_id'], keep='last')
    print(f"   Duplicados eliminados: {antes - len(df)}")
    
    # 3. Corregir tipos
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    
    # 4. Agregar campos calculados
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    df['is_high_value'] = df['total_amount'] > 100
    
    print(f"   Filas finales: {len(df)}")
    return df

def load(df: pd.DataFrame, output_dir: str = 'output'):
    """Guarda los resultados."""
    print(f"\n?? LOAD: Guardando en {output_dir}/...")
    os.makedirs(output_dir, exist_ok=True)
    
    # Datos limpios
    df.to_csv(f'{output_dir}/orders_clean.csv', index=False)
    df.to_parquet(f'{output_dir}/orders_clean.parquet', index=False)
    
    # Metricas
    ventas_cliente = df.groupby('customer_id')['total_amount'].sum().reset_index()
    ventas_cliente.to_csv(f'{output_dir}/ventas_por_cliente.csv', index=False)
    
    ventas_mes = df.groupby('order_month')['total_amount'].sum().reset_index()
    ventas_mes.to_csv(f'{output_dir}/ventas_por_mes.csv', index=False)
    
    print("   ? Archivos guardados")

def main():
    print("=" * 50)
    print("ETL Pipeline - E-commerce Data")
    print("=" * 50)
    
    tables = extract('data')
    df_clean = transform(tables)
    load(df_clean)
    
    print("\n? ETL completado exitosamente!")

if __name__ == "__main__":
    main()
