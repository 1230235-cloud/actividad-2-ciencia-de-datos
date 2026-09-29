"""
Procesamiento con DuckDB, Parquet y Data Quality
Pipeline completo: Sniffer, Conversión Parquet, JOIN, Limpieza, Contratos de Datos, Exportación.
"""

import duckdb
import os
import logging
from typing import Optional

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataPipeline:
    """Pipeline de procesamiento de datos con DuckDB."""
    
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._install_extensions()
    
    def _install_extensions(self):
        """Instala extensiones necesarias de DuckDB."""
        try:
            self.conn.execute("INSTALL parquet; LOAD parquet;")
            self.conn.execute("INSTALL json; LOAD json;")
            logger.info("Extensiones DuckDB cargadas correctamente")
        except Exception as e:
            logger.warning(f"Extensiones ya cargadas o error: {e}")
    
    def sniff_and_load_csv(self, csv_path: str) -> str:
        """
        Sniffer de DuckDB: Usa read_csv_auto para detectar esquema automáticamente.
        Retorna el nombre de la vista/tabla creada.
        """
        logger.info(f"[SNIPPER] Analizando CSV: {csv_path}")
        
        # read_csv_auto detecta automáticamente: delimitador, tipos, encoding, headers
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW raw_csv AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """)
        
        # Imprimir esquema inferido
        schema = self.conn.execute("DESCRIBE raw_csv").fetchall()
        logger.info("Esquema inferido del CSV:")
        for col in schema:
            logger.info(f"  - {col[0]}: {col[1]} (nullable: {col[2]}, key: {col[3]})")
        
        # Mostrar muestra de datos
        sample = self.conn.execute("SELECT * FROM raw_csv LIMIT 3").fetchall()
        logger.info(f"Muestra de datos (3 filas): {sample}")
        
        return "raw_csv"
    
    def sniff_and_load_json(self, json_path: str) -> str:
        """
        Sniffer de DuckDB: Usa read_json_auto para detectar esquema automáticamente.
        Retorna el nombre de la vista/tabla creada.
        """
        logger.info(f"[SNIPPER] Analizando JSON: {json_path}")
        
        # read_json_auto detecta automáticamente estructura, tipos, arrays anidados
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW raw_json AS 
            SELECT * FROM read_json_auto('{json_path}')
        """)
        
        # Imprimir esquema inferido
        schema = self.conn.execute("DESCRIBE raw_json").fetchall()
        logger.info("Esquema inferido del JSON:")
        for col in schema:
            logger.info(f"  - {col[0]}: {col[1]} (nullable: {col[2]}, key: {col[3]})")
        
        # Mostrar muestra de datos
        sample = self.conn.execute("SELECT * FROM raw_json LIMIT 3").fetchall()
        logger.info(f"Muestra de datos (3 filas): {sample}")
        
        return "raw_json"
    
    def convert_to_parquet(self, view_name: str, output_path: str) -> None:
        """Convierte una vista/tabla a formato Apache Parquet."""
        logger.info(f"[PARQUET] Convirtiendo {view_name} a {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        self.conn.execute(f"""
            COPY (SELECT * FROM {view_name}) TO '{output_path}' (FORMAT PARQUET)
        """)
        
        # Verificar archivo creado
        count = self.conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        logger.info(f"✓ Parquet guardado: {output_path} ({count} registros)")
    
    def left_join_integration(self) -> str:
        """
        Realiza LEFT JOIN entre datos CSV (izquierda) y JSON (derecha) usando product_id.
        El LEFT JOIN preserva todas las transacciones aunque no tengan info de catálogo.
        """
        logger.info("[JOIN] Ejecutando LEFT JOIN entre CSV (ventas) y JSON (catálogo)")
        
        self.conn.execute("""
            CREATE OR REPLACE VIEW joined_data AS
            SELECT 
                -- Campos de ventas (CSV - tabla izquierda)
                c.transaction_id,
                c.product_id,
                c.sale_date,
                c.quantity,
                c.unit_price,
                c.total_amount,
                c.customer_id,
                c.store_id,
                c.payment_method,
                c.sales_person,
                -- Campos de catálogo (JSON - tabla derecha)
                j.name AS product_name,
                j.category,
                j.brand,
                j.price AS catalog_price,
                j.stock,
                j.rating,
                j.description,
                j.scraped_at,
                j.source_url
            FROM raw_csv c
            LEFT JOIN raw_json j ON c.product_id = j.product_id
        """)
        
        count = self.conn.execute("SELECT COUNT(*) FROM joined_data").fetchone()[0]
        logger.info(f"✓ JOIN completado: {count} registros resultantes")
        
        return "joined_data"
    
    def clean_and_autocorrect(self) -> str:
        """
        Aplica limpieza y autocorrección controlada mediante SQL determinista:
        - Estandarización de texto (TRIM, UPPER)
        - Imputación de nulos no críticos (COALESCE)
        - Limpieza de inconsistencias
        """
        logger.info("[LIMPIEZA] Aplicando autocorrección controlada...")
        
        self.conn.execute("""
            CREATE OR REPLACE VIEW cleaned_data AS
            SELECT 
                -- Claves (no nulas, limpias)
                TRIM(transaction_id) AS transaction_id,
                TRIM(UPPER(product_id)) AS product_id,
                
                -- Fechas estandarizadas
                CASE 
                    WHEN sale_date IS NULL THEN CURRENT_DATE
                    ELSE CAST(sale_date AS DATE)
                END AS sale_date,
                
                -- Numéricos con imputación
                COALESCE(NULLIF(quantity, 0), 1) AS quantity,
                COALESCE(NULLIF(unit_price, 0), 0.0) AS unit_price,
                COALESCE(total_amount, quantity * unit_price) AS total_amount,
                
                -- Texto estandarizado
                TRIM(UPPER(customer_id)) AS customer_id,
                TRIM(UPPER(store_id)) AS store_id,
                TRIM(UPPER(payment_method)) AS payment_method,
                TRIM(UPPER(sales_person)) AS sales_person,
                
                -- Datos de catálogo (pueden ser NULL por LEFT JOIN)
                TRIM(product_name) AS product_name,
                TRIM(UPPER(category)) AS category,
                TRIM(UPPER(brand)) AS brand,
                COALESCE(catalog_price, 0.0) AS catalog_price,
                COALESCE(stock, 0) AS stock,
                COALESCE(rating, 0.0) AS rating,
                TRIM(description) AS description,
                scraped_at,
                source_url,
                
                -- Campos derivados/calculados
                CASE 
                    WHEN unit_price > 0 THEN ROUND((catalog_price - unit_price) / unit_price * 100, 2)
                    ELSE 0 
                END AS price_diff_pct,
                
                CASE 
                    WHEN quantity > 0 THEN 'Venta Normal'
                    ELSE 'Ajuste/Devolución'
                END AS sale_type
            FROM joined_data
        """)
        
        count = self.conn.execute("SELECT COUNT(*) FROM cleaned_data").fetchone()[0]
        logger.info(f"✓ Limpieza completada: {count} registros")
        
        return "cleaned_data"
    
    def validate_data_contracts(self) -> bool:
        """
        Contratos de Datos (Data Contracts): Aserciones SQL para validar calidad.
        Verifica:
        1. No llaves nulas (product_id, transaction_id)
        2. Valores numéricos en rango válido
        3. Fechas válidas
        4. Consistencia referencial básica
        """
        logger.info("[CONTRATOS] Validando contratos de datos...")
        
        violations = []
        
        # 1. Verificar llaves primarias no nulas
        null_txn = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE transaction_id IS NULL OR TRIM(transaction_id) = ''
        """).fetchone()[0]
        
        null_prod = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE product_id IS NULL OR TRIM(product_id) = ''
        """).fetchone()[0]
        
        if null_txn > 0:
            violations.append(f"transaction_id nulo: {null_txn} registros")
        if null_prod > 0:
            violations.append(f"product_id nulo: {null_prod} registros")
        
        # 2. Valores numéricos en rango
        neg_qty = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE quantity < 0
        """).fetchone()[0]
        
        neg_price = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE unit_price < 0 OR total_amount < 0
        """).fetchone()[0]
        
        if neg_qty > 0:
            violations.append(f"Cantidad negativa: {neg_qty} registros")
        if neg_price > 0:
            violations.append(f"Precio/Total negativo: {neg_price} registros")
        
        # 3. Rango de rating (0-5)
        bad_rating = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE rating < 0 OR rating > 5
        """).fetchone()[0]
        
        if bad_rating > 0:
            violations.append(f"Rating fuera de rango [0-5]: {bad_rating} registros")
        
        # 4. Stock no negativo
        neg_stock = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE stock < 0
        """).fetchone()[0]
        
        if neg_stock > 0:
            violations.append(f"Stock negativo: {neg_stock} registros")
        
        # 5. Fechas futuras (más de 1 día en el futuro)
        future_dates = self.conn.execute("""
            SELECT COUNT(*) FROM cleaned_data WHERE sale_date > CURRENT_DATE + INTERVAL '1 day'
        """).fetchone()[0]
        
        if future_dates > 0:
            violations.append(f"Fechas futuras: {future_dates} registros")
        
        if violations:
            logger.error("❌ VIOLACIONES DE CONTRATOS DE DATOS:")
            for v in violations:
                logger.error(f"  - {v}")
            raise ValueError(f"Contratos de datos violados: {'; '.join(violations)}")
        
        logger.info("✓ Todos los contratos de datos validados correctamente")
        return True
    
    def export_final_parquet(self, output_path: str) -> None:
        """Exporta el dataset final integrado y validado a Parquet."""
        logger.info(f"[EXPORTACIÓN] Guardando dataset final en {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        self.conn.execute(f"""
            COPY (SELECT * FROM cleaned_data) TO '{output_path}' (FORMAT PARQUET)
        """)
        
        count = self.conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        logger.info(f"✓ Dataset final exportado: {output_path} ({count} registros)")
        
        # Mostrar esquema final
        schema = self.conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchall()
        logger.info("Esquema final del dataset:")
        for col in schema:
            logger.info(f"  - {col[0]}: {col[1]}")
    
    def get_summary_stats(self) -> dict:
        """Obtiene estadísticas resumen del dataset final."""
        stats = {}
        
        stats['total_records'] = self.conn.execute("SELECT COUNT(*) FROM cleaned_data").fetchone()[0]
        stats['unique_products'] = self.conn.execute("SELECT COUNT(DISTINCT product_id) FROM cleaned_data").fetchone()[0]
        stats['unique_customers'] = self.conn.execute("SELECT COUNT(DISTINCT customer_id) FROM cleaned_data").fetchone()[0]
        stats['total_revenue'] = self.conn.execute("SELECT SUM(total_amount) FROM cleaned_data").fetchone()[0]
        stats['avg_rating'] = self.conn.execute("SELECT AVG(rating) FROM cleaned_data WHERE rating > 0").fetchone()[0]
        stats['categories'] = self.conn.execute("SELECT COUNT(DISTINCT category) FROM cleaned_data WHERE category IS NOT NULL").fetchone()[0]
        
        return stats
    
    def close(self):
        """Cierra la conexión."""
        self.conn.close()


def run_pipeline():
    """Ejecuta el pipeline completo."""
    print("=" * 60)
    print("INICIANDO PIPELINE DE PROCESAMIENTO DUCKDB")
    print("=" * 60)
    
    pipeline = DataPipeline()
    
    try:
        # 1. Sniffer y carga de fuentes
        print("\n[1/6] Sniffer y carga de CSV...")
        pipeline.sniff_and_load_csv("data/raw_data.csv")
        
        print("\n[2/6] Sniffer y carga de JSON...")
        pipeline.sniff_and_load_json("data/scraped_data.json")
        
        # 2. Conversión a Parquet
        print("\n[3/6] Conversión a Parquet...")
        pipeline.convert_to_parquet("raw_csv", "data/csv_data.parquet")
        pipeline.convert_to_parquet("raw_json", "data/json_data.parquet")
        
        # 3. Integración JOIN
        print("\n[4/6] Integración LEFT JOIN...")
        pipeline.left_join_integration()
        
        # 4. Limpieza y autocorrección
        print("\n[5/6] Limpieza y autocorrección controlada...")
        pipeline.clean_and_autocorrect()
        
        # 5. Contratos de datos
        print("\n[6/6] Validación de contratos de datos...")
        pipeline.validate_data_contracts()
        
        # 6. Exportación final
        print("\n[7/7] Exportación final...")
        pipeline.export_final_parquet("data/dataset_final.parquet")
        
        # Resumen
        stats = pipeline.get_summary_stats()
        print("\n" + "=" * 60)
        print("RESUMEN DEL PIPELINE")
        print("=" * 60)
        print(f"  Registros totales: {stats['total_records']:,}")
        print(f"  Productos únicos: {stats['unique_products']:,}")
        print(f"  Clientes únicos: {stats['unique_customers']:,}")
        print(f"  Ingreso total: ${stats['total_revenue']:,.2f}")
        print(f"  Rating promedio: {stats['avg_rating']:.2f}")
        print(f"  Categorías: {stats['categories']}")
        print("=" * 60)
        print("PIPELINE COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Error en pipeline: {e}")
        raise
    finally:
        pipeline.close()


if __name__ == "__main__":
    run_pipeline()