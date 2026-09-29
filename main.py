#!/usr/bin/env python3
"""
Main Execution Script
Orquesta la ejecución completa de la actividad: Scraping -> Pipeline -> Visualizaciones
"""

import sys
import os
import subprocess
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_script(script_name: str, description: str) -> bool:
    """Ejecuta un script Python y retorna True si fue exitoso."""
    logger.info(f"{'='*60}")
    logger.info(f"EJECUTANDO: {description}")
    logger.info(f"Script: {script_name}")
    logger.info(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                logger.info(f"  {line}")
        
        if result.returncode != 0:
            logger.error(f"ERROR en {script_name}:")
            if result.stderr:
                for line in result.stderr.strip().split('\n'):
                    logger.error(f"  {line}")
            return False
        
        logger.info(f"✓ {description} completado exitosamente")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error(f"TIMEOUT: {script_name} excedió 5 minutos")
        return False
    except Exception as e:
        logger.error(f"EXCEPCIÓN en {script_name}: {e}")
        return False


def verify_deliverables() -> bool:
    """Verifica que todos los entregables existan."""
    logger.info(f"{'='*60}")
    logger.info("VERIFICANDO ENTREGABLES")
    logger.info(f"{'='*60}")
    
    required_files = [
        # Datos fuente
        "data/scraped_data.json",
        "data/raw_data.csv",
        # Parquet intermedios
        "data/csv_data.parquet",
        "data/json_data.parquet",
        # Dataset final
        "data/dataset_final.parquet",
        # Visualizaciones
        "output/grafica_1.png",
        "output/grafica_2.png",
    ]
    
    optional_files = [
        "output/grafica_metodos_pago.png",
        "report_draft.md",
    ]
    
    all_ok = True
    
    print("\n📋 ENTREGABLES REQUERIDOS:")
    for f in required_files:
        exists = Path(f).exists()
        size = Path(f).stat().st_size if exists else 0
        status = "✅" if exists else "❌"
        print(f"  {status} {f} ({size:,} bytes)")
        if not exists:
            all_ok = False
    
    print("\n📋 ENTREGABLES OPCIONALES:")
    for f in optional_files:
        exists = Path(f).exists()
        size = Path(f).stat().st_size if exists else 0
        status = "✅" if exists else "⚠️"
        print(f"  {status} {f} ({size:,} bytes)")
    
    # Verificar contenido básico del dataset final
    if Path("data/dataset_final.parquet").exists():
        try:
            import duckdb
            conn = duckdb.connect()
            conn.execute("INSTALL parquet; LOAD parquet;")
            count = conn.execute("SELECT COUNT(*) FROM read_parquet('data/dataset_final.parquet')").fetchone()[0]
            cols = conn.execute("DESCRIBE SELECT * FROM read_parquet('data/dataset_final.parquet')").fetchall()
            conn.close()
            print(f"\n📊 DATASET FINAL: {count:,} registros, {len(cols)} columnas")
            print(f"   Columnas: {[c[0] for c in cols]}")
        except Exception as e:
            logger.warning(f"No se pudo verificar dataset final: {e}")
    
    return all_ok


def main():
    """Función principal que orquesta todo el flujo."""
    print("=" * 60)
    print("ACTIVIDAD: INTEGRACIÓN Y ANÁLISIS DE DATOS MULTIFUENTE")
    print("Senior Data Engineer & Data Analyst")
    print("=" * 60)
    
    # Verificar que estamos en el directorio correcto
    if not Path("src").exists():
        logger.error("Directorio 'src' no encontrado. Ejecutar desde la raíz del proyecto.")
        sys.exit(1)
    
    # Cambiar al directorio del script si es necesario
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Paso 1: Web Scraping y Generación de Datos
    print("\n🚀 PASO 1: Web Scraping y Fuentes de Datos")
    if not run_script("src/scraper.py", "Web Scraping y Generación CSV/JSON"):
        logger.error("Falló el scraping. Abortando.")
        sys.exit(1)
    
    # Paso 2: Pipeline DuckDB
    print("\n🚀 PASO 2: Pipeline de Procesamiento DuckDB")
    if not run_script("src/pipeline.py", "Pipeline DuckDB (Sniffer, Parquet, JOIN, Limpieza, Contratos)"):
        logger.error("Falló el pipeline. Abortando.")
        sys.exit(1)
    
    # Paso 3: Visualizaciones
    print("\n🚀 PASO 3: Visualización e Insight")
    if not run_script("src/visualizations.py", "Generación de Gráficas Analíticas"):
        logger.error("Falló la visualización. Abortando.")
        sys.exit(1)
    
    # Verificación final
    print("\n🚀 VERIFICACIÓN FINAL")
    success = verify_deliverables()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ ACTIVIDAD COMPLETADA EXITOSAMENTE - 100% ENTREGABLES")
    else:
        print("⚠️  ACTIVIDAD COMPLETADA CON ADVERTENCIAS - Revisar faltantes")
    print("=" * 60)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())