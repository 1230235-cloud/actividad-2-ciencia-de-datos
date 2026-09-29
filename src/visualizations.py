"""
Visualización e Insight
Genera gráficas analíticas a partir del dataset final procesado.
"""

import duckdb
import matplotlib
matplotlib.use('Agg')  # Backend no interactivo
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import pandas as pd
import numpy as np
import os
import logging
from typing import Optional

# Configurar estilo
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 11

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_final_dataset(parquet_path: str) -> pd.DataFrame:
    """Carga el dataset final desde Parquet usando DuckDB para eficiencia."""
    logger.info(f"Cargando dataset desde: {parquet_path}")
    
    conn = duckdb.connect()
    conn.execute("INSTALL parquet; LOAD parquet;")
    
    df = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    conn.close()
    
    logger.info(f"Dataset cargado: {df.shape[0]} filas x {df.shape[1]} columnas")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepara y enriquece datos para visualización."""
    df = df.copy()
    
    # Asegurar tipos correctos
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['month'] = df['sale_date'].dt.to_period('M').dt.to_timestamp()
    df['weekday'] = df['sale_date'].dt.day_name()
    
    # Categorías ordenadas por frecuencia
    cat_order = df['category'].value_counts().index.tolist()
    df['category'] = pd.Categorical(df['category'], categories=cat_order, ordered=True)
    
    return df


def grafica_1_ventas_por_categoria_mes(df: pd.DataFrame, output_path: str) -> None:
    """
    Gráfica 1: Ventas totales por categoría y mes (Heatmap/Stacked Bar).
    Responde: ¿Cómo evoluciona la venta por categoría a lo del tiempo?
    """
    logger.info("Generando Gráfica 1: Ventas por categoría y mes...")
    
    # Agregar datos
    pivot_data = df.groupby(['month', 'category'])['total_amount'].sum().unstack(fill_value=0)
    
    fig, axes = plt.subplots(2, 1, figsize=(14, 12), gridspec_kw={'height_ratios': [3, 1]})
    
    # Subplot 1: Stacked Bar Chart
    ax1 = axes[0]
    pivot_data.plot(kind='bar', stacked=True, ax=ax1, colormap='viridis', width=0.8)
    ax1.set_title('Evolución Mensual de Ventas por Categoría (Stacked)', fontsize=16, fontweight='bold', pad=20)
    ax1.set_xlabel('Mes', fontsize=12)
    ax1.set_ylabel('Ventas Totales ($)', fontsize=12)
    ax1.legend(title='Categoría', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=10)
    ax1.tick_params(axis='x', rotation=45)
    
    # Formatear eje Y con separadores de miles
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'${x:,.0f}'))
    
    # Subplot 2: Heatmap de proporción
    ax2 = axes[1]
    # Normalizar por fila (mes) para ver proporción
    pivot_pct = pivot_data.div(pivot_data.sum(axis=1), axis=0) * 100
    sns.heatmap(pivot_pct.T, annot=True, fmt='.1f', cmap='RdYlGn', 
                cbar_kws={'label': '% de Ventas del Mes'}, ax=ax2)
    ax2.set_title('Proporción de Ventas por Categoría (% del total mensual)', fontsize=14, pad=15)
    ax2.set_xlabel('Mes', fontsize=11)
    ax2.set_ylabel('Categoría', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"✓ Gráfica 1 guardada en: {output_path}")


def grafica_2_analisis_rendimiento_productos(df: pd.DataFrame, output_path: str) -> None:
    """
    Gráfica 2: Análisis de rendimiento - Precio vs Rating vs Ventas (Scatter + Marginal).
    Responde: ¿Qué relación existe entre precio de catálogo, rating y volumen de ventas?
    """
    logger.info("Generando Gráfica 2: Análisis de rendimiento productos...")
    
    # Agregar a nivel producto
    product_stats = df.groupby(['product_id', 'product_name', 'category', 'brand', 'catalog_price', 'rating']).agg(
        total_sales=('total_amount', 'sum'),
        total_quantity=('quantity', 'sum'),
        num_transactions=('transaction_id', 'count'),
        avg_unit_price=('unit_price', 'mean')
    ).reset_index()
    
    # Filtrar productos con datos de catálogo válidos
    product_stats = product_stats[product_stats['catalog_price'] > 0].copy()
    
    # Crear figura con subplots
    fig = plt.figure(figsize=(16, 10))
    
    # GridSpec para layout personalizado
    gs = fig.add_gridspec(2, 2, height_ratios=[3, 1], width_ratios=[3, 1], 
                          hspace=0.3, wspace=0.3)
    
    # Scatter principal: Precio vs Rating, tamaño = ventas, color = categoría
    ax_main = fig.add_subplot(gs[0, 0])
    
    categories = product_stats['category'].unique()
    colors = plt.cm.Set2(np.linspace(0, 1, len(categories)))
    cat_color_map = dict(zip(categories, colors))
    
    for cat in categories:
        cat_data = product_stats[product_stats['category'] == cat]
        scatter = ax_main.scatter(
            cat_data['catalog_price'], 
            cat_data['rating'],
            s=cat_data['total_sales'] / product_stats['total_sales'].max() * 1000 + 50,
            c=[cat_color_map[cat]], 
            alpha=0.7,
            edgecolors='white',
            linewidth=0.5,
            label=cat
        )
    
    ax_main.set_xlabel('Precio de Catálogo ($)', fontsize=12)
    ax_main.set_ylabel('Rating (1-5)', fontsize=12)
    ax_main.set_title('Rendimiento de Productos: Precio vs Rating vs Volumen de Ventas', 
                      fontsize=14, fontweight='bold', pad=20)
    ax_main.set_ylim(0.5, 5.5)
    ax_main.legend(title='Categoría', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)
    ax_main.grid(True, alpha=0.3)
    
    # Histograma marginal superior (distribución de precios)
    ax_top = fig.add_subplot(gs[1, 0], sharex=ax_main)
    ax_top.hist(product_stats['catalog_price'], bins=30, color='steelblue', alpha=0.7, edgecolor='white')
    ax_top.set_ylabel('Frecuencia', fontsize=10)
    ax_top.set_title('Distribución de Precios de Catálogo', fontsize=11)
    ax_top.grid(True, alpha=0.3)
    
    # Histograma marginal derecho (distribución de ratings)
    ax_right = fig.add_subplot(gs[0, 1], sharey=ax_main)
    ax_right.hist(product_stats['rating'], bins=20, orientation='horizontal', 
                  color='coral', alpha=0.7, edgecolor='white')
    ax_right.set_xlabel('Frecuencia', fontsize=10)
    ax_right.set_title('Distribución de Ratings', fontsize=11)
    ax_right.grid(True, alpha=0.3)
    
    # Tabla resumen inferior derecha
    ax_table = fig.add_subplot(gs[1, 1])
    ax_table.axis('off')
    
    # Top 5 productos por ventas
    top5 = product_stats.nlargest(5, 'total_sales')[['product_name', 'category', 'total_sales', 'rating']]
    table_data = [['Producto', 'Categoría', 'Ventas ($)', 'Rating']]
    for _, row in top5.iterrows():
        table_data.append([
            row['product_name'][:25], 
            row['category'], 
            f"${row['total_sales']:,.0f}", 
            f"{row['rating']:.1f}"
        ])
    
    table = ax_table.table(cellText=table_data, loc='center', cellLoc='left',
                           colWidths=[0.4, 0.2, 0.2, 0.15])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.5)
    # Estilo header
    for i in range(4):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
    ax_table.set_title('Top 5 Productos por Ventas', fontsize=11, pad=10)
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"✓ Gráfica 2 guardada en: {output_path}")


def grafica_3_adicional_distribucion_pagos(df: pd.DataFrame, output_path: str) -> None:
    """
    Gráfica adicional: Distribución de métodos de pago y ticket promedio.
    (Bonus - tercera visualización para mayor insight)
    """
    logger.info("Generando Gráfica adicional: Métodos de pago...")
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Pie chart métodos de pago
    ax1 = axes[0]
    payment_counts = df['payment_method'].value_counts()
    colors = plt.cm.Pastel1(np.linspace(0, 1, len(payment_counts)))
    wedges, texts, autotexts = ax1.pie(payment_counts.values, labels=payment_counts.index, 
                                        autopct='%1.1f%%', colors=colors, startangle=90)
    ax1.set_title('Distribución de Métodos de Pago', fontsize=14, fontweight='bold')
    
    # Bar chart ticket promedio por método
    ax2 = axes[1]
    avg_ticket = df.groupby('payment_method')['total_amount'].mean().sort_values(ascending=True)
    bars = ax2.barh(avg_ticket.index, avg_ticket.values, color=colors[::-1], edgecolor='white')
    ax2.set_xlabel('Ticket Promedio ($)', fontsize=12)
    ax2.set_title('Ticket Promedio por Método de Pago', fontsize=14, fontweight='bold')
    ax2.bar_label(bars, fmt='$%.2f', padding=3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    logger.info(f"✓ Gráfica adicional guardada en: {output_path}")


def generate_visualizations():
    """Función principal para generar todas las visualizaciones."""
    print("=" * 60)
    print("GENERANDO VISUALIZACIONES")
    print("=" * 60)
    
    # Cargar datos
    df = load_final_dataset("data/dataset_final.parquet")
    df = prepare_data(df)
    
    # Crear directorio output
    os.makedirs("output", exist_ok=True)
    
    # Generar gráficas requeridas
    print("\n[1/2] Generando Gráfica 1: Ventas por Categoría y Mes...")
    grafica_1_ventas_por_categoria_mes(df, "output/grafica_1.png")
    
    print("\n[2/2] Generando Gráfica 2: Análisis Rendimiento Productos...")
    grafica_2_analisis_rendimiento_productos(df, "output/grafica_2.png")
    
    # Gráfica adicional (bonus)
    print("\n[+] Generando Gráfica adicional: Métodos de Pago...")
    grafica_3_adicional_distribucion_pagos(df, "output/grafica_metodos_pago.png")
    
    print("\n" + "=" * 60)
    print("VISUALIZACIONES COMPLETADAS")
    print("=" * 60)
    print("Archivos generados:")
    print("  - output/grafica_1.png (Stacked Bar + Heatmap)")
    print("  - output/grafica_2.png (Scatter + Marginals + Tabla)")
    print("  - output/grafica_metodos_pago.png (Bonus: Pie + Bar)")
    print("=" * 60)


if __name__ == "__main__":
    generate_visualizations()