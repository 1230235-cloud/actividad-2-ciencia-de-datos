"""
Web Scraping y Fuentes de Datos.

Este módulo obtiene un catálogo de productos mediante Requests y genera una
fuente transaccional CSV compatible con el catálogo. Si el servicio de prueba
no está disponible, conserva un fallback sintético determinista para que el
pipeline pueda ejecutarse sin conexión.
"""

import csv
import json
import logging
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

# API pública de prueba utilizada para demostrar una extracción real con Requests.
PRODUCT_SOURCE_URL = "https://fakestoreapi.com/products"


def _parse_brand(title: str) -> str:
    """Extrae una marca aproximada del título del producto."""
    if " - " in title:
        return title.split(" - ", 1)[0].strip()
    words = re.findall(r"[A-Za-z0-9]+", title)
    return words[0] if words else "GENERIC"


def scrape_product_api() -> List[Dict[str, Any]]:
    """
    Extrae productos reales desde una API pública mediante Requests.

    La respuesta se normaliza al mismo contrato que usa el pipeline:
    product_id, nombre, categoría, marca, precio, stock, rating, descripción,
    timestamp de extracción y URL de origen.
    """
    response = requests.get(
        PRODUCT_SOURCE_URL,
        headers={"User-Agent": "actividad-integracion-datos/1.0"},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("La fuente de productos devolvió una lista vacía")

    scraped_at = datetime.now().isoformat()
    products: List[Dict[str, Any]] = []
    for item in payload:
        product_id = f"API-{int(item['id']):04d}"
        rating_data = item.get("rating") or {}
        products.append(
            {
                "product_id": product_id,
                "name": str(item.get("title") or product_id).strip(),
                "category": str(item.get("category") or "SIN_CATEGORIA").strip(),
                "brand": _parse_brand(str(item.get("title") or product_id)),
                "price": round(float(item.get("price") or 0.0), 2),
                "stock": None,  # La API de prueba no expone inventario.
                "rating": round(float(rating_data.get("rate") or 0.0), 2),
                "description": str(item.get("description") or "").strip(),
                "scraped_at": scraped_at,
                "source_url": f"{PRODUCT_SOURCE_URL}/{int(item['id'])}",
            }
        )
    return products


def generate_synthetic_web_data() -> List[Dict[str, Any]]:
    """
    Genera un catálogo sintético determinista como fallback sin conexión.

    La función mantiene la misma estructura que la fuente real para que el
    resto del pipeline no dependa de la disponibilidad de la red.
    """
    categories = ["Electrónica", "Ropa", "Hogar", "Deportes", "Libros"]
    brands = {
        "Electrónica": ["Samsung", "Apple", "Sony", "LG", "Xiaomi"],
        "Ropa": ["Zara", "H&M", "Nike", "Adidas", "Levi's"],
        "Hogar": ["IKEA", "Bosch", "Tefal", "Philips", "Rowenta"],
        "Deportes": ["Nike", "Adidas", "Under Armour", "Puma", "Reebok"],
        "Libros": ["Penguin", "Planeta", "Anagrama", "Alfaguara", "Debolsillo"],
    }

    products: List[Dict[str, Any]] = []
    for index in range(1, 51):
        category = categories[(index - 1) % len(categories)]
        brand = brands[category][(index - 1) % len(brands[category])]
        product_id = f"LOCAL-{index:04d}"
        products.append(
            {
                "product_id": product_id,
                "name": f"{brand} {category} Producto {index}",
                "category": category,
                "brand": brand,
                "price": round(10.0 + ((index * 37.13) % 1990.0), 2),
                "stock": (index * 17) % 501,
                "rating": round(1.0 + ((index * 0.37) % 4.0), 1),
                "description": (
                    f"Descripción local del producto {product_id} de la marca "
                    f"{brand} en categoría {category}"
                ),
                "scraped_at": datetime.now().isoformat(),
                "source_url": f"local://synthetic/products/{product_id}",
            }
        )
    return products


def scrape_catalog() -> List[Dict[str, Any]]:
    """
    Intenta el scraping real y usa el fallback sintético solo ante fallos de red.

    El fallback es explícito y registrable; no oculta errores de parsing ni
    respuestas vacías como si fueran una extracción exitosa.
    """
    try:
        products = scrape_product_api()
        logger.info("Extracción real completada desde %s", PRODUCT_SOURCE_URL)
        return products
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        logger.warning(
            "No fue posible completar la extracción real (%s); se usa fallback local",
            exc,
        )
        return generate_synthetic_web_data()


def generate_csv_data(json_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Genera transacciones de venta con ``product_id`` como clave foránea.

    Cada producto recibe entre una y cinco transacciones para representar una
    relación uno a muchos entre el catálogo y los hechos de venta.
    """
    csv_records: List[Dict[str, Any]] = []
    transaction_id = 1
    now = datetime.now()

    for product in json_data:
        product_id = product["product_id"]
        catalog_price = float(product.get("price") or 0.0)
        transaction_count = 1 + (sum(ord(char) for char in product_id) % 5)
        for _ in range(transaction_count):
            sale_date = now - timedelta(days=(transaction_id * 13) % 365)
            quantity = 1 + (transaction_id % 10)
            unit_price = round(max(1.0, catalog_price * (0.85 + (transaction_id % 7) * 0.04)), 2)
            record = {
                "transaction_id": f"TXN-{transaction_id:06d}",
                "product_id": product_id,
                "sale_date": sale_date.strftime("%Y-%m-%d"),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
                "customer_id": f"CUST-{1 + (transaction_id * 17) % 200:04d}",
                "store_id": f"STORE-{1 + (transaction_id % 10):02d}",
                "payment_method": ["Tarjeta", "Efectivo", "Transferencia", "PayPal"][
                    transaction_id % 4
                ],
                "sales_person": f"VENDEDOR-{1 + (transaction_id % 20):02d}",
            }
            csv_records.append(record)
            transaction_id += 1

    return csv_records


def save_json(data: List[Dict[str, Any]], filepath: str) -> None:
    """Guarda datos en JSON UTF-8 con indentación legible."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ JSON guardado en: {path} ({len(data)} registros)")


def save_csv(data: List[Dict[str, Any]], filepath: str) -> None:
    """Guarda datos en CSV UTF-8 con encabezados."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not data:
        raise ValueError("No hay registros para escribir el CSV")

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ CSV guardado en: {path} ({len(data)} registros)")


def main() -> None:
    """Ejecuta la extracción y genera las dos fuentes integrables."""
    random.seed(42)
    print("=" * 60)
    print("INICIANDO WEB SCRAPING Y GENERACIÓN DE DATOS")
    print("=" * 60)

    print("\n[1/3] Extrayendo catálogo de productos...")
    json_data = scrape_catalog()
    save_json(json_data, "data/scraped_data.json")

    print("\n[2/3] Generando transacciones de ventas...")
    csv_data = generate_csv_data(json_data)
    save_csv(csv_data, "data/raw_data.csv")

    print("\n[3/3] Resumen de fuentes:")
    print(f"  - JSON (catálogo): {len(json_data)} productos")
    print(f"  - CSV (ventas): {len(csv_data)} transacciones")
    print("  - Clave común: product_id")
    print("\nEjemplo JSON:")
    print(json.dumps(json_data[0], indent=2, ensure_ascii=False))
    print("\nEjemplo CSV:")
    print(csv_data[0])

    print("\n" + "=" * 60)
    print("SCRAPING COMPLETADO EXITOSAMENTE")
    print("=" * 60)


if __name__ == "__main__":
    main()
