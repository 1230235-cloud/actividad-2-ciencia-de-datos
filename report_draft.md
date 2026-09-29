# Actividad: Integración y análisis de datos provenientes de múltiples fuentes

## 1. Resumen ejecutivo

Esta actividad implementa un flujo reproducible de integración de datos que combina dos fuentes con granularidades diferentes:

1. **Catálogo de productos** en JSON (`data/scraped_data.json`), obtenido mediante un proceso de extracción web.
2. **Transacciones de ventas** en CSV (`data/raw_data.csv`), que representa un sistema transaccional o ERP.

La clave de integración es `product_id`. El catálogo tiene un registro por producto, mientras que el CSV puede contener varias transacciones por producto. Por esa razón, la relación lógica es **uno a muchos (1:N)** y la operación de integración utiliza un `LEFT JOIN` desde las ventas hacia el catálogo.

El pipeline está construido en Python y DuckDB. DuckDB se utiliza como motor analítico embebido para detectar esquemas, leer formatos heterogéneos, convertir datos a Parquet, ejecutar el JOIN, aplicar transformaciones deterministas y validar contratos de calidad antes de exportar el resultado final. El resultado integrado se guarda en `data/dataset_final.parquet` y se analizan dos visualizaciones en `output/`.

## 2. Arquitectura del flujo

```text
Fuente web / catálogo              Sistema transaccional
        │                                      │
        ▼                                      ▼
 data/scraped_data.json              data/raw_data.csv
        │                                      │
        └────────────── DuckDB ───────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
 read_json_auto                 read_csv_auto
          │                             │
          └─────────── LEFT JOIN ───────┘
                         │
          Limpieza + autocorrección SQL
                         │
          Contratos de datos (aserciones)
                         │
              data/dataset_final.parquet
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
 output/grafica_1.png        output/grafica_2.png
```

La ejecución completa se realiza con:

```bash
python main.py
```

`main.py` ejecuta secuencialmente:

1. `src/scraper.py`: genera o actualiza las fuentes JSON y CSV.
2. `src/pipeline.py`: realiza el procesamiento con DuckDB.
3. `src/visualizations.py`: genera las gráficas analíticas.
4. Verifica la existencia y estructura básica de los entregables.

## 3. Relación entre fuentes y justificación del LEFT JOIN

### 3.1 Granularidad de cada fuente

El JSON de catálogo tiene una fila por producto. Sus atributos principales son:

- `product_id`: identificador estable del producto.
- `name`, `category`, `brand`: descripción comercial.
- `price`, `stock`, `rating`: atributos del catálogo.
- `description`, `scraped_at`, `source_url`: metadatos de procedencia.

El CSV de ventas tiene una fila por transacción. Sus atributos principales son:

- `transaction_id`: identificador único de la transacción.
- `product_id`: referencia al producto vendido.
- `sale_date`, `quantity`, `unit_price`, `total_amount`: hechos comerciales.
- `customer_id`, `store_id`, `payment_method`, `sales_person`: dimensiones operativas.

Un producto puede aparecer en varias filas de ventas, por lo que el CSV es la tabla de hechos y el JSON es la dimensión de producto. La cardinalidad esperada es:

```text
raw_data.csv (muchas transacciones)  N ── product_id ── 1  scraped_data.json (un producto)
```

### 3.2 Por qué se usa LEFT JOIN

La consulta de integración coloca el CSV a la izquierda:

```sql
FROM raw_csv AS c
LEFT JOIN raw_json AS j
  ON c.product_id = j.product_id
```

El `LEFT JOIN` se eligió por cuatro razones:

1. **Preserva la integridad de los hechos comerciales.** Todas las transacciones deben permanecer en el dataset final, incluso si el catálogo no contiene el producto o si la extracción web falla para un registro.
2. **Hace visibles los problemas de cobertura del catálogo.** Las filas sin coincidencia quedan identificables mediante atributos nulos en las columnas de catálogo.
3. **Evita silenciar ventas.** Un `INNER JOIN` eliminaría transacciones huérfanas y distorsionaría ingresos, cantidades y tendencias temporales.
4. **Permite una estrategia de calidad explícita.** Los campos opcionales del catálogo pueden imputarse de forma controlada, mientras las claves críticas siguen siendo validadas por contratos de datos.

En este caso, `transaction_id` es la clave primaria de la tabla de hechos y `product_id` actúa como clave foránea hacia la dimensión de producto. La consulta no duplica transacciones siempre que `product_id` sea único en el catálogo; el contrato de calidad debe garantizar esa condición antes de considerar el resultado como analíticamente válido.

## 4. Sniffer de DuckDB

DuckDB ofrece funciones de lectura automática que inspeccionan una muestra del archivo y proponen un esquema sin requerir una declaración manual de tipos.

### 4.1 CSV

El pipeline utiliza:

```sql
SELECT *
FROM read_csv_auto('data/raw_data.csv')
```

`read_csv_auto` detecta, entre otros aspectos:

- Delimitador de columnas.
- Presencia y nombre de encabezados.
- Tipos de datos (texto, entero, decimal, fecha, etc.).
- Valores nulos y codificación compatible.
- Estructura general del archivo.

El resultado se materializa como la vista `raw_csv`. Después, `DESCRIBE raw_csv` imprime en consola cada columna, su tipo inferido y metadatos de nulidad.

### 4.2 JSON

El pipeline utiliza:

```sql
SELECT *
FROM read_json_auto('data/scraped_data.json')
```

`read_json_auto` infiere la estructura de objetos JSON, los nombres de campos y los tipos de datos. El resultado se expone como la vista `raw_json`. Esta capacidad es útil cuando la fuente cambia de formato o cuando se incorporan nuevas columnas, ya que el pipeline puede detectarlas sin reescribir todo el esquema inicial.

### 4.3 Ventajas y controles

El sniffer reduce acoplamiento y acelera la ingestión, pero no reemplaza los contratos de datos. La inferencia automática puede cambiar si cambia la muestra o si aparecen valores mixtos. Por ello, después del sniffer se aplican transformaciones explícitas y validaciones deterministas antes de exportar el dataset final.

## 5. Conversión a Apache Parquet

Cada fuente se guarda de forma independiente:

- `data/csv_data.parquet`: transacciones de ventas.
- `data/json_data.parquet`: catálogo de productos.

La conversión se realiza con `COPY ... (FORMAT PARQUET)`. Parquet es un formato columnar que ofrece:

- Compresión eficiente.
- Preservación de tipos.
- Lectura por columnas para consultas analíticas.
- Mejor rendimiento que CSV/JSON en procesos repetidos.
- Compatibilidad con DuckDB, pandas y herramientas de analítica modernas.

Mantener los Parquet intermedios permite auditar la ingesta original y reutilizarla sin volver a leer los archivos fuente en cada iteración.

## 6. Limpieza y autocorrección controlada

La autocorrección se implementa mediante SQL determinista sobre la vista `joined_data`. Se evitan reglas probabilísticas o modelos que puedan modificar los datos de forma no reproducible.

### 6.1 Estandarización de texto

Se aplican `TRIM` y `UPPER` a claves y dimensiones textuales:

```sql
TRIM(UPPER(product_id)) AS product_id
TRIM(UPPER(payment_method)) AS payment_method
TRIM(UPPER(store_id)) AS store_id
```

Esto elimina espacios accidentales al inicio o al final y normaliza variantes como `tarjeta`, `TARJETA  ` o `Tarjeta`.

### 6.2 Fechas

Las fechas vacías se sustituyen por `CURRENT_DATE`; los valores válidos se convierten explícitamente a `DATE`:

```sql
CASE
  WHEN sale_date IS NULL OR TRIM(sale_date) = '' THEN CURRENT_DATE
  ELSE CAST(sale_date AS DATE)
END AS sale_date
```

Esta regla es adecuada para un campo no crítico de la dimensión temporal. En un entorno de producción, la fecha imputada debería marcarse con una bandera de calidad para evitar interpretarla como una venta realmente ocurrida ese día.

### 6.3 Nulos no críticos

Se utiliza `COALESCE` para valores que pueden recuperarse sin inventar información crítica:

```sql
COALESCE(NULLIF(quantity, 0), 1) AS quantity
COALESCE(NULLIF(unit_price, 0), 0.0) AS unit_price
COALESCE(stock, 0) AS stock
COALESCE(rating, 0.0) AS rating
```

`NULLIF(quantity, 0)` convierte ceros no válidos en nulos antes de la imputación. El valor cero en `catalog_price` se conserva como sentinel controlado para indicar que no existe precio de catálogo disponible, no como un precio comercial real.

### 6.4 Campos derivados

El pipeline calcula:

- `price_diff_pct`: diferencia porcentual entre precio de catálogo y precio unitario de venta.
- `sale_type`: clasificación determinista entre venta normal y ajuste/devolución.

Estos campos facilitan análisis posteriores sin alterar las fuentes originales.

## 7. Contratos de datos (Data Contracts)

Un contrato de datos define condiciones mínimas que una tabla debe cumplir para ser considerada apta para consumo. En este proyecto las aserciones se ejecutan con SQL antes de guardar `dataset_final.parquet`.

### 7.1 Reglas implementadas

El contrato valida:

1. **Claves críticas no nulas:**
   - `transaction_id` no puede ser nulo ni una cadena vacía.
   - `product_id` no puede ser nulo ni una cadena vacía.
2. **Cantidad no negativa:**
   - `quantity >= 0`.
3. **Importes no negativos:**
   - `unit_price >= 0`.
   - `total_amount >= 0`.
4. **Rating dentro del dominio esperado:**
   - `0 <= rating <= 5`.
5. **Stock no negativo:**
   - `stock >= 0`.
6. **Fechas razonables:**
   - No se aceptan fechas de venta posteriores a un día del día actual.

Cada regla se implementa como una consulta de conteo de violaciones. Si alguna regla devuelve un conteo mayor que cero, el pipeline registra el detalle y termina con error; no exporta silenciosamente datos no conformes.

### 7.2 Diferencia entre corrección y validación

La autocorrección se aplica a problemas conocidos y reversibles, como espacios, mayúsculas y nulos no críticos. El contrato se aplica después para comprobar que la transformación no introdujo valores fuera de dominio. Esta separación evita que una corrección automática oculte fallas estructurales.

### 7.3 Extensiones recomendadas para producción

Para un entorno productivo se podrían añadir:

- Unicidad de `transaction_id`.
- Unicidad de `product_id` en el catálogo.
- Existencia de una coincidencia de catálogo para cada venta.
- Consistencia entre `total_amount`, `quantity` y `unit_price`.
- Validación de formato de URL y timestamp.
- Control de duplicados exactos.
- Umbrales de frescura y volumen.
- Metadatos de linaje y versión del contrato.

## 8. Exportación final

Después de aprobar los contratos, la vista `cleaned_data` se exporta a:

```text
data/dataset_final.parquet
```

El archivo contiene tanto los hechos de ventas como las dimensiones enriquecidas del catálogo. Esto permite que el equipo de analítica consulte una única fuente preparada, manteniendo la trazabilidad mediante `transaction_id`, `product_id` y `source_url`.

## 9. Visualizaciones e insights

### 9.1 Gráfica 1: evolución mensual de ventas por categoría

`output/grafica_1.png` combina:

- Un gráfico de barras apiladas con ventas mensuales por categoría.
- Un heatmap con la proporción porcentual de cada categoría dentro de cada mes.

Preguntas que responde:

- ¿Qué categorías generan más ingresos?
- ¿La composición de ventas cambia a lo largo del tiempo?
- ¿Hay estacionalidad o concentración en determinados meses?
- ¿El crecimiento total proviene de una categoría dominante o de varias categorías?

El gráfico apilado es efectivo para comparar el total mensual y la contribución absoluta de cada categoría. El heatmap complementa la lectura al mostrar la composición relativa, lo que evita que una categoría con mayor escala económica opaque cambios de proporción en categorías menores.

### 9.2 Gráfica 2: rendimiento de productos

`output/grafica_2.png` combina:

- Scatter plot de precio de catálogo versus rating.
- Tamaño de burbuja proporcional al volumen de ventas.
- Color por categoría.
- Histogramas marginales de precios y ratings.
- Tabla de los cinco productos con mayores ventas.

Preguntas que responde:

- ¿Los productos mejor valorados generan más ventas?
- ¿Existe una concentración de ventas en determinados rangos de precio?
- ¿Qué categorías tienen mayor presencia comercial?
- ¿Cuáles son los productos prioritarios para promociones, reposición o revisión de precio?

La visualización multivariada permite identificar productos con alta demanda y alto rating, así como productos caros con baja rotación. Los histogramas marginales ayudan a interpretar la densidad de precios y valoraciones, mientras la tabla de líderes convierte el patrón visual en una lista accionable.

### 9.3 Comparación de efectividad visual

La **gráfica 1** es más efectiva para análisis temporal y de composición. Su lectura es directa para responder preguntas de evolución, estacionalidad y participación de categorías. La combinación de barras y heatmap equilibra valores absolutos y porcentuales, aunque puede volverse densa si hay muchos meses o categorías.

La **gráfica 2** es más efectiva para análisis de portafolio y priorización. El scatter multivariado revela relaciones entre precio, rating y ventas que una tabla agregada no muestra fácilmente. Su principal limitación es la posible superposición de puntos; el tamaño de burbuja y la tabla de apoyo mitigan ese riesgo, pero una interacción con tooltips sería conveniente en una versión web.

En conjunto, las dos gráficas son complementarias: la primera explica el comportamiento agregado en el tiempo y la segunda explica la distribución y el rendimiento a nivel de producto.

## 10. Cortafuegos semántico con modelo Open Source

### 10.1 Selección recomendada: Llama 3 8B Instruct

Se recomienda **Llama 3 8B Instruct** como modelo Open Source para un cortafuegos semántico. La elección se basa en:

- Licencia y disponibilidad abiertas para implementación propia.
- Buen equilibrio entre calidad de comprensión lingüística y costo computacional.
- Capacidad de clasificar intención, contexto y riesgo en texto no estructurado.
- Posibilidad de ejecutarlo on-premise o en una infraestructura controlada.
- Compatibilidad con técnicas de cuantización para reducir memoria y latencia.
- Facilidad de fine-tuning o adaptación mediante prompts para reglas específicas del negocio.

Mistral 7B Instruct es una alternativa válida cuando se prioriza un modelo más ligero. Llama 3 8B se selecciona aquí por su balance general entre razonamiento, cobertura de lenguaje y control operativo.

### 10.2 Función del cortafuegos semántico

El cortafuegos semántico no reemplaza los contratos de datos. Actúa en una capa anterior o paralela para evaluar contenido textual, por ejemplo:

- Descripciones de productos.
- Comentarios de clientes.
- Campos libres de transacciones.
- URLs o metadatos de procedencia.
- Solicitudes de transformación o enriquecimiento.

Su función es detectar y bloquear entradas que puedan ser maliciosas, ambiguas, fuera de dominio o incompatibles con la política de datos. Ejemplos de señales de riesgo:

- Instrucciones de inyección de prompts.
- Solicitudes de revelar secretos o credenciales.
- Texto ofensivo o no permitido por política.
- Descripciones que contradicen la categoría del producto.
- URLs o identificadores con patrones anómalos.
- Información personal sensible no autorizada.

### 10.3 Diseño seguro

El modelo debe operar con un esquema de decisión cerrado:

```text
entrada textual
      │
      ▼
normalización + metadatos de origen
      │
      ▼
Llama 3 8B Instruct con prompt restringido
      │
      ▼
clasificación: permitir / revisar / bloquear
      │
      ├── permitir ──► continuar pipeline
      ├── revisar  ──► cola de aprobación humana
      └── bloquear ──► registrar incidente y detener registro
```

El prompt debe indicar que el modelo solo clasifique, no ejecute instrucciones encontradas en los datos. La salida debe limitarse a categorías y una justificación breve. Las decisiones críticas deben registrarse con hash del contenido, versión del modelo, versión del prompt, fecha y responsable de revisión.

### 10.4 Limitaciones

Un LLM puede cometer falsos positivos y falsos negativos. Por ello, el cortafuegos semántico debe complementarse con:

- Reglas deterministas y expresiones regulares.
- Listas de permitidos y bloqueados.
- Validación de esquemas.
- Contratos de datos.
- Revisión humana para casos de riesgo medio.
- Monitoreo de deriva y auditoría periódica.

No se debe usar el modelo para corregir automáticamente hechos numéricos o financieros sin una regla de negocio explícita y trazable.

## 11. Reproducibilidad y operación

### 11.1 Estructura de archivos

```text
.
├── data/
│   ├── scraped_data.json
│   ├── raw_data.csv
│   ├── csv_data.parquet
│   ├── json_data.parquet
│   └── dataset_final.parquet
├── src/
│   ├── scraper.py
│   ├── pipeline.py
│   └── visualizations.py
├── output/
│   ├── grafica_1.png
│   ├── grafica_2.png
│   └── grafica_metodos_pago.png
├── main.py
└── report_draft.md
```

### 11.2 Ejecución

```bash
python src/scraper.py
python src/pipeline.py
python src/visualizations.py
```

O, de forma orquestada:

```bash
python main.py
```

### 11.3 Criterios de aceptación

La actividad se considera completada cuando:

- El scraper produce JSON y CSV con `product_id` compatible.
- DuckDB infiere y muestra los esquemas de ambas fuentes.
- Se generan los Parquet intermedios.
- El JOIN se ejecuta como `LEFT JOIN`.
- La limpieza se realiza mediante SQL determinista.
- Los contratos de datos no presentan violaciones.
- Se exporta `data/dataset_final.parquet`.
- Se generan `output/grafica_1.png` y `output/grafica_2.png`.
- `main.py` finaliza con código de salida cero y verifica los entregables.

## 12. Conclusión

La solución demuestra una integración multifuente robusta y auditable. La separación entre ingesta, transformación, validación y visualización permite mantener trazabilidad y facilita la evolución del pipeline. El uso de DuckDB reduce la complejidad operativa al proporcionar lectura automática, procesamiento SQL y exportación Parquet en un único motor embebido.

El `LEFT JOIN` protege los hechos de ventas y hace explícita la cobertura del catálogo. La autocorrección controlada mejora la consistencia sin ocultar problemas críticos, mientras los contratos de datos actúan como puerta de salida. Finalmente, las visualizaciones convierten el dataset integrado en decisiones concretas sobre estacionalidad, composición comercial y rendimiento de productos.
