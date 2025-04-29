# README - Casos-de-Analisis-Resurtidor-Innovacion

---

# 🔹 Overview del Proyecto

Este proyecto tiene como objetivo realizar un análisis de inventario utilizando la arquitectura Medallion (Bronze, Silver) en Azure Databricks, partiendo de un archivo Excel suministrado por Walmart. El enfoque fue:

- Cargar datos en capa **Bronze** (sin transformaciones).
- Realizar limpieza y validaciones en capa **Silver**.
- Generar análisis estadísticos y visualizaciones para entender el desempeño de **In Stock** y **Dispersión**.

**Tecnologías utilizadas:**
- Microsoft Excel
- Azure Databricks
- PySpark
- Python (Matplotlib, Pandas)

---

# 💡 Caso 1: Preparación de datos y Análisis Exploratorio

## 1.1 Procesamiento Inicial en Excel
- Se abrió el archivo `Base_Prueba.xlsx`.
- Se identificó que el tab único relevante era `Base_prueba`.
- Se calcularon las columnas adicionales:
  - **In Stock**: `(Combinaciones - Oust) / Combinaciones`
  - **Dispersión**: `Tiendas sin Inventario / Tiendas con Inventario`
- Se agregaron fórmulas en Excel para todo el dataset.
- Se exportó únicamente la hoja `Base_prueba` como un nuevo archivo `.csv` llamado `base_prueba_bronze.csv`.

## 1.2 Creación de Infraestructura en Azure
- **Resource Group** creado: `rg-walmart-prueba-dev-eus2`
- **Workspace de Databricks** creado: `adb-walmart-prueba`
- **Cluster** creado: `cluster-walmart-prueba`
  - Tipo: Single Node
  - Runtime: 12.2 LTS (Scala 2.12, Spark 3.3.2)
  - Worker Type: Se intentó `Standard_DS4_v2`, pero debido a error de "stockout" en `eastus2`, se cambió exitosamente a `Standard_DS3_v2`.

## 1.3 Subida de datos a DBFS
- El archivo `base_prueba_bronze.csv` se subió exitosamente al path `/FileStore/tables/`.
- Se verificó que el archivo estaba disponible usando `dbutils.fs.ls("/FileStore/tables/")`.

## 1.4 Carga de Bronze Layer
- Se creó el notebook `nb-bronze-carga-base-prueba`.
- Se cargó el archivo `.csv` utilizando PySpark con:
  ```python
  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/base_prueba_bronze.csv")
  ```
- Se desplegó exitosamente `df_bronze`.

## 1.5 Limpieza de datos (Silver Layer)
- En el mismo notebook (de forma excepcional para esta prueba), se realizó la limpieza de datos:
  - Eliminación de registros con `Combinaciones <= 0` o nulos en `In Stock`, `Dispersión`, `Tiendas con Inventario` y `Tiendas sin Inventario`.
- Se creó un nuevo DataFrame `df_silver`:
  ```python
  df_silver = df_bronze.filter((col("Combinaciones") > 0) & (col("In Stock").isNotNull()) & (col("Dispersión").isNotNull()) & (col("Tiendas con Inventario").isNotNull()) & (col("Tiendas sin Inventario").isNotNull()))
  ```
- Se creó una vista temporal `silver_base_prueba`.

## 1.6 Análisis Estadístico
- Se realizaron resúmenes y estadísticas usando PySpark:
  - Estadísticas globales (`describe`) para `Combinaciones`, `Oust`, `In Stock` y `Dispersión`.
  - Promedio de `In Stock` y `Dispersión` por **País**.
  - Promedio de `In Stock` y `Dispersión` por **Formato**.
  - Top 10 Categorías con **peor In Stock**.

## 1.7 Visualizaciones
- Se crearon los siguientes gráficos:
  - Gráfico de barras: **In Stock Promedio por País**.
  - Gráfico de barras: **Dispersión Promedio por Formato**.
  - Gráfico de barras horizontal: **Top 10 Categorías con Peor In Stock**.

---

# 🚧 Problemas encontrados y soluciones aplicadas

| Problema | Solución Aplicada |
|:--|:--|
| Stockout de VM `Standard_DS4_v2` en EastUS2 | Cambiar a `Standard_DS3_v2` para cluster |
| No aparecía "Data" en Databricks para subir CSV | Se subió manualmente usando `Upload` en `/FileStore/tables/` |
| Intento de creación de tabla automática en Databricks | Se evitó, prefiriendo carga limpia por PySpark |
| Valores nulos encontrados en "Tiendas con Inventario" y "Tiendas sin Inventario" | Se filtraron en la limpieza de `df_silver` |

---

# 🔹 Caso 2: Análisis de In Stock

## Actividades realizadas
- Se analizó el comportamiento de In Stock a nivel global, país, formato y categoría.
- Se identificaron desviaciones respecto a la meta de 97.5%.
- Se construyeron gráficos de apoyo para visualización.
- Se elaboraron conclusiones y recomendaciones por nivel de análisis.

## Principales hallazgos
- El In Stock promedio global fue de 96.36%, por debajo de la meta.
- Guatemala lideró en In Stock, pero no cumplió meta.
- DISCOUNT fue el formato con mejor desempeño.
- Cosméticos (D59) fue la categoría más afectada.

## Recomendaciones
- Intervenir categoría de cosméticos.
- Replicar buenas prácticas de DISCOUNT.
- Implementar alertas automáticas para categorías con In Stock < 95%.

---

# 🔹 Caso 3: Análisis de Dispersión

## Actividades realizadas
- Se analizó la Dispersión global, por país y por formato.
- Se verificó el cumplimiento de la meta de 9%.
- Se elaboraron visualizaciones de Dispersión promedio.

## Principales hallazgos
- La Dispersión promedio global fue de 12.37%, superando la meta.
- Nicaragua fue el único país que cumplió la meta de Dispersión.
- HYPERMARKET presentó la mayor Dispersión entre los formatos.

## Recomendaciones
- Replicar las prácticas de Nicaragua y DISCOUNT.
- Rediseñar logística en HYPERMARKET y BODEGA.
- Establecer alertas de Dispersión >15% y auditar tiendas en Costa Rica.

---

# 🔹 Caso 4: Script SQL

Este caso se desarrollará durante la entrevista. Aún no ha sido abordado en esta fase.

---

# 🔹 Caso 5: Modelo de Almacenamiento de Datos

## Objetivo
Diseñar un modelo de almacenamiento robusto que permita:
- Construir un historial confiable de datos.
- Facilitar el análisis de tendencias y evolución en el tiempo.
- Evitar duplicidades.
- Permitir auditoría del origen y carga de los datos.

## Características clave
- Uso de `fecha_corte` y `fecha_carga` para diferenciar el momento del dato y su ingreso al sistema.
- Campo `source` para trazabilidad.
- Clave primaria compuesta para prevenir duplicados.
- ID técnico `id_inventario` para referencia interna.

## Estructura propuesta: `fact_inventario_historial`

| Campo                   | Tipo         | Descripción                                               |
|------------------------|--------------|-----------------------------------------------------------|
| `id_inventario`        | UUID / String| ID técnico del registro                                  |
| `pais`                 | String       | Código del país (ej.: CR, GT, HN)                         |
| `formato`              | String       | Tipo de tienda (DISCOUNT, BODEGA, etc.)                   |
| `categoría`            | String       | Categoría de producto                                     |
| `mes`                  | Integer      | Mes del dato (ej.: 1 a 12)                                |
| `combinaciones`        | Integer      | Total de combinaciones registradas                        |
| `oust`                 | Integer      | Total de quiebres (Oust)                                  |
| `tiendas_con_inventario` | Integer    | Tiendas que tenían el producto                            |
| `tiendas_sin_inventario` | Integer    | Tiendas que no tenían el producto                         |
| `in_stock`             | Decimal(5,4) | Porcentaje de In Stock                                    |
| `dispersión`           | Decimal(5,4) | Porcentaje de Dispersión                                  |
| `fecha_corte`          | Date         | Fecha a la que corresponde el dato                        |
| `fecha_carga`          | Timestamp    | Fecha en que se cargó el registro al sistema              |
| `source`               | String       | Fuente del dato (ej.: `upload_excel`, `pipeline_autom`)   |

## Diagrama del modelo

![Diagram_model](https://github.com/user-attachments/assets/dcd8f5d2-8cdd-4ddb-9715-2ee2bef09652)

## Beneficios del modelo
- Histórico limpio y auditable.
- Preparado para dashboards y tendencias.
- Flexible y extensible.

---

_Elaborado por: Mario Barboza_

_Proyecto: Prueba de Casos de Analisis Resurtidor Innovacion_

