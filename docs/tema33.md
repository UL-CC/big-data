# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.3. Procesamiento Escalable y Particionamiento

**Objetivo**:

Asegurar eficiencia y rendimiento en grandes volúmenes de datos mediante el diseño de flujos ETL que aprovechen técnicas de escalabilidad y particionamiento, optimizando recursos de cómputo y distribución de carga.

**Introducción**:

En contextos de Big Data, la capacidad de procesar grandes volúmenes de información de forma eficiente es crítica. A medida que los conjuntos de datos crecen, también lo hacen los desafíos asociados al rendimiento y al uso óptimo de los recursos. El diseño de procesos escalables, junto con una estrategia adecuada de particionamiento, es esencial para garantizar una ejecución rápida, segura y económica de los pipelines ETL.

**Desarrollo**:

Este tema aborda los fundamentos y técnicas para construir pipelines ETL escalables. Se analizarán los tipos de escalabilidad, técnicas específicas en Apache Spark como el particionamiento y tuning de parámetros, y se compararán enfoques de procesamiento batch y streaming. Además, se presentarán ejemplos prácticos que ilustran cómo controlar el volumen y balancear la carga computacional en entornos distribuidos.

### 3.3.1 Concepto de escalabilidad horizontal y vertical

La escalabilidad se refiere a la capacidad de un sistema para manejar un aumento de carga o datos sin comprometer el rendimiento.

##### Escalabilidad vertical (scale-up)

Consiste en mejorar un solo nodo, aumentando sus capacidades de hardware (más CPU, más RAM, almacenamiento más rápido).

1. Incrementar la memoria RAM en un nodo Spark para permitir cargas mayores en memoria.
2. Usar máquinas con discos SSD de alta velocidad para mejorar I/O en procesos locales.

##### Escalabilidad horizontal (scale-out)

Consiste en distribuir la carga entre múltiples nodos para procesamiento paralelo.

1. Añadir más nodos a un clúster Spark en Databricks para reducir el tiempo total de ejecución de un ETL masivo.
2. Usar auto-scaling en Amazon EMR para ajustar dinámicamente los recursos al volumen de datos procesado.

### 3.3.2 Técnicas de particionamiento en Spark

El particionamiento eficiente permite dividir los datos para procesarlos en paralelo, reduciendo tiempos de ejecución.

##### Técnicas clave

* **PartitionBy**: organiza los datos según columnas clave, útil al escribir DataFrames en formatos como Parquet.
* **Bucketing**: divide datos en buckets predefinidos; útil para joins.
* **Repartition**: redistribuye datos generando más particiones (shuffle costoso).
* **Coalesce**: reduce particiones sin movimiento de datos (optimización de salida).

**Guardar datos de ventas particionados por país y mes**, para mejorar la velocidad de consultas agregadas sobre ventas regionales.:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Partitioned Sales Data").getOrCreate()

# Simulación de datos de ventas
data = [("USA", "2025-06", 5000),
        ("Canada", "2025-06", 3500),
        ("USA", "2025-07", 7000),
        ("Mexico", "2025-07", 4200)]

columns = ["country", "month", "sales"]

df = spark.createDataFrame(data, columns)

# Guardar datos particionados por país y mes
df.write.partitionBy("country", "month").parquet("sales_partitioned")
```

**Aplicar bucketing sobre ID de usuario para facilitar joins**, para mejorar la eficiencia de joins entre registros de actividad y detalles de usuario:

```python
# Guardar datos con bucketing sobre ID de usuario
df.write.bucketBy(10, "user_id").saveAsTable("user_logs_bucketed")
```

Aquí, los datos de `user_logs_bucketed` se almacenarán en 10 buckets según `user_id`, lo que agiliza joins con otras tablas.

**Particionar datos de transacciones bancarias por año y tipo de transacción**, para permitir consultas eficientes sobre historiales de transacciones:

```python
df.write.partitionBy("year", "transaction_type").parquet("bank_transactions_partitioned")
```

**Aplicar bucketing sobre ID de producto para mejorar joins**, para optimizar combinaciones entre ventas y detalles de productos en grandes volúmenes de datos:

```python
df.write.bucketBy(20, "product_id").saveAsTable("products_bucketed")
```

##### Cuándo usar Repartition vs. Coalesce

**Usar `repartition(100)` antes de un join masivo entre DataFrames**, para distribuir uniformemente los datos antes de un join masivo, evitando particiones desbalanceadas:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Repartition Example").getOrCreate()

# Simulación de DataFrames
df1 = spark.range(100000).withColumnRenamed("id", "key")
df2 = spark.range(100000).withColumnRenamed("id", "key")

# Redistribuir los datos antes del join
df1 = df1.repartition(100, "key")
df2 = df2.repartition(100, "key")

# Realizar el join
df_joined = df1.join(df2, "key")
```

Esto equilibra el procesamiento en múltiples nodos antes del join, mejorando el rendimiento.

**Usar `coalesce(1)` para consolidar archivos en un pipeline ETL**, para reducir la cantidad de archivos generados al escribir resultados finales en almacenamiento:

```python
df_final = df_joined.coalesce(1)  # Reduce las particiones al mínimo
df_final.write.mode("overwrite").parquet("final_results.parquet")
```

Aquí, los datos se consolidan en un único archivo antes de la exportación, ideal para resultados de consumo humano o reportes.

**Usar `repartition(50)` antes de una agregación intensiva**, para garantizar una distribución uniforme de los datos para evitar que una sola partición haga todo el trabajo:

```python
from pyspark.sql.functions import sum

df_large = spark.range(1000000).withColumnRenamed("id", "transaction_id")

# Redistribuir antes de la agregación
df_large = df_large.repartition(50, "transaction_id")

# Calcular la suma de valores
df_aggregated = df_large.groupBy("transaction_id").agg(sum("transaction_id").alias("total"))
```

Repartir los datos equilibra la carga entre los workers de Spark, evitando problemas de desempeño.

**Usar `coalesce(5)` tras una transformación intensiva**, para optimizar la escritura sin alterar demasiado la distribución de datos:

```python
df_transformed = df_large.withColumn("transaction_type", df_large.transaction_id % 3)

# Reducir las particiones antes de escribir
df_transformed = df_transformed.coalesce(5)

df_transformed.write.mode("overwrite").parquet("optimized_transactions.parquet")
```

Esto minimiza la fragmentación en almacenamiento pero mantiene algo de paralelismo en la escritura.

Cada técnica tiene su aplicación específica, dependiendo de la necesidad de redistribuir o consolidar los datos.

### 3.3.3 Tuning de parámetros en Spark

La optimización de los parámetros de Spark es esencial para mejorar el rendimiento y la eficiencia de los trabajos distribuidos. Aquí te dejo una lista extendida de parámetros clave, junto con estrategias para su ajuste adecuado.

##### Parámetros clave

* `spark.executor.memory`: Define la cantidad de memoria asignada a cada executor en el cluster.
    - Si el trabajo involucra operaciones intensivas en memoria, como transformaciones con `groupBy` y `join`, aumenta el valor.
    - Evita asignar más memoria de la disponible en el nodo, ya que puede provocar *Out of Memory (OOM)*.
    - Usa `spark.memory.fraction` para controlar cuánto de esta memoria se usa para almacenamiento frente a ejecución.

* `spark.sql.shuffle.partitions`: Número de particiones creadas en operaciones de shuffle.
    - Aumenta si el dataset es grande y quieres reducir la carga en cada partición.
    - Para conjuntos de datos pequeños, reducir el número puede evitar sobrecarga de pequeños archivos (*tiny partitions*).
    - Generalmente, valores entre **200-400** funcionan bien para datos medianos.

* `spark.executor.instances`: Número de ejecutores disponibles.
    - Incrementarlo mejora la paralelización pero requiere suficiente recursos en el cluster.
    - Un buen punto de partida es asignar `(número de núcleos del cluster / núcleos por executor)`, asegurando que cada executor tenga al menos **1-2 GB de RAM por núcleo**.

* `spark.default.parallelism`: Número de tareas paralelas por defecto.
    - Ajusta a `2 * número de núcleos totales` para un paralelismo eficiente.
    - Si los datos son pequeños, reducir este valor puede evitar sobreasignación innecesaria de tareas.

* `spark.executor.cores`: Número de núcleos asignados a cada executor.
    - Mayor número de cores aumenta la concurrencia, pero puede sobrecargar la memoria si no se gestiona bien.
    - Generalmente **4-8** cores por executor funciona bien en clusters grandes.

* `spark.memory.fraction`: Fracción de memoria de ejecución disponible para almacenamiento de datos internos.
    - Si los trabajos requieren muchas operaciones en memoria, aumentar este valor puede mejorar la eficiencia.
    - Mantener un equilibrio con `spark.memory.storageFraction` para evitar presión sobre la memoria disponible.

* `spark.sql.autoBroadcastJoinThreshold`: Tamaño máximo de datos para que una tabla se transmita (*broadcast*) en joins.
    - Si se hacen muchos joins en datasets pequeños, aumentar el valor permite optimizar los *broadcast joins*.
    - Si los datasets son grandes, desactivar *broadcast* puede evitar consumo excesivo de memoria.

* `spark.task.cpus`: Número de núcleos usados por cada tarea dentro de un executor.
    - Para cargas de trabajo CPU-intensivas, asignar más núcleos puede mejorar el rendimiento.
    - Para tareas I/O-intensivas, mantener un valor bajo es mejor para paralelismo.

* `spark.dynamicAllocation.enabled`: Permite que Spark ajuste dinámicamente el número de ejecutores en función de la carga de trabajo.
    - Habilitar en entornos con variabilidad en cargas de trabajo mejora la eficiencia.
    - Desactivarlo en clusters con recursos fijos evita fluctuaciones inesperadas.

##### Estrategia General de Tuning

1. **Analizar la carga de trabajo**: Si el trabajo es CPU-intensivo, aumentar núcleos; si es memoria-intensivo, ajustar `spark.executor.memory`.
2. **Optimizar particiones**: Reducir el número si hay demasiados archivos pequeños, aumentarlo si las tareas tardan demasiado.
3. **Monitorear métricas en Spark UI**: Identificar cuellos de botella con *storage*, *execution* y *shuffle*.
4. **Iterar pruebas**: No hay valores absolutos, cada sistema tiene su configuración óptima según los datos y recursos disponibles.


### 3.3.4 Comparación de procesamiento batch vs. streaming (ej. Spark Structured Streaming)

##### Procesamiento por lotes (batch)

El procesamiento por lotes es un paradigma que maneja grandes volúmenes de datos almacenados de forma estática, ejecutándose en intervalos predefinidos como horarios, diarios, semanales o mensuales. Este enfoque es ideal cuando la latencia no es crítica y se necesita procesar datos históricos completos para obtener insights profundos. Spark excede en este tipo de procesamiento gracias a su capacidad de distribución masiva y optimizaciones como Catalyst Optimizer y Tungsten.

Las características principales incluyen alta throughput, procesamiento de datasets completos, tolerancia a fallos robusta mediante lineage de RDDs, y la capacidad de manejar transformaciones complejas que requieren múltiples pasadas sobre los datos. El procesamiento batch permite optimizaciones avanzadas como particionado inteligente, caching estratégico y ejecución lazy evaluation.

1. **ETL nocturno que consolida logs de ventas y los carga al DWH** - Transformación y limpieza de millones de registros de transacciones diarias para análisis de negocio.

2. **Agrupamiento semanal de datos para generación de KPIs** - Cálculo de métricas de rendimiento empresarial agregando datos de múltiples fuentes.

3. **Procesamiento de datos de machine learning para entrenamiento de modelos** - Preparación de datasets masivos con feature engineering, normalización y particionado para algoritmos de ML distribuidos usando MLlib.

4. **Análisis forense de logs de seguridad** - Procesamiento retrospectivo de terabytes de logs de red para identificar patrones de ataques, correlacionar eventos sospechosos y generar reportes de amenazas.

5. **Migración y reconciliación de bases de datos legacy** - Transformación masiva de esquemas antiguos, validación de integridad referencial y carga incremental hacia arquitecturas modernas de data lakes.

##### Procesamiento en tiempo real (streaming)

El procesamiento en streaming maneja flujos continuos de datos conforme van llegando, proporcionando capacidades de análisis y respuesta con latencia ultra-baja (milisegundos a segundos). Spark Structured Streaming ofrece un modelo unificado que trata los streams como tablas infinitas, permitiendo usar la misma API de SQL y DataFrame tanto para batch como streaming, garantizando exactly-once processing y manejo automático de late data.

Este paradigma es fundamental para casos de uso que requieren decisiones inmediatas, monitoreo en tiempo real, alertas proactivas y experiencias interactivas. Spark Structured Streaming proporciona checkpointing automático, recuperación de fallos sin pérdida de datos, y la capacidad de manejar múltiples fuentes de datos simultáneamente (Kafka, Kinesis, TCP sockets, etc.).

1. **Detección en tiempo real de fraudes bancarios con Spark Structured Streaming** - Análisis instantáneo de patrones transaccionales para bloquear operaciones sospechosas antes de su ejecución.

2. **Pipeline de logs de sensores IoT para monitoreo ambiental** - Procesamiento continuo de métricas de temperatura, humedad y calidad del aire para alertas automáticas.

3. **Análisis de clickstream en plataformas de e-commerce** - Seguimiento en tiempo real del comportamiento de usuarios para personalización dinámica, recomendaciones instantáneas y optimización de conversion rates.

4. **Monitoreo de infraestructura y DevOps** - Procesamiento continuo de métricas de servidores, aplicaciones y redes para detección proactiva de anomalías, auto-scaling y resolución automática de incidentes.

5. **Trading algorítmico y análisis financiero** - Procesamiento de feeds de mercados financieros en tiempo real para ejecutar estrategias de trading, calcular riesgos dinámicamente y generar alertas de volatilidad extrema.

### 3.3.5 Ejemplos de control de volumen y distribución de carga

El diseño efectivo de pipelines en Spark requiere estrategias sofisticadas para distribuir la carga de trabajo de manera óptima y evitar cuellos de botella que pueden degradar significativamente el rendimiento. La gestión adecuada del volumen de datos y su distribución a través del cluster es fundamental para maximizar el paralelismo, minimizar el shuffle de datos y optimizar el uso de recursos computacionales. Esto incluye técnicas de pre-procesamiento, particionado inteligente, y balanceamiento dinámico de workloads.

##### Técnicas de control de volumen

Las técnicas de control de volumen se enfocan en reducir la cantidad de datos que deben procesarse en las etapas más costosas del pipeline, aplicando principios de optimización temprana que pueden resultar en mejoras de rendimiento de órdenes de magnitud.

1. **Filtrado temprano de datos irrelevantes antes de operaciones costosas** - Aplicar `filter` antes de `join` para reducir drasticamente el volumen de datos en operaciones de shuffle intensivo, minimizando el tráfico de red y uso de memoria.

2. **Muestreo (`sample`) para pruebas y validaciones antes del procesamiento completo** - Utilizar subconjuntos representativos para validar lógica de negocio, ajustar parámetros de configuración y estimar recursos necesarios.

3. **Pushdown de predicados y proyecciones** - Aprovechar optimizaciones de Catalyst Optimizer para ejecutar filtros y selecciones directamente en el storage layer (Parquet, Delta Lake), reduciendo I/O y transferencia de datos.

4. **Compresión y codificación adaptativa** - Implementar esquemas de compresión específicos por tipo de dato (dictionary encoding para strings, delta encoding para timestamps) para minimizar footprint de memoria y acelerar serialización.

5. **Lazy evaluation estratégica** - Diseñar transformaciones que aprovechan la evaluación perezosa de Spark para combinar múltiples operaciones en stages optimizados, reduciendo materializaciones intermedias innecesarias.

##### Estrategias de distribución de carga

La distribución eficiente de carga requiere un entendimiento profundo de los patrones de acceso a datos, características de las claves de particionado, y topología del cluster para evitar data skew y hotspots de procesamiento.

1. **Pre-particionar archivos de entrada en HDFS/Parquet para alinear con claves de join** - Organizar datos físicamente según las claves más frecuentemente utilizadas en joins, eliminando shuffle operations y mejorando locality.

2. **Balancear workloads entre particiones usando claves con distribución uniforme** - Evitar data skew seleccionando claves de particionado con cardinalidad alta y distribución equilibrada, implementando técnicas como salting o bucketing.

3. **Dynamic partition pruning y predicate pushdown** - Configurar Spark para eliminar dinámicamente particiones irrelevantes durante la ejecución, reduciendo significativamente el conjunto de datos a procesar.

4. **Coalescing y repartitioning inteligente** - Aplicar `coalesce()` para consolidar particiones pequeñas sin shuffle, y `repartition()` para redistribuir datos cuando se requiere cambiar el esquema de particionado.

5. **Broadcasting de datasets pequeños** - Utilizar broadcast joins para tablas de dimensiones pequeñas (<200MB), eliminando shuffle operations y mejorando dramatically el rendimiento de joins.

##### Casos reales

1. **Pipeline de ingestión de logs web optimizado** - En un sistema de análisis de logs web procesando 50TB diarios, se aplicó `filter` temprano para eliminar bots y requests irrelevantes, seguido de `repartition` por tipo de evento y timestamp, resultando en 70% reducción en tiempo de procesamiento.

2. **ETL de transacciones financieras acelerado** - Un ETL procesando millones de registros de transacciones fue optimizado dividiendo los datos por región geográfica y ejecutando pipelines paralelos, implementando particionado por hash de customer_id para evitar skew, logrando 5x mejora en throughput.

3. **Análisis de sensores IoT con particionado temporal** - Sistema de procesamiento de datos de sensores industriales implementó particionado por device_id y timestamp, con pre-agregación por ventanas de tiempo, utilizando Delta Lake para optimizar upserts y reducir small files problem.

4. **Data warehouse de e-commerce con bucketing estratégico** - Pipeline ETL para plataforma de comercio electrónico implementó bucketing por customer_id en tablas de facts, combinado con Z-ordering en product_id, resultando en 80% reducción en tiempo de queries analíticas y mejora significativa en concurrent user experience.

5. **Streaming analytics con watermarking avanzado** - Sistema de análisis en tiempo real de eventos de aplicaciones móviles implementó watermarking dinámico y particionado por session_id con salting, manejando efectivamente late-arriving data y achieving sub-second latency para alertas críticas de negocio.

## Tarea

Desarrolla los siguientes ejercicios para aplicar los conocimientos adquiridos:

1. Implementa un pipeline en PySpark que lea datos de transacciones, los particione por país y mes, y los escriba en formato Parquet.
2. Ajusta parámetros de Spark (`executor memory`, `shuffle partitions`) para mejorar el rendimiento de un ETL sobre 1 millón de registros con múltiples joins.
3. Compara dos pipelines: uno en batch y otro en streaming usando Spark Structured Streaming, que consuman datos simulados de sensores de temperatura.
4. Diseña una estrategia de particionamiento y bucketing para un sistema de recomendaciones que une ratings de usuarios y metadatos de películas.
5. Realiza pruebas de escalabilidad horizontal en un entorno local o en la nube (por ejemplo, Databricks Community Edition) incrementando el número de nodos y midiendo el tiempo de ejecución de un pipeline.

