# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.7. Patrones de Diseño y Optimización en la Nube

**Objetivo**:

Consolidar las buenas prácticas de diseño y optimización para el desarrollo de soluciones de Big Data en entornos de nube, enfocándose en la eficiencia operativa, la robustez del sistema y el control de costos.

**Introducción**:

El diseño y la implementación de soluciones de Big Data en la nube presentan oportunidades sin precedentes para la escalabilidad, flexibilidad y velocidad de procesamiento. Sin embargo, para capitalizar estas ventajas y evitar trampas comunes como los costos descontrolados o la ineficiencia operativa, es crucial comprender y aplicar patrones de diseño y estrategias de optimización probadas. Este tema profundiza en las mejores prácticas para la extracción, transformación, carga (ETL), el control y monitoreo de datos, así como en patrones arquitectónicos y técnicas específicas para minimizar los costos en infraestructuras cloud.

**Desarrollo**:

La adopción de patrones de diseño en el ámbito de Big Data y la nube no solo estandariza la forma en que se construyen los pipelines de datos, sino que también fomenta la reutilización, mejora la mantenibilidad y asegura la resiliencia de los sistemas. Al integrar estas prácticas desde la fase de diseño, se pueden anticipar y mitigar problemas como la inconsistencia de datos, los cuellos de botella en el rendimiento y los gastos excesivos. Además, una comprensión profunda de las estrategias de optimización de costos en la nube es vital para garantizar la viabilidad a largo plazo de las soluciones implementadas, permitiendo a las organizaciones escalar sus operaciones de datos de manera sostenible.

### 3.7.1. Patrones de Extracción

La extracción de datos es la primera fase crítica en cualquier pipeline ETL, donde la eficiencia y la selección adecuada del patrón pueden impactar significativamente el rendimiento y los costos. Estos patrones se enfocan en cómo los datos son leídos desde las fuentes de origen.

##### Extracción Incremental

La extracción incremental es una técnica fundamental que busca reducir la carga de procesamiento y el volumen de datos transferidos al obtener únicamente los registros que han sido modificados o añadidos desde la última ejecución del proceso de extracción. Esto se logra típicamente mediante el uso de marcas de tiempo (`timestamps`), números de secuencia o banderas de modificación (`flags de modificación`) en las tablas de origen. Su implementación es vital en entornos de Big Data para manejar grandes volúmenes de información que cambian constantemente, ya que evita el reprocesamiento innecesario de datos ya existentes y estables.

Imaginemos una base de datos transaccional de ventas que se actualiza continuamente. En lugar de extraer toda la tabla de ventas cada hora, podemos extraer solo las transacciones que ocurrieron desde la última extracción.

```sql
-- Último timestamp de extracción guardado en una tabla de metadatos
SET @last_extracted_timestamp = (SELECT max_timestamp FROM etl_metadata WHERE job_name = 'sales_extraction');

-- Extracción incremental de nuevas ventas
SELECT *
FROM sales_transactions
WHERE last_updated_at > @last_extracted_timestamp;

-- Actualizar el timestamp de la última extracción después de una extracción exitosa
UPDATE etl_metadata
SET max_timestamp = CURRENT_TIMESTAMP
WHERE job_name = 'sales_extraction';
```

##### Extracción por Lotes vs Streaming

La elección entre extracción por lotes y streaming depende directamente de los requisitos de latencia y el volumen de datos.

* **Extracción por Lotes (Batch Processing)**: Procesa grandes volúmenes de datos en intervalos programados (por ejemplo, cada hora, diariamente, semanalmente). Es ideal para datos que no requieren inmediatez, como informes analíticos históricos o cargas de data warehouses nocturnas. Este enfoque es generalmente más eficiente en términos de recursos para grandes volúmenes de datos estáticos o semi-estáticos.
* **Extracción por Streaming (Stream Processing)**: Procesa datos en tiempo real conforme llegan a la fuente. Es crucial para aplicaciones que requieren baja latencia, como detección de fraude, monitoreo de sistemas, personalización en tiempo real o IoT. Este patrón implica el uso de tecnologías que puedan manejar flujos continuos de datos, como Apache Kafka o Amazon Kinesis.

Para la generación de un informe de ventas mensual consolidado, la extracción por lotes es adecuada. Para monitorear transacciones bancarias en busca de actividad fraudulenta, el streaming es indispensable.

```python
# Productor de Kafka (simula un sistema de punto de venta enviando transacciones)
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    transaction = {'id': i, 'amount': i * 10.5, 'timestamp': time.time()}
    producer.send('transactions_topic', transaction)
    print(f"Sent: {transaction}")
    time.sleep(0.1) # Simula llegada de transacciones en tiempo real

# Consumidor de Kafka (simula un procesador de fraude)
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('transactions_topic',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='fraud_detector_group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    transaction = message.value
    if transaction['amount'] > 500:
        print(f"ALERTA DE FRAUDE POTENCIAL: {transaction}")
    else:
        print(f"Procesando transacción normal: {transaction}")
```

##### Change Data Capture (CDC)

Change Data Capture (CDC) es una técnica que identifica y captura los cambios (inserciones, actualizaciones, eliminaciones) realizados en una base de datos fuente. A diferencia de la extracción incremental basada en `timestamps`, CDC opera a un nivel más granular, típicamente leyendo los logs de transacciones de la base de datos (como el binlog de MySQL o el WAL de PostgreSQL) o utilizando triggers. Esto permite replicar los cambios de forma asíncrona y con una latencia mínima, sin imponer una carga significativa en el sistema operacional de origen. Es ideal para mantener réplicas de bases de datos, alimentar data warehouses en tiempo real o sincronizar sistemas.

Mantener un data lake o data warehouse actualizado con los últimos movimientos de una base de datos OLTP sin afectar su rendimiento. Debezium es un popular framework de CDC.

```yaml
# Configuración de un conector Debezium para MySQL en Kafka Connect
# Este es un ejemplo de cómo se configura en un archivo properties/JSON
name: mysql-connector
connector.class: io.debezium.connector.mysql.MySqlConnector
database.hostname: 192.168.99.100
database.port: 3306
database.user: debezium
database.password: dbz
database.server.id: 12345
database.server.name: my-app-connector
database.whitelist: inventory,customers
database.history.kafka.bootstrap.servers: kafka:9092
database.history.kafka.topic: dbhistory.inventory

# Una vez configurado, Debezium publicará los cambios en Kafka topics
# Por ejemplo, para la tabla 'customers', los cambios irán a 'my-app-connector.inventory.customers'
```

Posteriormente, un consumidor de Kafka (como un procesador Spark Streaming) puede leer estos cambios y aplicarlos a un destino:

```python
# Pseudo-código para un consumidor Spark Streaming leyendo cambios de CDC
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType, LongType, IntegerType

spark = SparkSession.builder.appName("CDCProcessor").getOrCreate()

# Esquema para los mensajes de Debezium (simplificado)
# Un mensaje de Debezium típico incluye 'before' y 'after' estados del registro, y el tipo de operación 'op'
schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("op", StringType(), True) # 'c' for create, 'u' for update, 'd' for delete
])

# Leer el stream de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-app-connector.inventory.customers") \
    .load()

# Parsear el valor JSON del mensaje de Kafka
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Aplicar lógica basada en el tipo de operación
query = parsed_df \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: process_cdc_batch(df)) \
    .start()

def process_cdc_batch(df):
    # Lógica para aplicar los cambios a un Data Warehouse o Data Lake
    df_inserts = df.filter(col("op") == "c").select(col("after.*"))
    df_updates = df.filter(col("op") == "u").select(col("after.*"))
    df_deletes = df.filter(col("op") == "d").select(col("before.*"))

    # Aquí iría la lógica para escribir en el destino, por ejemplo, en una tabla Delta Lake
    # df_inserts.write.format("delta").mode("append").save("/delta/customers")
    # df_updates.write.format("delta").mode("merge").option("mergeSchema", "true").save("/delta/customers") # Asumiendo Merge Into
    # df_deletes.write.format("delta").mode("merge").where("id = <deleted_id>").delete("/delta/customers") # Pseudo-código para delete

    print(f"Processed batch {epoch_id}: Inserts={df_inserts.count()}, Updates={df_updates.count()}, Deletes={df_deletes.count()}")


query.awaitTermination()
```

### 3.7.2. Patrones de Transformación

La fase de transformación es donde los datos brutos se limpian, enriquecen, validan y se preparan para su consumo final. Los patrones aquí se centran en la eficiencia y la modularidad del procesamiento.

##### Pipeline de Transformación

Un pipeline de transformación organiza las operaciones de procesamiento de datos en una secuencia lógica de pasos, donde la salida de una etapa se convierte en la entrada de la siguiente. Esta modularidad facilita el mantenimiento, la depuración y la reutilización de componentes. Cada paso del pipeline puede ser una función o un microservicio independiente que realiza una tarea específica, como la limpieza de datos nulos, la normalización de formatos o la agregación de información. Este patrón es fundamental para construir flujos de trabajo ETL complejos y robustos.

Un pipeline que primero limpia los datos de clientes (elimina duplicados, corrige errores de formato), luego los enriquece con información geográfica y finalmente los agrega por región.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, count

spark = SparkSession.builder.appName("DataTransformationPipeline").getOrCreate()

# Paso 1: Cargar datos brutos (simulamos un DataFrame)
raw_data = [
    ("john doe", "john.doe@example.com ", "New York", 1),
    ("Jane Smith", "jane.smith@example.com", "Los Angeles ", 2),
    (" JOHN DOE ", "john.doe@example.com", "new york", 3), # Duplicado, nombre en mayúsculas
    ("Peter Jones", "peter.jones@example.com", "Chicago", 4)
]
raw_df = spark.createDataFrame(raw_data, ["name", "email", "city", "id"])

# Paso 2: Limpieza de datos
def clean_data(df):
    return df.withColumn("name", trim(upper(col("name")))) \
             .withColumn("city", trim(col("city"))) \
             .dropDuplicates(["email"])

cleaned_df = clean_data(raw_df)
cleaned_df.show()
# +----------+--------------------+----------+---+
# |      name|               email|      city| id|
# +----------+--------------------+----------+---+
# | PETER JONES|peter.jones@example.com|   Chicago|  4|
# | JOHN DOE|john.doe@example.com| New York|  1|
# |JANE SMITH|jane.smith@example.com|Los Angeles|  2|
# +----------+--------------------+----------+---+

# Paso 3: Enriquecimiento (simulamos la adición de una columna "region")
def enrich_data(df):
    # En un caso real, esto podría ser una búsqueda en una tabla de mapeo de ciudades a regiones
    return df.withColumn("region",
                         when(col("city") == "New York", "East")
                         .when(col("city") == "Los Angeles", "West")
                         .otherwise("Central"))

enriched_df = enrich_data(cleaned_df)
enriched_df.show()
# +----------+--------------------+----------+---+-------+
# |      name|               email|      city| id| region|
# +----------+--------------------+----------+---+-------+
# | PETER JONES|peter.jones@example.com|   Chicago|  4|Central|
# | JOHN DOE|john.doe@example.com| New York|  1|   East|
# |JANE SMITH|jane.smith@example.com|Los Angeles|  2|   West|
# +----------+--------------------+----------+---+-------+

# Paso 4: Agregación
def aggregate_data(df):
    return df.groupBy("region").agg(count("*").alias("customer_count"))

aggregated_df = aggregate_data(enriched_df)
aggregated_df.show()
# +-------+--------------+
# | region|customer_count|
# +-------+--------------+
# |Central|             1|
# |   East|             1|
# |   West|             1|
# +-------+--------------+

spark.stop()
```

##### Transformación en Paralelo

La transformación en paralelo es una técnica que divide un gran conjunto de datos en subconjuntos más pequeños (chunks) y procesa estos subconjuntos simultáneamente utilizando múltiples hilos, procesos o nodos de un clúster distribuido. Este patrón es fundamental en entornos de Big Data para aprovechar al máximo los recursos disponibles, ya sea en un solo servidor con múltiples núcleos o en un clúster distribuido (como Spark o Hadoop). Al procesar datos en paralelo, se reduce significativamente el tiempo total de ejecución y se mejora la escalabilidad del pipeline de transformación.

Procesar millones de registros de logs para extraer información relevante, donde cada archivo de log o un bloque de registros puede ser procesado independientemente.

**Con código (ejemplo conceptual con Dask, una librería Python para computación paralela)**:

```python
import dask.dataframe as dd
import pandas as pd

# Crear un DataFrame de Pandas grande
data = {'value': range(100_000_000)}
df = pd.DataFrame(data)

# Convertir a Dask DataFrame (lo divide en particiones automáticamente)
ddf = dd.from_pandas(df, npartitions=8) # Procesará en 8 particiones/tareas

# Definir una función de transformación (ejemplo: elevar al cuadrado)
def square_value(x):
    return x * x

# Aplicar la transformación en paralelo
result_ddf = ddf['value'].apply(square_value, meta=('value', 'int64'))

# Computar el resultado (esto dispara la ejecución paralela)
# Utiliza un scheduler local por defecto, que usa múltiples núcleos
final_result = result_ddf.compute()

print(f"Processed {len(final_result)} records.")
print(f"First 5 results: {final_result.head()}")

# Otro ejemplo con PySpark (Spark ya es distribuido por defecto)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ParallelTransformation").getOrCreate()

# Cargar un dataset grande (ejemplo: CSV de 100 millones de registros)
# spark.read.csv("s3://your-bucket/large_dataset.csv", header=True, inferSchema=True).repartition(100)
# Para este ejemplo, crearemos un DataFrame grande en memoria
data = [(i,) for i in range(10_000_000)]
df = spark.createDataFrame(data, ["value"])

# La transformación se aplica automáticamente en paralelo por Spark
transformed_df = df.withColumn("squared_value", col("value") * col("value"))

# Mostrar algunos resultados (esto activa la computación distribuida)
transformed_df.show(5)
# +--------+-------------+
# |   value|squared_value|
# +--------+-------------+
# |       0|            0|
# |       1|            1|
# |       2|            4|
# |       3|            9|
# |       4|           16|
# +--------+-------------+

# Escribir el resultado a un destino distribuido (ej. Parquet)
# transformed_df.write.mode("overwrite").parquet("s3://your-bucket/output/squared_values.parquet")

spark.stop()
```

##### Lookup y Enriquecimiento

Este patrón implica combinar datos de una fuente principal con información adicional de una o varias fuentes secundarias para enriquecer los registros. El enriquecimiento puede lograrse mediante operaciones de `join` en bases de datos relacionales, búsquedas en tablas de referencia (dimensiones en un data warehouse), o llamadas a APIs externas para obtener información en tiempo real. Es crucial para añadir contexto y valor a los datos brutos, permitiendo análisis más profundos.

Enriquecer registros de transacciones de ventas con detalles del producto (nombre, categoría, precio unitario) y del cliente (datos demográficos, historial de compras) que se encuentran en tablas separadas.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("LookupAndEnrichment").getOrCreate()

# Datos de transacciones (Hechos)
transactions_data = [
    (1, "prodA", 100, 50.0),
    (2, "prodB", 101, 75.0),
    (3, "prodA", 102, 50.0),
    (4, "prodC", 100, 120.0)
]
transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "product_id", "customer_id", "amount"])

# Datos de productos (Dimensión)
products_data = [
    ("prodA", "Laptop", "Electronics"),
    ("prodB", "Mouse", "Electronics"),
    ("prodC", "Keyboard", "Peripherals")
]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category"])

# Datos de clientes (Dimensión)
customers_data = [
    (100, "Alice", "USA"),
    (101, "Bob", "Canada"),
    (102, "Charlie", "Mexico")
]
customers_df = spark.createDataFrame(customers_data, ["customer_id", "customer_name", "country"])

# Enriquecimiento de transacciones con datos de productos
enriched_transactions_df = transactions_df.join(products_df, "product_id", "inner")
enriched_transactions_df.show()
# +----------+--------------+-----------+------+------------+-----------+
# |product_id|transaction_id|customer_id|amount|product_name|   category|
# +----------+--------------+-----------+------+------------+-----------+
# |     prodA|             1|        100|  50.0|      Laptop|Electronics|
# |     prodA|             3|        102|  50.0|      Laptop|Electronics|
# |     prodB|             2|        101|  75.0|       Mouse|Electronics|
# |     prodC|             4|        100| 120.0|    Keyboard|Peripherals|
# +----------+--------------+-----------+------+------------+-----------+

# Enriquecimiento adicional con datos de clientes
final_enriched_df = enriched_transactions_df.join(customers_df, "customer_id", "inner")
final_enriched_df.show()
# +----------+--------------+-----------+------+------------+-----------+-------------+-------+
# |product_id|transaction_id|customer_id|amount|product_name|   category|customer_name|country|
# +----------+--------------+-----------+------+------------+-----------+-------------+-------+
# |     prodA|             1|        100|  50.0|      Laptop|Electronics|        Alice|    USA|
# |     prodC|             4|        100| 120.0|    Keyboard|Peripherals|        Alice|    USA|
# |     prodB|             2|        101|  75.0|       Mouse|Electronics|          Bob| Canada|
# |     prodA|             3|        102|  50.0|      Laptop|Electronics|      Charlie| Mexico|
# +----------+--------------+-----------+------+------------+-----------+-------------+-------+

spark.stop()
```

### 3.7.3. Patrones de Carga

La fase de carga es la etapa final del proceso ETL, donde los datos transformados se mueven al destino final (data warehouse, data lake, base de datos analítica). La eficiencia y la estrategia de carga son cruciales para el rendimiento y la integridad de los datos en el destino.

##### Upsert (Insert/Update)

El patrón Upsert es una operación que intenta insertar un registro en una tabla; si el registro ya existe (basado en una clave única), se actualiza en lugar de insertarse un nuevo registro. Esto es particularmente útil en escenarios donde los datos de origen pueden contener actualizaciones para registros ya existentes o nuevos registros que se añaden con el tiempo. El Upsert garantiza que el destino refleje el estado más actual de los datos sin crear duplicados ni requerir lógica de `DELETE` e `INSERT` separadas.

Mantener una tabla de clientes o productos donde se reciben actualizaciones esporádicas y también nuevos registros.

Delta Lake es un formato de tabla de almacenamiento de código abierto que soporta operaciones `MERGE INTO` (Upsert).

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("UpsertPattern").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").getOrCreate()

# Ruta para la tabla Delta
delta_table_path = "/tmp/delta/customer_profiles"

# Crear una tabla Delta inicial (si no existe)
try:
    spark.read.format("delta").load(delta_table_path).show()
except Exception:
    initial_data = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com")
    ]
    initial_df = spark.createDataFrame(initial_data, ["id", "name", "email"])
    initial_df.write.format("delta").mode("overwrite").save(delta_table_path)
    print("Initial Delta table created.")

# Cargar la tabla Delta existente
customer_profiles_df = spark.read.format("delta").load(delta_table_path)
print("Current customer profiles:")
customer_profiles_df.show()

# Nuevos datos a upsert: un nuevo registro (id 3) y una actualización (id 1)
new_data = [
    (1, "Alice Smith", "alice.smith@example.com"), # Update
    (3, "Charlie", "charlie@example.com")         # New
]
updates_df = spark.createDataFrame(new_data, ["id", "name", "email"])
print("Updates to apply:")
updates_df.show()

# Realizar la operación MERGE INTO (Upsert)
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, delta_table_path)

deltaTable.alias("target") \
  .merge(
    updates_df.alias("source"),
    "target.id = source.id"
  ) \
  .whenMatchedUpdate(set = { "name" : col("source.name"), "email" : col("source.email") }) \
  .whenNotMatchedInsert(values = { "id" : col("source.id"), "name" : col("source.name"), "email" : col("source.email") }) \
  .execute()

print("Customer profiles after upsert:")
spark.read.format("delta").load(delta_table_path).show()

spark.stop()
```

##### Slowly Changing Dimensions (SCD)

Las Dimensiones de Cambio Lento (SCD) son un concepto crucial en el modelado de data warehouses para manejar cambios en los datos de dimensiones a lo largo del tiempo. A diferencia de las tablas de hechos que registran eventos transaccionales, las tablas de dimensiones describen entidades (como clientes, productos, ubicaciones) que pueden cambiar sus atributos. Existen varios tipos de SCD:

* **SCD Tipo 1 (Sobreescritura)**: El valor anterior se sobrescribe con el nuevo valor. Esto es simple de implementar, pero no conserva el historial de cambios. Es adecuado para correcciones o cuando el historial no es relevante.
* **SCD Tipo 2 (Historial Completo)**: Se crea una nueva fila en la tabla de dimensiones para cada cambio en un atributo clave. La fila anterior se "cierra" (por ejemplo, con una fecha de fin o una bandera de activo/inactivo), y la nueva fila se "abre". Esto permite analizar los datos de hechos con el estado de la dimensión en un punto específico del tiempo. Es el tipo más común para la analítica histórica.
* **SCD Tipo 3 (Historial Limitado)**: Se añade una nueva columna a la tabla de dimensiones para almacenar el valor anterior de un atributo específico. Solo mantiene un historial limitado (típicamente el valor actual y el valor anterior). Es útil cuando solo se necesita rastrear un cambio reciente y no un historial completo.

El cambio de dirección de un cliente en una empresa; con SCD Tipo 2, se podría rastrear dónde vivía el cliente en cada momento.

Para SCD Tipo 2 en un data warehouse, supongamos que tenemos una tabla `dim_customers` y un nuevo lote de datos de clientes (`staging_customers`).

```sql
-- Estructura de la tabla dim_customers para SCD Tipo 2
CREATE TABLE dim_customers (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255),
    address VARCHAR(255),
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- Suponiendo que staging_customers tiene los nuevos datos (ej. id, name, address)
-- Lógica para insertar nuevos clientes o actualizar existentes (SCD Tipo 2)
INSERT INTO dim_customers (customer_id, customer_name, address, start_date, end_date, is_current)
SELECT
    s.customer_id,
    s.customer_name,
    s.address,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM staging_customers s
LEFT JOIN dim_customers d ON s.customer_id = d.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL; -- Solo insertar clientes nuevos

-- Actualizar registros existentes (cerrar la versión anterior y abrir una nueva)
MERGE INTO dim_customers AS target
USING (
    SELECT
        s.customer_id,
        s.customer_name,
        s.address,
        CURRENT_DATE AS new_start_date
    FROM staging_customers s
    JOIN dim_customers d ON s.customer_id = d.customer_id AND d.is_current = TRUE
    WHERE s.customer_name <> d.customer_name OR s.address <> d.address -- Solo si hay cambios en atributos relevantes
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET target.end_date = DATEADD(day, -1, source.new_start_date),
               target.is_current = FALSE;

-- Insertar las nuevas versiones de los registros actualizados
INSERT INTO dim_customers (customer_id, customer_name, address, start_date, end_date, is_current)
SELECT
    s.customer_id,
    s.customer_name,
    s.address,
    CURRENT_DATE,
    '9999-12-31',
    TRUE
FROM staging_customers s
JOIN dim_customers d ON s.customer_id = d.customer_id AND d.is_current = FALSE AND d.end_date = DATEADD(day, -1, CURRENT_DATE)
WHERE s.customer_name <> d.customer_name OR s.address <> d.address;
```

##### Bulk Loading

El Bulk Loading, o carga masiva, es una técnica optimizada para cargar grandes volúmenes de datos en una base de datos o sistema de almacenamiento. En lugar de procesar cada registro individualmente con sentencias `INSERT` una por una (lo cual es ineficiente para grandes conjuntos de datos), las operaciones de bulk loading agrupan los datos y los cargan de manera más eficiente, minimizando la sobrecarga transaccional. Esto a menudo implica deshabilitar temporalmente índices, restricciones (`constraints`) y triggers durante la carga, y luego reconstruirlos una vez que la carga ha finalizado para maximizar la velocidad. Es el método preferido para las cargas iniciales o para añadir grandes conjuntos de datos periódicamente.

Cargar archivos Parquet o CSV directamente en una tabla de un data warehouse en la nube como Snowflake, Amazon Redshift o Google BigQuery, utilizando sus comandos nativos de carga masiva.

Supongamos que en Snowflake, tenemos un archivo CSV llamado `sales_data.csv` en un bucket S3.

```sql
-- Crear un formato de archivo para CSV
CREATE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('\\N', 'NULL')
EMPTY_FIELD_AS_NULL = TRUE;

-- Crear un stage externo que apunte a un bucket S3
CREATE STAGE my_s3_stage
URL = 's3://your-s3-bucket/data/sales/'
CREDENTIALS = (AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID', AWS_SECRET_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY');

-- Crear la tabla donde se cargarán los datos
CREATE TABLE sales_transactions (
    transaction_id INT,
    product_id VARCHAR(50),
    customer_id INT,
    amount DECIMAL(10, 2),
    transaction_date DATE
);

-- Usar el comando COPY INTO para realizar la carga masiva desde S3 a la tabla
COPY INTO sales_transactions
FROM @my_s3_stage/sales_data.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE'; -- Continúa la carga incluso si hay errores en algunas filas

-- Ejemplo de carga masiva en Google BigQuery desde un archivo CSV en GCS
-- Asumiendo que el esquema está en un archivo JSON o se detecta automáticamente
bq load --source_format=CSV --skip_leading_rows=1 \
your_dataset.your_table gs://your-gcs-bucket/data/sales/sales_data.csv \
./schema.json # O BigQuery detecta el esquema automáticamente
```

### 3.7.4. Patrones de Control y Monitoreo

La resiliencia y la observabilidad son cruciales en los pipelines de Big Data. Estos patrones aseguran que los procesos puedan recuperarse de fallas y que se puedan identificar y resolver problemas de manera eficiente.

##### Checkpoint y Restart

El patrón Checkpoint y Restart implica guardar periódicamente el estado de un proceso de larga duración en puntos de control (`checkpoints`). En caso de una falla (por ejemplo, interrupción de la red, error de software, fallo de un nodo), el proceso puede reanudarse desde el último checkpoint exitoso en lugar de tener que comenzar desde el principio. Esto no solo ahorra tiempo y recursos al evitar el reprocesamiento de datos ya completados, sino que también garantiza la integridad de los datos en entornos distribuidos donde las fallas son comunes. Es fundamental para la robustez de los pipelines de datos distribuidos y en tiempo real.

Un proceso de Spark Streaming que procesa datos de Kafka. Si el clúster de Spark falla, puede reiniciar desde el último offset de Kafka procesado con éxito, sin perder datos ni procesar duplicados.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import os

spark = SparkSession.builder.appName("SparkStreamingCheckpoint") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .getOrCreate()

# Limpiar el directorio de checkpoint para una nueva ejecución
if os.path.exists("/tmp/spark_checkpoint"):
    import shutil
    shutil.rmtree("/tmp/spark_checkpoint")
    print("Cleaned existing checkpoint directory.")

# Simular un stream de datos desde una fuente de datos (ej. Kafka)
# Para este ejemplo, usaremos una fuente de datos en memoria para simplicidad
# En un caso real, esto sería: spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "input_topic").load()
data = [("A", 1), ("B", 2), ("C", 3), ("D", 4)]
df_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Transformación de ejemplo
transformed_df = df_stream.withColumn("processed_time", current_timestamp()) \
                           .withColumn("value_squared", col("value") * col("value"))

# Escribir el stream a la consola con un checkpoint
query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

# Para simular una falla, puedes detener el script manualmente después de unos segundos
# y luego reiniciarlo. Spark debería continuar desde donde se quedó.

query.awaitTermination()
```

##### Dead Letter Queue (DLQ)

Una Dead Letter Queue (DLQ), o cola de mensajes no procesables, es un mecanismo donde se enrutan los mensajes o registros que fallaron en su procesamiento. En un pipeline de datos, si un registro no cumple con las validaciones, causa un error de procesamiento o no puede ser entregado al destino después de varios reintentos, en lugar de detener todo el pipeline, se mueve a la DLQ. Esto permite que el flujo de datos principal continúe ininterrumpido mientras los registros problemáticos pueden ser analizados, depurados y, si es posible, reprocesados manualmente o a través de un proceso separado. Mejora la robustez y la resiliencia del sistema.

En un sistema de procesamiento de eventos en tiempo real con Apache Kafka y Kafka Streams, los mensajes que fallan la deserialización o validación pueden ser enviados a una DLQ.

Procesamiento de mensajes fallidos en AWS SQS y Lambda, supongamos que una función Lambda procesa mensajes de SQS. Si falla, el mensaje se envía a una DLQ.

```python
# Código Python para una función AWS Lambda (ejemplo conceptual)
import json
import boto3

def lambda_handler(event, context):
    sqs_client = boto3.client('sqs')
    dlq_url = '[https://sqs.us-east-1.amazonaws.com/123456789012/MyDeadLetterQueue](https://sqs.us-east-1.amazonaws.com/123456789012/MyDeadLetterQueue)'

    for record in event['Records']:
        message_body = json.loads(record['body'])
        try:
            # Lógica de procesamiento de datos
            if message_body.get('error_flag'):
                raise ValueError("Simulating a processing error for this message.")

            print(f"Successfully processed message: {message_body}")
            # Si se procesa con éxito, el mensaje se elimina de la cola principal automáticamente
        except Exception as e:
            print(f"Error processing message: {message_body}, Error: {e}")
            # Enviar el mensaje a la DLQ
            # En AWS SQS, esto se configura directamente en la cola principal con una Redrive Policy
            # La Lambda no necesita enviar explícitamente a la DLQ si la Redrive Policy está configurada
            # pero este pseudo-código muestra la intención de manejo de errores
            sqs_client.send_message(
                QueueUrl=dlq_url,
                MessageBody=json.dumps({"original_message": message_body, "error": str(e)})
            )
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
```

**Configuración de SQS (Redrive Policy)**:
En la consola de AWS SQS, para la cola principal, se configura una "Redrive policy" que especifica la DLQ a la que se deben enviar los mensajes después de un número determinado de intentos fallidos.

##### Idempotencia

La idempotencia es una propiedad de una operación que, cuando se ejecuta múltiples veces con los mismos parámetros de entrada, produce el mismo resultado y efecto secundario que si se hubiera ejecutado una sola vez. En el contexto de los pipelines ETL, diseñar procesos idempotentes es crucial para la recuperación de errores y el reprocesamiento seguro. Si un paso del ETL falla y se reintenta, la idempotencia asegura que los datos no se dupliquen o corrompan en el destino. Esto es especialmente importante en sistemas distribuidos donde las operaciones pueden fallar parcialmente o los mensajes pueden entregarse varias veces.

Un proceso de carga de datos que utiliza una clave primaria para `UPSERT` registros en lugar de solo `INSERT`, garantizando que si se ejecuta dos veces, el registro solo se actualice o se inserte una vez.

```sql
-- Suponiendo una tabla de destino `processed_transactions` con una clave primaria `transaction_id`
-- y una tabla de staging `new_transactions`

-- Enfoque no idempotente (podría insertar duplicados si se re-ejecuta)
-- INSERT INTO processed_transactions (transaction_id, amount, status)
-- SELECT transaction_id, amount, status FROM new_transactions;

-- Enfoque idempotente usando UPSERT (para PostgreSQL, por ejemplo)
INSERT INTO processed_transactions (transaction_id, amount, status, last_processed_at)
SELECT transaction_id, amount, status, NOW()
FROM new_transactions
ON CONFLICT (transaction_id) DO UPDATE SET
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    last_processed_at = EXCLUDED.last_processed_at;

-- Otro ejemplo conceptual: procesar archivos y marcarlos como procesados
-- Si el proceso lee archivos de un bucket, y los mueve o renombra después de procesarlos
-- Esto es idempotente porque no procesará el mismo archivo dos veces si ya fue movido/renombrado.
# Pseudo-código Python para un procesamiento de archivos idempotente
def process_file_idempotently(file_path, processed_path):
    if not file_exists(file_path):
        print(f"File {file_path} does not exist, likely already processed.")
        return

    try:
        # Lógica de procesamiento del archivo
        print(f"Processing file: {file_path}")
        # ... hacer algo con el archivo ...

        # Mover el archivo a una carpeta de "procesados"
        move_file(file_path, processed_path)
        print(f"File {file_path} moved to {processed_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        # En caso de error, el archivo permanece en la ubicación original para reintento
```

### 3.7.5. Patrones Arquitectónicos

Estos patrones definen la estructura general de los sistemas de Big Data, abordando cómo los componentes interactúan para manejar el flujo de datos.

##### Staging Area

Una Staging Area, o área de preparación, es un espacio de almacenamiento temporal donde los datos extraídos de las fuentes se almacenan antes de ser transformados y cargados al destino final (generalmente un data warehouse o data lake). Esta área sirve como un "amortiguador" entre los sistemas fuente y el destino. Ofrece múltiples beneficios:

* **Punto de Recuperación**: Si una transformación falla, los datos brutos aún están disponibles en el área de staging para ser reprocesados sin necesidad de re-extraer desde la fuente.
* **Aislamiento**: Protege los sistemas fuente de las cargas de procesamiento de las transformaciones y permite que las validaciones y limpieza iniciales se realicen en un entorno separado.
* **Consistencia**: Permite consolidar datos de múltiples fuentes heterogéneas en un formato común antes de la transformación.
* **Validación**: Es un lugar ideal para realizar comprobaciones de calidad de datos iniciales.

Cargar archivos CSV desde un sistema de punto de venta a un bucket de AWS S3 (staging area) antes de que un trabajo de Spark los procese y los cargue en un data warehouse en Redshift.

```python
# Pseudo-código de un script de AWS Glue (PySpark)
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Extracción de datos brutos a la Staging Area (ej. S3)
# Este paso generalmente se hace fuera de Glue, por ejemplo, vía AWS DataSync, Kinesis Firehose o FTP.
# Asumimos que los archivos ya están en s3://your-bucket/raw_data/

# 2. Leer datos desde la Staging Area
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/raw_data/sales_data/"], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# 3. Aplicar transformaciones
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[
    ("transaction_id", "string", "transaction_id", "long"),
    ("product_name", "string", "product_name", "string"),
    ("amount", "string", "amount", "double")
])

# 4. Cargar a la tabla final (ej. Redshift)
datasink2 = glueContext.write_dynamic_frame.from_jdbc_conf(
    catalog_connection="redshift_connection",
    connection_options={"dbtable": "public.processed_sales", "database": "dev"},
    redshift_tmp_dir=args["TempDir"],
    frame=applymapping1
)

job.commit()
```

##### Hub and Spoke

El patrón Hub and Spoke, o "Centro y Radios", es una arquitectura que centraliza la extracción de datos de múltiples fuentes en un punto central (el "Hub") antes de distribuir esos datos a varios destinos o consumidores (los "Spokes"). En lugar de tener conexiones punto a punto entre cada fuente y cada destino (lo que puede volverse inmanejable con muchas integraciones), los datos fluyen a través de un nodo centralizado. Este patrón simplifica la gestión de integraciones, mejora la consistencia de los datos, facilita el monitoreo centralizado y reduce la complejidad al escalar.

Una empresa con múltiples sistemas de origen (CRM, ERP, bases de datos de clientes) que envían datos a un Data Lake central (usando Apache Kafka como Hub), desde donde los datos se distribuyen a data warehouses, herramientas de BI, o modelos de Machine Learning (los Spokes).

```python
# Pseudo-código: Múltiples productores enviando a Kafka (el Hub)
# Productor 1 (CRM)
from kafka import KafkaProducer
import json
producer_crm = KafkaProducer(bootstrap_servers='localhost:9099')
producer_crm.send('raw_data_hub_topic', json.dumps({"source": "CRM", "data": {"customer_id": "C123", "name": "Alice"}}).encode('utf-8'))

# Productor 2 (ERP)
producer_erp = KafkaProducer(bootstrap_servers='localhost:9099')
producer_erp.send('raw_data_hub_topic', json.dumps({"source": "ERP", "data": {"order_id": "O456", "item": "Laptop"}}).encode('utf-8'))

# Un consumidor centralizado (el Hub) que ingiere y clasifica/transforma datos
# Este podría ser un proceso Spark Streaming, un Kafka Streams application, etc.
# Su función es tomar los datos brutos del topic 'raw_data_hub_topic'
# y luego, después de una limpieza o estandarización mínima,
# publicarlos en topics más específicos para los spokes.
# Ejemplo: 'customers_topic', 'orders_topic'

# Pseudo-código de un consumidor/procesador central (parte del Hub)
from kafka import KafkaConsumer
import json

consumer_hub = KafkaConsumer('raw_data_hub_topic', bootstrap_servers='localhost:9099', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer_processed = KafkaProducer(bootstrap_servers='localhost:9099')

for message in consumer_hub:
    data = message.value
    if data['source'] == 'CRM':
        # Simple transformación: agregar un timestamp
        data['data']['processed_at'] = time.time()
        producer_processed.send('processed_customers_topic', json.dumps(data['data']).encode('utf-8'))
    elif data['source'] == 'ERP':
        data['data']['processed_at'] = time.time()
        producer_processed.send('processed_orders_topic', json.dumps(data['data']).encode('utf-8'))

# Múltiples consumidores (Spokes) leyendo de topics específicos
# Consumidor Spoke 1 (Data Warehouse para clientes)
consumer_dw_customers = KafkaConsumer('processed_customers_topic', bootstrap_servers='localhost:9099')
# Lógica para cargar en Data Warehouse

# Consumidor Spoke 2 (Sistema de analítica de pedidos)
consumer_analytics_orders = KafkaConsumer('processed_orders_topic', bootstrap_servers='localhost:9099')
# Lógica para alimentar sistema de analítica
```

##### Event-Driven ETL

El patrón Event-Driven ETL (ETL impulsado por eventos) transforma los procesos ETL de operaciones programadas (batch) a operaciones reactivas que se disparan en respuesta a eventos específicos. Estos eventos pueden ser la llegada de un nuevo archivo a un bucket de almacenamiento, un cambio en una base de datos (usando CDC), la publicación de un mensaje en una cola de mensajes, o una señal de un sistema externo. Este enfoque reduce la latencia de los datos, optimiza el uso de recursos al procesar solo cuando hay datos nuevos, y permite una mayor agilidad en el flujo de información. Es fundamental para arquitecturas de datos en tiempo real y microservicios.

Cuando se carga un archivo de log a un bucket AWS S3, un evento de S3 activa una función AWS Lambda, que a su vez invoca un trabajo de AWS Glue para procesar el log y cargarlo en un data lake.

1. Configuración de S3 para enviar eventos a Lambda: Configurar una notificación de evento en un bucket S3 para `s3:ObjectCreated:*` que apunte a una función Lambda.

2. Código de la función AWS Lambda (Python):

```python
import json
import boto3

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    glue_client = boto3.client('glue')

    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        print(f"New object created in bucket: {bucket_name}, key: {object_key}")

        # Determinar qué trabajo de Glue ejecutar basado en el prefijo del objeto, etc.
        # Aquí, un ejemplo simple que siempre dispara el mismo trabajo
        job_name = "MyGlueETLJob"
            
        try:
            # Disparar el trabajo de AWS Glue
            response = glue_client.start_job_run(
                JobName=job_name,
                Arguments={
                    '--input_bucket': bucket_name,
                    '--input_key': object_key
                }
            )
            print(f"Started Glue job {job_name} with RunId: {response['JobRunId']}")
        except Exception as e:
            print(f"Error starting Glue job {job_name}: {e}")
            raise e # Re-lanzar la excepción para que Lambda la maneje si es necesario

    return {
        'statusCode': 200,
        'body': json.dumps('Glue job triggered successfully!')
    }
```

3. Código de AWS Glue Job (PySpark): Este trabajo de Glue leería el archivo especificado por `--input_key` y lo procesaría.

```python
# Pseudo-código para el trabajo de AWS Glue (PySpark)
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_bucket', 'input_key'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = f"s3://{args['input_bucket']}/{args['input_key']}"
print(f"Processing file: {input_path}")

# Leer el archivo que activó el evento
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Realizar transformaciones (ejemplo: contar filas)
row_count = df.count()
print(f"File {args['input_key']} has {row_count} rows.")

# Escribir el resultado a otro destino (ej. una tabla procesada en S3 o un Data Warehouse)
output_path = f"s3://your-processed-bucket/processed_data/{args['input_key'].replace('.csv', '')}_processed.parquet"
df.write.mode("overwrite").parquet(output_path)

print(f"Processed data written to: {output_path}")

job.commit()
```

### 3.7.6. Estrategias de optimización de costos

La optimización de costos en la nube es tan crucial como el rendimiento. Permite a las organizaciones escalar sin incurrir en gastos excesivos, garantizando la sostenibilidad financiera de las operaciones de Big Data.

##### Autoscaling

El Autoscaling (autoescalado) es la capacidad de un sistema en la nube para ajustar automáticamente la cantidad de recursos de computación utilizados en función de la demanda actual. Esto significa que la infraestructura puede aumentar (escalar hacia arriba) durante los picos de carga y disminuir (escalar hacia abajo) durante los períodos de baja actividad. El autoscaling es fundamental para la optimización de costos porque se paga solo por los recursos que realmente se utilizan, evitando el sobreaprovisionamiento y el desperdicio de capacidad. Aplica tanto a clústeres de computación (ej. instancias EC2, nodos de Spark) como a servicios de bases de datos o colas.

Un clúster de Apache Spark en Amazon EMR que automáticamente añade o quita nodos de worker según la carga de trabajo de los jobs.

```json
# Configuración de Auto Scaling en un Cluster de EMR (ejemplo simplificado)
{
  "InstanceGroups": [
    {
      "InstanceRole": "MASTER",
      "InstanceType": "m5.xlarge",
      "InstanceCount": 1
    },
    {
      "InstanceRole": "CORE",
      "InstanceType": "m5.xlarge",
      "InstanceCount": 2,
      "AutoScalingPolicy": {
        "Constraints": {
          "MinCapacity": 2,
          "MaxCapacity": 10
        },
        "Rules": [
          {
            "Name": "ScaleOut",
            "Description": "Scale out when YARN memory utilization is high",
            "Action": {
              "Market": "ON_DEMAND",
              "SimpleScalingPolicyConfiguration": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": 1,
                "CoolDown": 300
              }
            },
            "Trigger": {
              "CloudWatchAlarmDefinition": {
                "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                "EvaluationPeriods": 5,
                "MetricName": "YARNMemoryAvailablePercentage",
                "Namespace": "AWS/ElasticMapReduce",
                "Period": 300,
                "Statistic": "Average",
                "Threshold": 15, # Trigger if available memory drops below 15%
                "Unit": "Percent",
                "Dimensions": [
                  {"Name": "ClusterId", "Value": "${emr.cluster.id}"}
                ]
              }
            }
          },
          {
            "Name": "ScaleIn",
            "Description": "Scale in when YARN memory utilization is low",
            "Action": {
              "Market": "ON_DEMAND",
              "SimpleScalingPolicyConfiguration": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": -1,
                "CoolDown": 300
              }
            },
            "Trigger": {
              "CloudWatchAlarmDefinition": {
                "ComparisonOperator": "LESS_THAN_OR_EQUAL",
                "EvaluationPeriods": 5,
                "MetricName": "YARNMemoryAvailablePercentage",
                "Namespace": "AWS/ElasticMapReduce",
                "Period": 300,
                "Statistic": "Average",
                "Threshold": 80, # Trigger if available memory is above 80% (low utilization)
                "Unit": "Percent",
                "Dimensions": [
                  {"Name": "ClusterId", "Value": "${emr.cluster.id}"}
                ]
              }
            }
          }
        ]
      }
    }
  ]
}
```

##### Uso de spot instances

Las `spot instances` (instancias spot) son instancias de computación en la nube que se ofrecen a un precio significativamente reducido (hasta un 90% de descuento en comparación con las instancias bajo demanda) a cambio de la posibilidad de que el proveedor de la nube las recupere (interrumpa) con poca antelación si necesita la capacidad. Son ideales para cargas de trabajo tolerantes a fallos, no críticas, o aquellas que pueden resumirse fácilmente, como trabajos de procesamiento de Big Data por lotes que pueden reintentar tareas o donde los datos pueden ser reprocesados. Su uso puede generar ahorros masivos en la infraestructura de computación.

Ejecutar un gran trabajo de Spark para un análisis de datos que no es de misión crítica y que se ejecuta durante la noche. Si algunas instancias spot son interrumpidas, Spark puede redistribuir el trabajo a otras instancias o reintentar las tareas fallidas.

Al crear un clúster EMR, se puede especificar el tipo de instancias para los grupos de instancias Core y Task.

```json
# Configuración de un cluster EMR usando instancias Spot para los grupos de Core y Task
{
  "Name": "MySpotEMRCluster",
  "ReleaseLabel": "emr-6.x.0",
  "Applications": [
    {"Name": "Spark"},
    {"Name": "Hadoop"}
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master Instance Group",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core Instance Group",
        "InstanceRole": "CORE",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 2,
        "Market": "SPOT", # Usar Spot Instances para Core
        "BidPrice": "0.50" # Por ejemplo, ofertar hasta $0.50 por hora
      },
      {
        "Name": "Task Instance Group",
        "InstanceRole": "TASK",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 2,
        "Market": "SPOT", # Usar Spot Instances para Task
        "BidPriceAsPercentageOfOnDemandPrice": 100 # Ofertar el 100% del precio bajo demanda, aún así es más barato
      }
    ]
  },
  "JobFlowRole": "EMR_EC2_DefaultRole",
  "ServiceRole": "EMR_DefaultRole"
}
```

##### Diseño para uso eficiente del almacenamiento y ejecución

La optimización del almacenamiento y la ejecución son pilares de la reducción de costos en la nube, ya que ambos recursos son facturados por uso. Un diseño eficiente implica:

* **Formatos de Almacenamiento**: Usar formatos de archivo optimizados para Big Data como Parquet u ORC. Estos formatos son columnares, lo que permite una mayor compresión y una lectura más eficiente al seleccionar solo las columnas necesarias. También soportan esquemas y metadatos, mejorando la interoperabilidad.
* **Compresión de Datos**: Aplicar algoritmos de compresión (Snappy, Gzip, Zstd) a los datos almacenados. Esto reduce el espacio de almacenamiento y, consecuentemente, el costo. Además, al haber menos datos que transferir, se mejora el rendimiento de la lectura.
* **Particionamiento y Agrupación (Bucketing)**: Organizar los datos en el almacenamiento (ej. S3, HDFS) en directorios lógicos (particiones) basados en columnas de uso frecuente (ej. fecha, región). Esto permite que las consultas escaneen solo un subconjunto de los datos, reduciendo el volumen de I/O y los costos de cómputo. El bucketing organiza datos dentro de las particiones para optimizar joins y agregaciones.
* **Selección de Instancias y Tipo de Computación**: Elegir el tipo y tamaño de instancia adecuados para la carga de trabajo. No sobre-aprovisionar CPU o memoria. Considerar el uso de servicios sin servidor (serverless) como AWS Lambda o Google Cloud Functions para tareas de corta duración, ya que solo se paga por el tiempo de ejecución.
* **Optimización de Consultas**: Escribir consultas SQL o scripts de procesamiento que minimicen el escaneo de datos, aprovechen los índices o las particiones, y eviten operaciones costosas como `FULL SCAN` o `CROSS JOIN` innecesarios.

Almacenar datos en un Data Lake en S3 en formato Parquet, particionados por `año/mes/día`, y usar Spark para procesar solo las particiones relevantes para una consulta.

```python
# PySpark y almacenamiento en Parquet particionado
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

spark = SparkSession.builder.appName("EfficientStorageAndExecution").getOrCreate()

# Simular la creación de un DataFrame de logs
log_data = [
    ("user1", "login", "2023-01-01 10:00:00"),
    ("user2", "logout", "2023-01-01 10:05:00"),
    ("user1", "purchase", "2023-01-02 11:15:00"),
    ("user3", "login", "2023-01-02 12:00:00"),
    ("user4", "view_product", "2023-02-15 09:30:00")
]
logs_df = spark.createDataFrame(log_data, ["user_id", "event_type", "event_timestamp"])

# Convertir el timestamp a tipo de dato de fecha y extraer componentes para particionamiento
logs_df = logs_df.withColumn("event_date", col("event_timestamp").cast("date")) \
                 .withColumn("year", year(col("event_date"))) \
                 .withColumn("month", month(col("event_date"))) \
                 .withColumn("day", dayofmonth(col("event_date")))

# Escribir el DataFrame en formato Parquet, particionado por año, mes, y día
# Esto creará una estructura de carpetas como s3://your-bucket/logs/year=2023/month=1/day=1/
output_path = "/tmp/logs_partitioned_parquet" # En un entorno cloud, sería s3://your-bucket/logs/
logs_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)

print(f"Data written to {output_path} with partitioning.")

# Leer solo los datos de una partición específica para una consulta (eficiente)
# Por ejemplo, para obtener logs del 1 de enero de 2023
query_df = spark.read.parquet(f"{output_path}/year=2023/month=1/day=1")
print("\nLogs for 2023-01-01:")
query_df.show()
# +-------+----------+-------------------+----------+-----+-----+---+
# |user_id|event_type|    event_timestamp|event_date| year|month|day|
# +-------+----------+-------------------+----------+-----+-----+---+
# |  user1|     login|2023-01-01 10:00:00|2023-01-01| 2023|    1|  1|
# |  user2|    logout|2023-01-01 10:05:00|2023-01-01| 2023|    1|  1|
# +-------+----------+-------------------+----------+-----+-----+---+

# Al consultar, Spark solo leerá los archivos dentro de esa partición específica,
# ahorrando I/O y cómputo.
spark.stop()
```

## Tarea

1.  **Diseño de un Pipeline ETL Event-Driven (Teórico)**: Imagina que trabajas para una empresa de e-commerce. Cuando se carga un nuevo archivo CSV de "devoluciones de productos" a un bucket de S3, necesitas que se active automáticamente un proceso ETL para:
    * Cargar el CSV.
    * Validar que las columnas clave (ID de producto, cantidad, fecha de devolución) no estén vacías.
    * Enriquecer los datos con el nombre del producto y la categoría, consultando una tabla de productos existente.
    * Actualizar una tabla de métricas de devoluciones, realizando un `UPSERT` para evitar duplicados y manteniendo un registro único por producto y fecha de devolución.
    Describe paso a paso cómo diseñarías esta arquitectura en la nube usando servicios de AWS (S3, Lambda, Glue, Redshift/DynamoDB para la tabla de productos y la de métricas).

2.  **Optimización de Costos en un Cluster Spark (Análisis de Caso)**: Tu equipo ha notado que el costo de su clúster de Spark (ejecutado en EMR o Databricks) para los trabajos ETL diarios se ha disparado. Los trabajos se ejecutan una vez al día y tardan aproximadamente 3 horas. Actualmente, utilizan instancias bajo demanda grandes. Propón al menos tres estrategias específicas, basadas en lo aprendido, para reducir significativamente estos costos, justificando cada una con sus pros y contras.

3.  **Implementación de SCD Tipo 2 (SQL)**: Diseña una tabla de dimensiones para `dim_productos` que soporte SCD Tipo 2. La tabla debe incluir al menos `product_id`, `product_name`, `category`, `price`, `start_date`, `end_date`, y `is_current`. Luego, escribe las sentencias SQL (simulando una base de datos relacional) para manejar un escenario donde:
    * Llega un nuevo producto.
    * El precio de un producto existente cambia.
    * El nombre de un producto existente cambia.
    Asegúrate de que la lógica de actualización cierre la versión anterior y cree una nueva.

4.  **Desarrollo de un Patrón de Extracción Incremental (Python/Pandas o PySpark)**: Escribe un script en Python (puedes usar Pandas para simular DataFrames pequeños o PySpark si tienes un entorno) que demuestre la extracción incremental. Simula una tabla de origen con una columna `last_updated_at`. Crea una lógica para:
    * Mantener un "último timestamp procesado" en una variable o archivo.
    * Extraer solo los registros donde `last_updated_at` es mayor que el último timestamp procesado.
    * Actualizar el "último timestamp procesado" después de una extracción exitosa.
    Proporciona un ejemplo de datos de entrada y salida para dos ejecuciones sucesivas del script.

5.  **Diseño Idempotente para Carga de Archivos (Conceptos)**: Explica cómo harías que un proceso de carga de archivos (que lee archivos de un directorio de entrada y los mueve a un directorio de "procesados") sea idempotente. ¿Qué sucedería si el proceso fallara justo después de leer un archivo pero antes de moverlo? ¿Cómo asegurarías que al reintentar el proceso, ese archivo no sea procesado dos veces, o que su estado final sea el mismo que si se hubiera procesado una sola vez correctamente?

