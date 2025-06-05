# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.2. Conexión a Múltiples Fuentes de Datos

**Objetivo**:

Comprender cómo integrar múltiples tipos de fuentes de datos —estructuradas, semiestructuradas y no estructuradas— en un pipeline ETL, utilizando herramientas como Apache Spark y Apache Airflow. Esto incluye el uso de conectores apropiados, estrategias de autenticación segura, y técnicas de lectura eficientes.

**Introducción**:

En entornos Big Data, los datos se originan desde diversas fuentes con distintos formatos, velocidades y estructuras. La capacidad de extraer datos de múltiples orígenes y consolidarlos de manera eficiente en un pipeline ETL es clave para cualquier sistema analítico moderno. Este tema explora las fuentes de datos más comunes y cómo integrarlas usando tecnologías como Spark y Airflow.

**Desarrollo**:

La integración de múltiples fuentes en un pipeline ETL implica tanto desafíos técnicos como de gobernanza. Cada tipo de fuente —bases de datos, archivos, APIs— requiere técnicas y conectores específicos. Además, aspectos como la autenticación segura, la lectura incremental y el manejo de configuraciones son fundamentales para construir flujos de datos robustos y escalables. Spark y Airflow ofrecen mecanismos potentes para esta integración, facilitando el desarrollo de pipelines mantenibles y eficientes.

### 3.2.1 Tipos de fuentes comunes

La diversidad de fuentes de datos implica conocer sus características y particularidades para una integración adecuada en el pipeline. Cada tipo de fuente presenta desafíos únicos en términos de conectividad, formato de datos, escalabilidad y optimización de rendimiento.

##### Bases de datos relacionales (PostgreSQL, MySQL)

Las bases de datos relacionales constituyen el backbone de muchas organizaciones, almacenando datos estructurados con esquemas bien definidos y relaciones entre tablas. Estas fuentes requieren conexión vía JDBC (Java Database Connectivity) y ofrecen la ventaja de poder ejecutar consultas SQL complejas directamente en la fuente, reduciendo la transferencia de datos innecesarios.

Para una integración efectiva, es crucial considerar aspectos como la partición de datos, el control de paralelismo para evitar sobrecargar la base de datos origen, y la gestión de transacciones. Spark permite configurar parámetros como `numPartitions`, `lowerBound`, `upperBound` y `partitionColumn` para optimizar la lectura distribuida.

**Usar JDBC en Spark para leer de PostgreSQL y transformar los resultados**:

```python
from pyspark.sql import SparkSession

# Configuración de Spark con driver JDBC
spark = SparkSession.builder \
    .appName("PostgreSQL_ETL") \
    .config("spark.jars", "/path/to/postgresql-42.6.0.jar") \
    .getOrCreate()

# Configuración de conexión
jdbc_url = "jdbc:postgresql://localhost:5432/production_db"
connection_properties = {
    "user": "spark_user",
    "password": "secure_password",
    "driver": "org.postgresql.Driver",
    "numPartitions": "4",
    "partitionColumn": "id",
    "lowerBound": "1",
    "upperBound": "1000000"
}

# Lectura optimizada con particionamiento
df = spark.read \
    .jdbc(url=jdbc_url, 
          table="(SELECT * FROM sales WHERE created_date >= '2024-01-01') as sales_subset",
          properties=connection_properties)

# Transformaciones sobre los datos
df_transformed = df \
    .filter(df.amount > 100) \
    .groupBy("customer_id", "product_category") \
    .agg(
        sum("amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value")
    )

# Escritura a formato optimizado
df_transformed.write \
    .mode("overwrite") \
    .parquet("s3://data-lake/processed/sales_summary/")
```

**Ejecutar consultas filtradas desde Airflow para limitar la carga de datos**:

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'postgresql_incremental_etl',
    default_args=default_args,
    description='ETL incremental desde PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

# Operador para extraer datos incrementales
extract_incremental_data = PostgresOperator(
    task_id='extract_incremental_data',
    postgres_conn_id='postgres_production',
    sql="""
        CREATE TEMP TABLE temp_incremental_data AS
        SELECT 
            customer_id,
            order_date,
            product_id,
            quantity,
            unit_price,
            total_amount
        FROM orders 
        WHERE order_date >= '{{ ds }}' 
        AND order_date < '{{ next_ds }}'
        AND status = 'completed';
        
        COPY temp_incremental_data TO '/tmp/incremental_data_{{ ds }}.csv' 
        WITH CSV HEADER;
    """,
    dag=dag
)

# Función personalizada para transferir datos
def transfer_to_spark(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_production')
    
    # Consulta optimizada con límites
    sql_query = f"""
        SELECT * FROM orders 
        WHERE order_date = '{context['ds']}'
        AND total_amount > 0
        ORDER BY order_id
        LIMIT 100000
    """
    
    # Exportar a formato intermedio
    df = postgres_hook.get_pandas_df(sql_query)
    df.to_parquet(f"/tmp/orders_{context['ds']}.parquet")
    
    return f"/tmp/orders_{context['ds']}.parquet"

transfer_task = PythonOperator(
    task_id='transfer_to_staging',
    python_callable=transfer_to_spark,
    dag=dag
)

extract_incremental_data >> transfer_task
```

##### Almacenes NoSQL (MongoDB, Cassandra)

Los almacenes NoSQL ofrecen flexibilidad de esquema y están diseñados para manejar grandes volúmenes de datos semiestructurados con alta disponibilidad y escalabilidad horizontal. MongoDB es ideal para documentos JSON complejos, mientras que Cassandra sobresale en casos de uso que requieren escrituras masivas y alta disponibilidad.

La integración con estos sistemas requiere conectores específicos y consideraciones especiales para el particionamiento y la distribución de carga. Es importante entender los patrones de acceso a datos y las limitaciones de cada sistema para optimizar las consultas.

**Ingestar documentos de MongoDB con Spark para análisis de logs**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuración de Spark con conector MongoDB
spark = SparkSession.builder \
    .appName("MongoDB_Log_Analysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/logs.application_logs") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/analytics.processed_logs") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Lectura de documentos con filtros a nivel de MongoDB
df_logs = spark.read \
    .format("mongo") \
    .option("collection", "application_logs") \
    .option("pipeline", '[{"$match": {"timestamp": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}}]') \
    .load()

# Análisis de logs con transformaciones complejas
df_analyzed = df_logs \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("log_level", upper(col("level"))) \
    .withColumn("response_time_ms", col("response_time").cast("integer")) \
    .filter(col("response_time_ms").isNotNull()) \
    .groupBy("hour", "log_level", "service_name") \
    .agg(
        count("*").alias("log_count"),
        avg("response_time_ms").alias("avg_response_time"),
        max("response_time_ms").alias("max_response_time"),
        countDistinct("user_id").alias("unique_users")
    )

# Detección de anomalías
df_anomalies = df_analyzed \
    .filter(
        (col("avg_response_time") > 5000) | 
        (col("log_level") == "ERROR") |
        (col("log_count") > 10000)
    )

# Escritura de resultados procesados
df_analyzed.write \
    .format("mongo") \
    .option("collection", "hourly_metrics") \
    .mode("overwrite") \
    .save()

df_anomalies.write \
    .format("mongo") \
    .option("collection", "detected_anomalies") \
    .mode("append") \
    .save()
```

**Usar Cassandra para almacenar datos de sensores IoT y analizarlos por lotes**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuración para Cassandra
spark = SparkSession.builder \
    .appName("IoT_Sensor_Analytics") \
    .config("spark.cassandra.connection.host", "cassandra-cluster.example.com") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "spark_user") \
    .config("spark.cassandra.auth.password", "secure_password") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()

# Lectura de datos de sensores con filtros optimizados
df_sensor_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="sensor_readings", keyspace="iot_data") \
    .option("spark.cassandra.input.split.size_in_mb", "512") \
    .load() \
    .filter(col("timestamp") >= lit("2024-01-01")) \
    .filter(col("sensor_type").isin(["temperature", "humidity", "pressure"]))

# Análisis de tendencias y agregaciones
df_hourly_aggregates = df_sensor_data \
    .withColumn("hour_bucket", date_trunc("hour", col("timestamp"))) \
    .groupBy("device_id", "sensor_type", "location", "hour_bucket") \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        stddev("value").alias("stddev_value"),
        count("*").alias("reading_count")
    )

# Detección de valores anómalos usando ventanas
from pyspark.sql.window import Window

window_spec = Window.partitionBy("device_id", "sensor_type").orderBy("hour_bucket")

df_with_trends = df_hourly_aggregates \
    .withColumn("prev_avg", lag("avg_value").over(window_spec)) \
    .withColumn("value_change", col("avg_value") - col("prev_avg")) \
    .withColumn("is_anomaly", 
                when(abs(col("value_change")) > 3 * col("stddev_value"), True)
                .otherwise(False))

# Escritura optimizada a Cassandra con TTL
df_hourly_aggregates.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="hourly_sensor_metrics", keyspace="iot_analytics") \
    .option("spark.cassandra.output.ttl", "2592000") \
    .mode("append") \
    .save()

# Almacenar anomalías para alertas
df_anomalies = df_with_trends.filter(col("is_anomaly") == True)
df_anomalies.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="sensor_anomalies", keyspace="iot_alerts") \
    .mode("append") \
    .save()
```

##### Archivos planos (CSV, JSON, Parquet)

Los archivos planos representan una forma común de almacenar y intercambiar datos, especialmente en arquitecturas de data lake. Cada formato tiene sus ventajas: CSV para simplicidad e interoperabilidad, JSON para datos semiestructurados, y Parquet para optimización de almacenamiento y consultas analíticas.

Spark proporciona APIs nativas optimizadas para estos formatos, con capacidades avanzadas como lectura schema-on-read, particionamiento automático, y optimizaciones de predicados pushdown en el caso de Parquet.

**Leer archivos CSV con Spark y aplicar transformaciones**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("CSV_Processing_Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Definición de schema para optimizar la lectura
sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("discount", DecimalType(5,2), True),
    StructField("transaction_date", DateType(), True),
    StructField("store_location", StringType(), True)
])

# Lectura optimizada de múltiples archivos CSV
df_sales = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("multiline", "true") \
    .option("escape", '"') \
    .schema(sales_schema) \
    .csv("s3://data-lake/raw/sales/2024/*/sales_*.csv")

# Limpieza y validación de datos
df_cleaned = df_sales \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .withColumn("total_amount", 
                col("quantity") * col("unit_price") * (1 - col("discount")/100)) \
    .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM")) \
    .withColumn("day_of_week", dayofweek(col("transaction_date"))) \
    .filter(col("total_amount").isNotNull())

# Agregaciones para análisis de ventas
df_sales_summary = df_cleaned \
    .groupBy("year_month", "store_location", "day_of_week") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum("quantity").alias("total_units_sold"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("total_amount").alias("avg_transaction_value"),
        count("transaction_id").alias("transaction_count")
    ) \
    .withColumn("revenue_per_customer", 
                col("total_revenue") / col("unique_customers"))

# Escritura particionada para optimizar consultas futuras
df_sales_summary.write \
    .partitionBy("year_month") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("s3://data-lake/processed/sales_summary/")

# Cache para reutilización en múltiples análisis
df_cleaned.cache()

# Análisis adicional: productos top por ubicación
df_top_products = df_cleaned \
    .groupBy("store_location", "product_id") \
    .agg(sum("quantity").alias("total_sold")) \
    .withColumn("rank", 
                row_number().over(
                    Window.partitionBy("store_location")
                    .orderBy(desc("total_sold"))
                )) \
    .filter(col("rank") <= 10)

df_top_products.write \
    .mode("overwrite") \
    .json("s3://data-lake/analytics/top_products_by_location/")
```

**Ingerir JSON de logs web para análisis de tráfico**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Web_Logs_Traffic_Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Lectura de logs JSON con schema flexible
df_logs = spark.read \
    .option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3://logs-bucket/web-logs/2024/*/*/*.json")

# Extracción y procesamiento de campos anidados
df_processed = df_logs \
    .withColumn("timestamp", to_timestamp(col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("ip_address", col("clientip")) \
    .withColumn("user_agent", col("agent")) \
    .withColumn("request_method", regexp_extract(col("request"), r'^(\w+)', 1)) \
    .withColumn("request_url", regexp_extract(col("request"), r'^\w+\s+([^\s]+)', 1)) \
    .withColumn("response_code", col("response").cast("integer")) \
    .withColumn("response_size", col("bytes").cast("long")) \
    .withColumn("referrer", col("referrer")) \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("date", to_date(col("timestamp"))) \
    .filter(col("_corrupt_record").isNull()) \
    .drop("_corrupt_record")

# Análisis de tráfico web
df_traffic_analysis = df_processed \
    .groupBy("date", "hour", "request_method") \
    .agg(
        count("*").alias("request_count"),
        countDistinct("ip_address").alias("unique_visitors"),
        avg("response_size").alias("avg_response_size"),
        sum("response_size").alias("total_bytes_served"),
        sum(when(col("response_code") >= 400, 1).otherwise(0)).alias("error_count"),
        sum(when(col("response_code") == 200, 1).otherwise(0)).alias("success_count")
    ) \
    .withColumn("error_rate", col("error_count") / col("request_count") * 100) \
    .withColumn("success_rate", col("success_count") / col("request_count") * 100)

# Análisis de páginas más visitadas
df_popular_pages = df_processed \
    .filter(col("request_method") == "GET") \
    .filter(col("response_code") == 200) \
    .groupBy("date", "request_url") \
    .agg(
        count("*").alias("page_views"),
        countDistinct("ip_address").alias("unique_visitors")
    ) \
    .withColumn("rank", 
                row_number().over(
                    Window.partitionBy("date")
                    .orderBy(desc("page_views"))
                ))

# Detección de posibles ataques o comportamientos anómalos
df_security_analysis = df_processed \
    .groupBy("ip_address", "date") \
    .agg(
        count("*").alias("requests_per_day"),
        countDistinct("request_url").alias("unique_pages_accessed"),
        sum(when(col("response_code") >= 400, 1).otherwise(0)).alias("failed_requests")
    ) \
    .withColumn("suspicious_activity", 
                when(
                    (col("requests_per_day") > 10000) | 
                    (col("failed_requests") > 100) |
                    (col("unique_pages_accessed") < 2), 
                    True
                ).otherwise(False))

# Escritura de resultados en diferentes formatos
df_traffic_analysis.write \
    .partitionBy("date") \
    .mode("overwrite") \
    .parquet("s3://analytics-bucket/web-traffic-analysis/")

df_popular_pages.filter(col("rank") <= 50).write \
    .partitionBy("date") \
    .mode("overwrite") \
    .json("s3://analytics-bucket/popular-pages/")

df_security_analysis.filter(col("suspicious_activity") == True).write \
    .mode("append") \
    .option("compression", "gzip") \
    .csv("s3://security-bucket/suspicious-activity/", header=True)
```

##### APIs y servicios web

Las APIs y servicios web permiten acceder a datos en tiempo real o expuestos por terceros, proporcionando información dinámica y actualizada. La integración requiere manejo de autenticación, rate limiting, paginación, y gestión de errores. Airflow es ideal para orquestar estas integraciones mediante HttpHook, mientras que Spark puede procesar los datos obtenidos.

**Obtener tasas de cambio de divisas desde una API REST**:

```python
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import json
import pandas as pd

# Configuración del DAG
default_args = {
    'owner': 'finance-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'currency_exchange_etl',
    default_args=default_args,
    description='ETL para tasas de cambio de divisas',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Función para extraer datos de API con manejo de errores
def extract_exchange_rates(**context):
    http_hook = HttpHook(http_conn_id='exchange_api', method='GET')
    
    # Configuración de la petición
    endpoint = 'v1/latest'
    headers = {
        'Authorization': 'Bearer {{ var.value.exchange_api_key }}',
        'Content-Type': 'application/json'
    }
    
    params = {
        'base': 'USD',
        'symbols': 'EUR,GBP,JPY,CAD,AUD,CHF,CNY,MXN,BRL'
    }
    
    try:
        # Realizar petición HTTP
        response = http_hook.run(
            endpoint=endpoint,
            headers=headers,
            data=params
        )
        
        exchange_data = json.loads(response.content)
        
        # Validar respuesta
        if 'rates' not in exchange_data:
            raise ValueError("API response missing 'rates' field")
        
        # Transformar datos para almacenamiento
        rates_list = []
        base_currency = exchange_data.get('base', 'USD')
        timestamp = exchange_data.get('timestamp', context['ts'])
        
        for currency, rate in exchange_data['rates'].items():
            rates_list.append({
                'base_currency': base_currency,
                'target_currency': currency,
                'exchange_rate': float(rate),
                'timestamp': timestamp,
                'source': 'exchangerates-api',
                'extraction_date': context['ds']
            })
        
        # Guardar en archivo temporal para siguiente tarea
        df = pd.DataFrame(rates_list)
        temp_file = f"/tmp/exchange_rates_{context['ds']}.json"
        df.to_json(temp_file, orient='records', date_format='iso')
        
        return temp_file
        
    except Exception as e:
        print(f"Error extracting exchange rates: {str(e)}")
        raise

# Función para procesar y validar datos
def process_exchange_data(**context):
    temp_file = context['task_instance'].xcom_pull(task_ids='extract_rates')
    
    if not temp_file:
        raise ValueError("No data file received from extraction task")
    
    # Cargar y procesar datos
    df = pd.read_json(temp_file)
    
    # Validaciones de calidad de datos
    df = df.dropna(subset=['exchange_rate'])
    df = df[df['exchange_rate'] > 0]  # Eliminar tasas negativas o cero
    
    # Detección de valores anómalos (cambios > 10% desde última lectura)
    postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse')
    
    for currency in df['target_currency'].unique():
        last_rate_query = f"""
            SELECT exchange_rate 
            FROM exchange_rates 
            WHERE target_currency = '{currency}' 
            AND base_currency = 'USD'
            ORDER BY timestamp DESC 
            LIMIT 1
        """
        
        last_rate = postgres_hook.get_first(last_rate_query)
        if last_rate:
            current_rate = df[df['target_currency'] == currency]['exchange_rate'].iloc[0]
            change_pct = abs((current_rate - last_rate[0]) / last_rate[0] * 100)
            
            if change_pct > 10:
                print(f"ALERT: {currency} rate changed by {change_pct:.2f}%")
    
    # Guardar datos procesados
    processed_file = f"/tmp/processed_rates_{context['ds']}.csv"
    df.to_csv(processed_file, index=False)
    
    return processed_file

# Tareas del DAG
extract_rates = PythonOperator(
    task_id='extract_rates',
    python_callable=extract_exchange_rates,
    dag=dag
)

process_rates = PythonOperator(
    task_id='process_rates',
    python_callable=process_exchange_data,
    dag=dag
)

# Cargar datos a PostgreSQL
load_to_db = PostgresOperator(
    task_id='load_to_database',
    postgres_conn_id='postgres_warehouse',
    sql="""
        INSERT INTO exchange_rates 
        (base_currency, target_currency, exchange_rate, timestamp, source, extraction_date)
        SELECT 
            base_currency,
            target_currency,
            exchange_rate,
            TO_TIMESTAMP(timestamp),
            source,
            TO_DATE(extraction_date, 'YYYY-MM-DD')
        FROM temp_exchange_staging
        ON CONFLICT (base_currency, target_currency, DATE(timestamp)) 
        DO UPDATE SET 
            exchange_rate = EXCLUDED.exchange_rate,
            timestamp = EXCLUDED.timestamp;
    """,
    dag=dag
)

# Definir dependencias
extract_rates >> process_rates >> load_to_db
```

**Ingerir datos meteorológicos en tiempo real para análisis predictivo**:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import requests
import json
import boto3

default_args = {
    'owner': 'weather-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline de datos meteorológicos en tiempo real',
    schedule_interval=timedelta(minutes=15),
    catchup=False
)

def fetch_weather_data(**context):
    """Extrae datos meteorológicos de múltiples APIs"""
    
    # Lista de ciudades para monitorear
    cities = [
        {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
        {'name': 'London', 'lat': 51.5074, 'lon': -0.1278},
        {'name': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503},
        {'name': 'Sydney', 'lat': -33.8688, 'lon': 151.2093}
    ]
    
    weather_data = []
    api_key = "{{ var.value.openweather_api_key }}"
    
    for city in cities:
        try:
            # Datos meteorológicos actuales
            current_url = f"https://api.openweathermap.org/data/2.5/weather"
            current_params = {
                'lat': city['lat'],
                'lon': city['lon'],
                'appid': api_key,
                'units': 'metric'
            }
            
            current_response = requests.get(current_url, params=current_params)
            current_response.raise_for_status()
            current_data = current_response.json()
            
            # Pronóstico extendido
            forecast_url = f"https://api.openweathermap.org/data/2.5/forecast"
            forecast_response = requests.get(forecast_url, params=current_params)
            forecast_response.raise_for_status()
            forecast_data = forecast_response.json()
            
            # Estructurar datos actuales
            current_record = {
                'city_name': city['name'],
                'latitude': city['lat'],
                'longitude': city['lon'],
                'timestamp': context['ts'],
                'temperature': current_data['main']['temp'],
                'feels_like': current_data['main']['feels_like'],
                'humidity': current_data['main']['humidity'],
                'pressure': current_data['main']['pressure'],
                'wind_speed': current_data.get('wind', {}).get('speed', 0),
                'wind_direction': current_data.get('wind', {}).get('deg', 0),
                'cloud_coverage': current_data['clouds']['all'],
                'weather_condition': current_data['weather'][0]['main'],
                'weather_description': current_data['weather'][0]['description'],
                'visibility': current_data.get('visibility', 0),
                'data_type': 'current'
            }
            
            weather_data.append(current_record)
            
            # Procesar datos de pronóstico
            for forecast_item in forecast_data['list'][:8]:  # Próximas 24 horas
                forecast_record = {
                    'city_name': city['name'],
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'timestamp': forecast_item['dt_txt'],
                    'temperature': forecast_item['main']['temp'],
                    'feels_like': forecast_item['main']['feels_like'],
                    'humidity': forecast_item['main']['humidity'],
                    'pressure': forecast_item['main']['pressure'],
                    'wind_speed': forecast_item.get('wind', {}).get('speed', 0),
                    'wind_direction': forecast_item.get('wind', {}).get('deg', 0),
                    'cloud_coverage': forecast_item['clouds']['all'],
                    'weather_condition': forecast_item['weather'][0]['main'],
                    'weather_description': forecast_item['weather'][0]['description'],
                    'visibility': forecast_item.get('visibility', 0),
                    'data_type': 'forecast',
                    'precipitation_probability': forecast_item.get('pop', 0) * 100
                }
                
                weather_data.append(forecast_record)
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city['name']}: {str(e)}")
            continue
        except KeyError as e:
            print(f"Missing data field for {city['name']}: {str(e)}")
            continue
    
    # Guardar datos en S3 para procesamiento con Spark
    s3_client = boto3.client('s3')
    file_key = f"weather-data/raw/{context['ds']}/{context['ts']}/weather_data.json"
    
    s3_client.put_object(
        Bucket='weather-data-lake',
        Key=file_key,
        Body=json.dumps(weather_data, indent=2),
        ContentType='application/json'
    )
    
    return f"s3://weather-data-lake/{file_key}"

# Función para procesar datos con Spark
def create_spark_weather_analysis():
    """Script de Spark para análisis predictivo de datos meteorológicos"""
    
    spark_script = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# Configuración de Spark
spark = SparkSession.builder \\
    .appName("Weather_Predictive_Analytics") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Schema para datos meteorológicos
weather_schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("cloud_coverage", IntegerType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("data_type", StringType(), True),
    StructField("precipitation_probability", DoubleType(), True)
])

# Lectura de datos actuales y históricos
df_current = spark.read \\
    .schema(weather_schema) \\
    .json("s3://weather-data-lake/weather-data/raw/*/*/weather_data.json")

# Procesamiento temporal y feature engineering
df_processed = df_current \\
    .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \\
    .withColumn("hour", hour(col("timestamp_parsed"))) \\
    .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \\
    .withColumn("month", month(col("timestamp_parsed"))) \\
    .withColumn("season", 
                when(col("month").isin([12, 1, 2]), "winter")
                .when(col("month").isin([3, 4, 5]), "spring")
                .when(col("month").isin([6, 7, 8]), "summer")
                .otherwise("autumn")) \\
    .withColumn("temp_humidity_ratio", col("temperature") / col("humidity")) \\
    .withColumn("pressure_normalized", (col("pressure") - 1013.25) / 50) \\
    .withColumn("wind_pressure_interaction", col("wind_speed") * col("pressure_normalized"))

# Crear características de ventana temporal para tendencias
window_spec = Window.partitionBy("city_name").orderBy("timestamp_parsed")

df_with_trends = df_processed \\
    .withColumn("temp_lag_1h", lag("temperature", 1).over(window_spec)) \\
    .withColumn("temp_lag_3h", lag("temperature", 3).over(window_spec)) \\
    .withColumn("pressure_lag_1h", lag("pressure", 1).over(window_spec)) \\
    .withColumn("temp_change_1h", col("temperature") - col("temp_lag_1h")) \\
    .withColumn("temp_change_3h", col("temperature") - col("temp_lag_3h")) \\
    .withColumn("pressure_change_1h", col("pressure") - col("pressure_lag_1h"))

# Filtrar datos válidos para el modelo
df_model_ready = df_with_trends \\
    .filter(col("data_type") == "current") \\
    .filter(col("temp_lag_1h").isNotNull()) \\
    .filter(col("temperature").between(-50, 60)) \\
    .filter(col("humidity").between(0, 100))

# Preparar features para modelo predictivo
feature_cols = [
    "latitude", "longitude", "hour", "day_of_week", "month",
    "humidity", "pressure", "wind_speed", "wind_direction", 
    "cloud_coverage", "visibility", "temp_lag_1h", "temp_lag_3h",
    "pressure_lag_1h", "temp_change_1h", "pressure_change_1h",
    "temp_humidity_ratio", "pressure_normalized", "wind_pressure_interaction"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
rf = RandomForestRegressor(featuresCol="features", labelCol="temperature", numTrees=100)

# Pipeline de ML
pipeline = Pipeline(stages=[assembler, rf])

# División de datos para entrenamiento y validación
train_data, test_data = df_model_ready.randomSplit([0.8, 0.2], seed=42)

# Entrenar modelo
model = pipeline.fit(train_data)

# Predicciones y evaluación
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="temperature", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Square Error: {rmse}")

# Análisis de patrones climáticos
df_climate_patterns = df_processed \\
    .groupBy("city_name", "season", "hour") \\
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("pressure").alias("avg_pressure"),
        avg("wind_speed").alias("avg_wind_speed"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),
        stddev("temperature").alias("temp_volatility"),
        count("*").alias("observation_count")
    )

# Detección de eventos climáticos extremos
df_extreme_events = df_processed \\
    .withColumn("is_extreme_temp", 
                when((col("temperature") > 35) | (col("temperature") < -10), True)
                .otherwise(False)) \\
    .withColumn("is_high_wind", when(col("wind_speed") > 15, True).otherwise(False)) \\
    .withColumn("is_low_pressure", when(col("pressure") < 1000, True).otherwise(False)) \\
    .filter((col("is_extreme_temp") == True) | 
            (col("is_high_wind") == True) | 
            (col("is_low_pressure") == True))

# Predicciones futuras para las próximas 6 horas
df_for_prediction = df_processed \\
    .filter(col("data_type") == "current") \\
    .orderBy(desc("timestamp_parsed")) \\
    .limit(4)  # Una observación por ciudad

future_predictions = model.transform(df_for_prediction.select(*feature_cols + ["city_name", "timestamp_parsed"]))

# Guardar resultados
df_climate_patterns.write \\
    .partitionBy("city_name", "season") \\
    .mode("overwrite") \\
    .parquet("s3://weather-data-lake/analytics/climate_patterns/")

df_extreme_events.write \\
    .partitionBy("city_name") \\
    .mode("append") \\
    .parquet("s3://weather-data-lake/analytics/extreme_events/")

future_predictions.select("city_name", "timestamp_parsed", "prediction") \\
    .write \\
    .mode("overwrite") \\
    .json("s3://weather-data-lake/predictions/temperature_forecast/")

# Métricas del modelo para monitoreo
model_metrics = spark.createDataFrame([
    ("temperature_prediction_rmse", rmse, "{{ ds }}")
], ["metric_name", "metric_value", "date"])

model_metrics.write \\
    .mode("append") \\
    .parquet("s3://weather-data-lake/model_metrics/")

spark.stop()
"""
    
    # Guardar script en S3
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket='weather-data-lake',
        Key='scripts/weather_analysis.py',
        Body=spark_script,
        ContentType='text/plain'
    )
    
    return 's3://weather-data-lake/scripts/weather_analysis.py'

# Función para generar alertas basadas en predicciones
def generate_weather_alerts(**context):
    """Genera alertas basadas en condiciones meteorológicas extremas"""
    
    s3_client = boto3.client('s3')
    
    try:
        # Leer predicciones más recientes
        response = s3_client.get_object(
            Bucket='weather-data-lake',
            Key='predictions/temperature_forecast/part-00000-*.json'
        )
        
        predictions_data = json.loads(response['Body'].read())
        
        alerts = []
        for prediction in predictions_data:
            city = prediction['city_name']
            predicted_temp = prediction['prediction']
            
            # Generar alertas por temperatura extrema
            if predicted_temp > 40:
                alerts.append({
                    'city': city,
                    'alert_type': 'EXTREME_HEAT',
                    'severity': 'HIGH',
                    'predicted_temperature': predicted_temp,
                    'message': f'Temperatura extrema prevista en {city}: {predicted_temp:.1f}°C'
                })
            elif predicted_temp < -15:
                alerts.append({
                    'city': city,
                    'alert_type': 'EXTREME_COLD',
                    'severity': 'HIGH',
                    'predicted_temperature': predicted_temp,
                    'message': f'Temperatura extrema fría prevista en {city}: {predicted_temp:.1f}°C'
                })
        
        # Enviar alertas si existen
        if alerts:
            # Aquí se podría integrar con servicios de notificación
            # como SNS, Slack, email, etc.
            print(f"WEATHER ALERTS GENERATED: {len(alerts)} alerts")
            for alert in alerts:
                print(f"ALERT: {alert['message']}")
        
        return alerts
        
    except Exception as e:
        print(f"Error generating weather alerts: {str(e)}")
        return []

# Definición de tareas del DAG
fetch_weather = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

create_spark_script = PythonOperator(
    task_id='create_spark_script',
    python_callable=create_spark_weather_analysis,
    dag=dag
)

# Tarea de Spark para análisis predictivo
weather_analysis = SparkSubmitOperator(
    task_id='weather_predictive_analysis',
    application='s3://weather-data-lake/scripts/weather_analysis.py',
    conn_id='spark_cluster',
    conf={
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '4',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    },
    dag=dag
)

generate_alerts = PythonOperator(
    task_id='generate_weather_alerts',
    python_callable=generate_weather_alerts,
    dag=dag
)

# Definir dependencias del pipeline
fetch_weather >> create_spark_script >> weather_analysis >> generate_alerts
```

Este pipeline demuestra:

1. **Extracción de APIs múltiples**: Obtiene datos actuales y pronósticos de OpenWeatherMap
2. **Procesamiento distribuido**: Utiliza Spark para análisis a gran escala
3. **Machine Learning**: Implementa modelos predictivos con MLlib
4. **Feature Engineering**: Crea características temporales y combinadas
5. **Detección de anomalías**: Identifica eventos climáticos extremos
6. **Almacenamiento optimizado**: Usa particionamiento por ciudad y estación
7. **Sistema de alertas**: Genera notificaciones basadas en predicciones
8. **Monitoreo de modelos**: Rastrea métricas de rendimiento del ML

La arquitectura permite escalabilidad horizontal, procesamiento en tiempo real, y análisis predictivo avanzado para aplicaciones meteorológicas críticas.

### 3.2.2 Conectores Spark: JDBC, FileSource, Delta, etc.

Apache Spark proporciona una amplia gama de conectores nativos y de terceros que permiten la integración fluida con diversas fuentes de datos, desde bases de datos relacionales tradicionales hasta sistemas de almacenamiento distribuido modernos. Estos conectores abstraen la complejidad de acceso a datos y proporcionan APIs unificadas para lectura, escritura y procesamiento de datos a gran escala.

##### Conectores JDBC

Los conectores JDBC de Spark permiten establecer conexiones directas con bases de datos relacionales utilizando drivers estándar JDBC. Estos conectores soportan paralelización automática de consultas mediante particionado de datos, optimización de predicados (predicate pushdown) y gestión eficiente de conexiones para maximizar el rendimiento en entornos distribuidos.

**Aplicaciones**:

- Migración de datos desde sistemas legacy
- Integración con data warehouses tradicionales
- Sincronización incremental de datos transaccionales
- Ejecución de consultas analíticas distribuidas

**Conectar Spark a una base de datos MySQL para lectura de tablas normalizadas**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configuración de Spark con driver MySQL
spark = SparkSession.builder \
    .appName("MySQL_JDBC_Connector") \
    .config("spark.jars", "/path/to/mysql-connector-java-8.0.33.jar") \
    .getOrCreate()

# Configuración de conexión JDBC
jdbc_url = "jdbc:mysql://localhost:3306/ecommerce_db"
connection_properties = {
    "user": "spark_user",
    "password": "secure_password",
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchsize": "10000",  # Optimización de fetch
    "numPartitions": "8",   # Paralelización
    "partitionColumn": "customer_id",
    "lowerBound": "1",
    "upperBound": "1000000"
}

# Lectura de tabla completa
customers_df = spark.read \
    .jdbc(url=jdbc_url, 
          table="customers", 
          properties=connection_properties)

# Lectura con particionado personalizado
orders_df = spark.read \
    .jdbc(url=jdbc_url,
          table="orders",
          column="order_date",
          lowerBound="2023-01-01",
          upperBound="2024-12-31",
          numPartitions=12,  # Una partición por mes
          properties=connection_properties)

# Mostrar esquema y datos
customers_df.printSchema()
customers_df.show(20, truncate=False)
```

**Ejecutar una consulta SQL desde Spark para filtrar solo los datos necesarios**:

```python
# Consulta SQL personalizada con predicado pushdown
custom_query = """
(SELECT c.customer_id, c.customer_name, c.registration_date,
        o.order_id, o.order_date, o.total_amount,
        p.product_name, p.category
 FROM customers c 
 INNER JOIN orders o ON c.customer_id = o.customer_id
 INNER JOIN order_items oi ON o.order_id = oi.order_id
 INNER JOIN products p ON oi.product_id = p.product_id
 WHERE o.order_date >= '2024-01-01' 
   AND p.category IN ('Electronics', 'Books')
   AND o.total_amount > 100) AS filtered_data
"""

# Ejecución de consulta optimizada
filtered_df = spark.read \
    .jdbc(url=jdbc_url, 
          table=custom_query, 
          properties=connection_properties)

# Procesamiento adicional en Spark
result_df = filtered_df \
    .groupBy("customer_id", "customer_name", "category") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_spent"),
        max("order_date").alias("last_purchase_date")
    ) \
    .filter(col("total_orders") >= 3) \
    .orderBy(desc("total_spent"))

result_df.show()

# Escritura de resultados de vuelta a MySQL
result_df.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, 
          table="customer_analytics", 
          properties=connection_properties)
```

##### FileSource y formatos soportados

Spark FileSource es un conector unificado que proporciona acceso eficiente a sistemas de archivos distribuidos como HDFS, Amazon S3, Azure Data Lake Storage (ADLS), Google Cloud Storage, y sistemas de archivos locales. Soporta múltiples formatos de archivo optimizados para big data, incluyendo formatos columnares (Parquet, ORC), formatos de texto (CSV, JSON, XML) y formatos binarios personalizados.

**Características**:

- Particionado automático y manual
- Compresión transparente (gzip, snappy, lz4, brotli)
- Schema evolution y schema inference
- Predicate pushdown para formatos columnares
- Vectorización para mejor rendimiento

**Leer archivos Parquet de S3 con particiones por fecha**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuración optimizada para S3
spark = SparkSession.builder \
    .appName("S3_Parquet_Reader") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Ruta base con estructura particionada
s3_base_path = "s3a://data-lake-bucket/sales_data/year=*/month=*/day=*/"

# Lectura con filtros de partición (partition pruning)
sales_df = spark.read \
    .option("basePath", "s3a://data-lake-bucket/sales_data/") \
    .parquet(s3_base_path) \
    .filter(
        (col("year") == 2024) & 
        (col("month").isin([10, 11, 12])) &
        (col("day") >= 1)
    )

# Lectura con esquema específico para mejor rendimiento
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("timestamp", TimestampType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True)
])

optimized_sales_df = spark.read \
    .schema(schema) \
    .parquet("s3a://data-lake-bucket/sales_data/year=2024/month=12/")

# Análisis de particiones y estadísticas
print(f"Número de particiones: {optimized_sales_df.rdd.getNumPartitions()}")
print(f"Total de registros: {optimized_sales_df.count()}")

# Agregaciones optimizadas
daily_summary = optimized_sales_df \
    .withColumn("total_amount", col("quantity") * col("unit_price")) \
    .groupBy("year", "month", "day") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("total_amount").alias("daily_revenue"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("total_amount").alias("avg_transaction_value")
    )

daily_summary.show()
```

**Procesar archivos CSV diarios desde un directorio HDFS**:

```python
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Esquema explícito para archivos CSV
csv_schema = StructType([
    StructField("log_timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("response_code", IntegerType(), True),
    StructField("response_size", LongType(), True)
])

# Función para procesar archivos por rango de fechas
def process_daily_logs(start_date, end_date, hdfs_base_path):
    current_date = start_date
    all_files = []
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        daily_path = f"{hdfs_base_path}/date={date_str}/*.csv"
        all_files.append(daily_path)
        current_date += timedelta(days=1)
    
    return all_files

# Configuración para procesamiento de archivos grandes
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB
spark.conf.set("spark.sql.files.openCostInBytes", "8388608")     # 8MB

# Lectura de múltiples archivos CSV diarios
hdfs_path = "hdfs://namenode:9000/logs/web_access"
start_date = datetime(2024, 12, 1)
end_date = datetime(2024, 12, 7)

file_paths = process_daily_logs(start_date, end_date, hdfs_path)

# Lectura optimizada con múltiples opciones
web_logs_df = spark.read \
    .schema(csv_schema) \
    .option("header", "true") \
    .option("multiline", "false") \
    .option("escape", '"') \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv(file_paths)

# Limpieza y enriquecimiento de datos
cleaned_logs_df = web_logs_df \
    .filter(col("_corrupt_record").isNull()) \
    .withColumn("date", to_date(col("log_timestamp"))) \
    .withColumn("hour", hour(col("log_timestamp"))) \
    .withColumn("is_bot", when(col("user_agent").rlike("(?i)bot|crawler|spider"), True).otherwise(False)) \
    .withColumn("status_category", 
                when(col("response_code").between(200, 299), "Success")
                .when(col("response_code").between(400, 499), "Client_Error")
                .when(col("response_code").between(500, 599), "Server_Error")
                .otherwise("Other"))

# Análisis de patrones de tráfico
traffic_analysis = cleaned_logs_df \
    .filter(~col("is_bot")) \
    .groupBy("date", "hour", "status_category") \
    .agg(
        count("*").alias("request_count"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("ip_address").alias("unique_ips"),
        sum("response_size").alias("total_bytes"),
        avg("response_size").alias("avg_response_size")
    )

# Escritura de resultados procesados en formato Parquet particionado
traffic_analysis.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet("hdfs://namenode:9000/analytics/web_traffic_summary")

traffic_analysis.show(50)
```

##### Conector Delta Lake

Delta Lake es una capa de almacenamiento open-source que proporciona transacciones ACID, manejo de metadatos escalable y unificación de streaming y batch processing sobre data lakes. El conector Delta para Spark permite operaciones avanzadas como merge, time travel, schema evolution y optimizaciones automáticas, siendo ideal para arquitecturas de datos modernas que requieren consistencia y versionado.

**Capacidades**:

- Transacciones ACID completas
- Versionado automático con time travel
- Schema enforcement y evolution
- Optimización automática (Auto Optimize, Z-Ordering)
- Change Data Feed (CDF) para captura de cambios
- Vacuum para limpieza de archivos obsoletos

**Ingerir datos en formato Delta y aplicar `MERGE INTO` para deduplicación**:

```python
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configuración de Spark con Delta Lake
builder = SparkSession.builder \
    .appName("Delta_Lake_Upsert") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.autoCompact.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Ruta de la tabla Delta
delta_table_path = "/delta/customer_profiles"

# Datos iniciales (simulando carga inicial)
initial_data = [
    (1, "John Doe", "john.doe@email.com", "2024-01-15", "Premium", 5000.0),
    (2, "Jane Smith", "jane.smith@email.com", "2024-01-20", "Standard", 2500.0),
    (3, "Bob Johnson", "bob.johnson@email.com", "2024-02-01", "Premium", 7500.0)
]

columns = ["customer_id", "name", "email", "registration_date", "tier", "lifetime_value"]
initial_df = spark.createDataFrame(initial_data, columns)

# Creación de tabla Delta inicial
initial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(delta_table_path)

# Creación de DeltaTable para operaciones avanzadas
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Datos nuevos y actualizados (simulando ingesta incremental)
new_data = [
    (2, "Jane Smith-Wilson", "jane.wilson@email.com", "2024-01-20", "Premium", 3500.0),  # Actualización
    (4, "Alice Brown", "alice.brown@email.com", "2024-03-10", "Standard", 1800.0),       # Nuevo
    (5, "Charlie Davis", "charlie.davis@email.com", "2024-03-15", "Premium", 9200.0),   # Nuevo
    (1, "John Doe", "john.doe@newemail.com", "2024-01-15", "Premium", 5500.0)           # Actualización email
]

updates_df = spark.createDataFrame(new_data, columns) \
    .withColumn("last_updated", current_timestamp())

# Operación MERGE con lógica de deduplicación
delta_table.alias("target") \
    .merge(
        updates_df.alias("source"),
        "target.customer_id = source.customer_id"
    ) \
    .whenMatchedUpdate(set={
        "name": "source.name",
        "email": "source.email",
        "tier": "source.tier",
        "lifetime_value": "source.lifetime_value",
        "last_updated": "source.last_updated"
    }) \
    .whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "name": "source.name",
        "email": "source.email",
        "registration_date": "source.registration_date",
        "tier": "source.tier",
        "lifetime_value": "source.lifetime_value",
        "last_updated": "source.last_updated"
    }) \
    .execute()

# Verificación de resultados
print("Estado actual de la tabla:")
spark.read.format("delta").load(delta_table_path).show()

# MERGE avanzado con condiciones complejas
complex_updates = [
    (1, "John Doe Sr.", "john.doe.sr@email.com", "2024-01-15", "Platinum", 12000.0),
    (6, "Diana Prince", "diana.prince@email.com", "2024-04-01", "Premium", 8500.0),
    (2, "Jane Smith-Wilson", "jane.wilson@email.com", "2024-01-20", "Standard", 2800.0)  # Downgrade
]

complex_df = spark.createDataFrame(complex_updates, columns) \
    .withColumn("last_updated", current_timestamp())

# MERGE con condiciones de negocio
delta_table.alias("target") \
    .merge(
        complex_df.alias("source"),
        "target.customer_id = source.customer_id"
    ) \
    .whenMatchedUpdate(
        condition="source.lifetime_value > target.lifetime_value OR source.tier != target.tier",
        set={
            "name": "source.name",
            "email": "source.email",
            "tier": "source.tier",
            "lifetime_value": "greatest(target.lifetime_value, source.lifetime_value)",
            "last_updated": "source.last_updated"
        }
    ) \
    .whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "name": "source.name",
        "email": "source.email",
        "registration_date": "source.registration_date",
        "tier": "source.tier",
        "lifetime_value": "source.lifetime_value",
        "last_updated": "source.last_updated"
    }) \
    .execute()

print("Estado después del MERGE condicional:")
spark.read.format("delta").load(delta_table_path).show()
```

**Mantener versiones de datasets para auditoría**:

```python
# Consulta del historial de versiones
print("Historial de versiones de la tabla:")
delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Time Travel - consultar versión específica
version_1_df = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .load(delta_table_path)

print("Estado en versión 1:")
version_1_df.show()

# Time Travel por timestamp
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
timestamp_query = spark.read \
    .format("delta") \
    .option("timestampAsOf", yesterday.strftime("%Y-%m-%d %H:%M:%S")) \
    .load(delta_table_path)

# Auditoría de cambios entre versiones
current_version = spark.read.format("delta").load(delta_table_path)
previous_version = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)

# Detectar registros modificados
changes_audit = current_version.alias("current") \
    .join(previous_version.alias("previous"), 
          col("current.customer_id") == col("previous.customer_id"), 
          "full_outer") \
    .select(
        coalesce(col("current.customer_id"), col("previous.customer_id")).alias("customer_id"),
        when(col("previous.customer_id").isNull(), "INSERTED")
        .when(col("current.customer_id").isNull(), "DELETED")
        .when(col("current.name") != col("previous.name") |
              col("current.email") != col("previous.email") |
              col("current.tier") != col("previous.tier") |
              col("current.lifetime_value") != col("previous.lifetime_value"), "UPDATED")
        .otherwise("UNCHANGED").alias("change_type"),
        col("current.name").alias("current_name"),
        col("previous.name").alias("previous_name"),
        col("current.email").alias("current_email"),
        col("previous.email").alias("previous_email")
    ) \
    .filter(col("change_type") != "UNCHANGED")

print("Auditoría de cambios:")
changes_audit.show(truncate=False)

# Configuración de retención y optimización
# Vacuum para eliminar archivos antiguos (cuidado en producción)
delta_table.vacuum(retentionHours=168)  # 7 días

# Optimize con Z-Ordering para mejor rendimiento de consultas
spark.sql(f"OPTIMIZE delta.`{delta_table_path}` ZORDER BY (customer_id, tier)")

# Habilitar Change Data Feed para captura de cambios
spark.sql(f"ALTER TABLE delta.`{delta_table_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Consultar cambios usando CDF
changes_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(delta_table_path)

print("Change Data Feed:")
changes_df.select("customer_id", "name", "_change_type", "_commit_version", "_commit_timestamp").show()

# Metadatos y estadísticas de la tabla
print("Detalles de la tabla Delta:")
spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`").show(truncate=False)

# Estadísticas de archivos
print("Estadísticas de archivos:")
print(f"Número de archivos: {len(delta_table.detail().collect()[0]['numFiles'])}")
print(f"Tamaño total: {delta_table.detail().collect()[0]['sizeInBytes']} bytes")
```

### 3.2.3 Airflow: uso de Hooks y Connections

Apache Airflow proporciona mecanismos reutilizables y seguros para conectarse de forma modular a distintas fuentes de datos y servicios externos. Esta arquitectura permite mantener las credenciales separadas del código y facilita la reutilización de conexiones a través de múltiples DAGs y tareas.

##### Hooks en Airflow

Los Hooks son interfaces de bajo nivel que proporcionan una capa de abstracción para acceder a sistemas externos como bases de datos, APIs REST, servicios de almacenamiento en la nube (S3, GCS), y otros servicios. Cada operador en Airflow utiliza internamente un Hook específico para realizar sus operaciones, lo que permite encapsular la lógica de conexión y las operaciones específicas del sistema.

| Ventajas |
|:--|
|**Reutilización**: Un mismo Hook puede ser usado por múltiples operadores y tareas|
|**Abstracción**: Ocultan la complejidad de las conexiones y protocolos específicos|
|**Consistencia**: Proporcionan una interfaz uniforme para sistemas similares|
|**Mantenibilidad**: Centralizan la lógica de conexión en un solo lugar|

**Usar `PostgresHook` para ejecutar una consulta y pasar los resultados a otra tarea**:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
import pandas as pd

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def extract_sales_data(**context):
    """Extrae datos de ventas desde PostgreSQL y los pasa a la siguiente tarea"""
    
    # Inicializar el Hook de PostgreSQL usando la conexión configurada
    postgres_hook = PostgresHook(postgres_conn_id='postgres_sales_db')
    
    # Ejecutar consulta SQL
    sql_query = """
    SELECT 
        DATE(sale_date) as fecha,
        SUM(amount) as total_ventas,
        COUNT(*) as num_transacciones,
        AVG(amount) as promedio_venta
    FROM sales 
    WHERE sale_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(sale_date)
    ORDER BY fecha DESC;
    """
    
    # Obtener resultados como DataFrame de pandas
    df = postgres_hook.get_pandas_df(sql_query)
    
    # Convertir a diccionario para pasar entre tareas
    sales_data = df.to_dict('records')
    
    # Guardar en XCom para la siguiente tarea
    context['task_instance'].xcom_push(key='weekly_sales', value=sales_data)
    
    print(f"Extraídos {len(sales_data)} registros de ventas")
    return sales_data

def process_sales_report(**context):
    """Procesa los datos de ventas y genera un reporte"""
    
    # Obtener datos de la tarea anterior
    sales_data = context['task_instance'].xcom_pull(
        task_ids='extract_sales_data', 
        key='weekly_sales'
    )
    
    if not sales_data:
        raise ValueError("No se recibieron datos de ventas")
    
    # Procesar datos
    total_sales = sum([record['total_ventas'] for record in sales_data])
    avg_daily_sales = total_sales / len(sales_data)
    
    # Generar reporte
    report = f"""
    Reporte Semanal de Ventas:
    - Total de ventas: ${total_sales:,.2f}
    - Promedio diario: ${avg_daily_sales:,.2f}
    - Días analizados: {len(sales_data)}
    """
    
    print(report)
    context['task_instance'].xcom_push(key='sales_report', value=report)
    
    return report

# Definir el DAG
dag = DAG(
    'sales_reporting_dag',
    default_args=default_args,
    description='Pipeline de reporte de ventas usando PostgresHook',
    schedule_interval='@daily',
    catchup=False
)

# Definir tareas
extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_sales_report',
    python_callable=process_sales_report,
    dag=dag
)

# Definir dependencias
extract_task >> process_task
```

**Usar `HttpHook` para llamar a una API y procesar su JSON**:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.postgres import PostgresOperator
import json

default_args = {
    'owner': 'api-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

def fetch_weather_data(**context):
    """Obtiene datos meteorológicos desde una API externa"""
    
    # Inicializar HttpHook con la conexión configurada
    http_hook = HttpHook(
        method='GET',
        http_conn_id='weather_api_conn'  # Conexión configurada en Airflow UI
    )
    
    # Parámetros para la API
    endpoint = '/current'
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Airflow-Weather-Pipeline/1.0'
    }
    
    # Obtener ciudad desde variables de Airflow
    from airflow.models import Variable
    city = Variable.get("weather_city", default_var="Cali")
    
    data = {
        'q': city,
        'units': 'metric',
        'lang': 'es'
    }
    
    try:
        # Realizar petición HTTP
        response = http_hook.run(
            endpoint=endpoint,
            headers=headers,
            data=data
        )
        
        # Procesar respuesta JSON
        weather_data = json.loads(response.content)
        
        # Extraer información relevante
        processed_data = {
            'timestamp': datetime.now().isoformat(),
            'ciudad': weather_data.get('name'),
            'temperatura': weather_data['main']['temp'],
            'sensacion_termica': weather_data['main']['feels_like'],
            'humedad': weather_data['main']['humidity'],
            'presion': weather_data['main']['pressure'],
            'descripcion': weather_data['weather'][0]['description'],
            'visibilidad': weather_data.get('visibility', 0) / 1000,  # km
            'viento_velocidad': weather_data.get('wind', {}).get('speed', 0)
        }
        
        print(f"Datos meteorológicos obtenidos para {processed_data['ciudad']}")
        print(f"Temperatura: {processed_data['temperatura']}°C")
        
        # Guardar en XCom
        context['task_instance'].xcom_push(
            key='weather_data', 
            value=processed_data
        )
        
        return processed_data
        
    except Exception as e:
        print(f"Error al obtener datos meteorológicos: {str(e)}")
        raise

def validate_and_transform_weather(**context):
    """Valida y transforma los datos meteorológicos"""
    
    # Obtener datos de la tarea anterior
    weather_data = context['task_instance'].xcom_pull(
        task_ids='fetch_weather_data',
        key='weather_data'
    )
    
    if not weather_data:
        raise ValueError("No se recibieron datos meteorológicos")
    
    # Validaciones
    if weather_data['temperatura'] < -50 or weather_data['temperatura'] > 60:
        raise ValueError(f"Temperatura fuera de rango: {weather_data['temperatura']}°C")
    
    if weather_data['humedad'] < 0 or weather_data['humedad'] > 100:
        raise ValueError(f"Humedad fuera de rango: {weather_data['humedad']}%")
    
    # Transformaciones adicionales
    weather_data['categoria_temperatura'] = (
        'Muy Frío' if weather_data['temperatura'] < 10 else
        'Frío' if weather_data['temperatura'] < 18 else
        'Templado' if weather_data['temperatura'] < 25 else
        'Cálido' if weather_data['temperatura'] < 30 else
        'Muy Cálido'
    )
    
    weather_data['indice_confort'] = (
        weather_data['temperatura'] - 
        (weather_data['humedad'] / 100) * 2 -
        weather_data['viento_velocidad'] * 0.5
    )
    
    print(f"Datos validados y transformados exitosamente")
    print(f"Categoría de temperatura: {weather_data['categoria_temperatura']}")
    print(f"Índice de confort: {weather_data['indice_confort']:.1f}")
    
    return weather_data

def store_weather_data(**context):
    """Almacena los datos procesados en la base de datos"""
    
    weather_data = context['task_instance'].xcom_pull(
        task_ids='validate_and_transform_weather'
    )
    
    # Aquí normalmente insertarías en la base de datos
    # usando otro Hook como PostgresHook
    print("Datos meteorológicos almacenados exitosamente")
    print(json.dumps(weather_data, indent=2, ensure_ascii=False))

# Definir el DAG
dag = DAG(
    'weather_api_pipeline',
    default_args=default_args,
    description='Pipeline para consumir API meteorológica usando HttpHook',
    schedule_interval=timedelta(hours=3),
    catchup=False,
    tags=['api', 'weather', 'http']
)

# Definir tareas
fetch_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_and_transform_weather',
    python_callable=validate_and_transform_weather,
    dag=dag
)

store_task = PythonOperator(
    task_id='store_weather_data',
    python_callable=store_weather_data,
    dag=dag
)

# Definir dependencias
fetch_task >> validate_task >> store_task
```

##### Connections y Variables

Las Connections y Variables de Airflow proporcionan un mecanismo seguro y centralizado para gestionar credenciales, configuraciones y parámetros sin exponerlos directamente en el código del DAG. Esto mejora la seguridad, facilita el mantenimiento y permite diferentes configuraciones entre entornos (desarrollo, testing, producción).

**Connections** almacenan información de conexión completa (host, puerto, usuario, contraseña, esquemas) mientras que **Variables** almacenan valores individuales que pueden ser utilizados a través de múltiples DAGs.

**Definir una conexión S3 en Airflow UI y usarla desde un `S3Hook`**:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import pandas as pd
import io

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

def upload_data_to_s3(**context):
    """Sube datos procesados a Amazon S3 usando conexión configurada"""
    
    # Inicializar S3Hook usando la conexión 'aws_s3_conn' definida en Airflow UI
    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    
    # Obtener configuración desde Variables de Airflow
    bucket_name = Variable.get("s3_data_bucket", default_var="mi-bucket-datos")
    s3_prefix = Variable.get("s3_data_prefix", default_var="processed-data")
    
    # Generar datos de ejemplo (normalmente vendrían de una tarea anterior)
    sample_data = {
        'fecha': pd.date_range('2024-01-01', periods=100, freq='D'),
        'ventas': pd.Series(range(100)) * 150.5,
        'region': ['Norte', 'Sur', 'Este', 'Oeste'] * 25,
        'producto': ['A', 'B', 'C'] * 33 + ['A']
    }
    df = pd.DataFrame(sample_data)
    
    # Convertir DataFrame a CSV en memoria
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_content = csv_buffer.getvalue()
    
    # Generar nombre de archivo con timestamp
    timestamp = context['execution_date'].strftime('%Y%m%d_%H%M%S')
    s3_key = f"{s3_prefix}/ventas_diarias_{timestamp}.csv"
    
    try:
        # Subir archivo a S3
        s3_hook.load_string(
            string_data=csv_content,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
            content_type='text/csv'
        )
        
        print(f"Archivo subido exitosamente a S3:")
        print(f"Bucket: {bucket_name}")
        print(f"Key: {s3_key}")
        print(f"Registros: {len(df)}")
        
        # Verificar que el archivo existe
        if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
            print("✓ Verificación exitosa: El archivo existe en S3")
        else:
            raise Exception("Error: El archivo no se encontró en S3 después de la subida")
        
        # Guardar información del archivo en XCom
        file_info = {
            'bucket': bucket_name,
            's3_key': s3_key,
            'size_bytes': len(csv_content.encode('utf-8')),
            'records_count': len(df),
            'upload_timestamp': timestamp
        }
        
        context['task_instance'].xcom_push(key='s3_file_info', value=file_info)
        
        return file_info
        
    except Exception as e:
        print(f"Error al subir archivo a S3: {str(e)}")
        raise

def process_s3_files(**context):
    """Lee y procesa archivos desde S3"""
    
    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    bucket_name = Variable.get("s3_data_bucket")
    s3_prefix = Variable.get("s3_data_prefix")
    
    # Listar archivos en el bucket con el prefijo especificado
    file_keys = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=s3_prefix
    )
    
    print(f"Archivos encontrados en S3: {len(file_keys)}")
    
    # Procesar el archivo más reciente
    if file_keys:
        latest_file = sorted(file_keys)[-1]
        
        # Leer archivo desde S3
        file_content = s3_hook.read_key(
            key=latest_file,
            bucket_name=bucket_name
        )
        
        # Convertir a DataFrame
        df = pd.read_csv(io.StringIO(file_content))
        
        # Realizar análisis básico
        analysis = {
            'total_records': len(df),
            'total_sales': df['ventas'].sum(),
            'avg_sales': df['ventas'].mean(),
            'unique_regions': df['region'].nunique(),
            'date_range': f"{df['fecha'].min()} to {df['fecha'].max()}"
        }
        
        print("Análisis del archivo procesado:")
        for key, value in analysis.items():
            print(f"  {key}: {value}")
        
        return analysis
    
    else:
        print("No se encontraron archivos para procesar")
        return {}

# Definir el DAG
dag = DAG(
    's3_data_pipeline',
    default_args=default_args,
    description='Pipeline de datos usando S3Hook y conexiones configuradas',
    schedule_interval='@daily',
    catchup=False,
    tags=['s3', 'aws', 'data-pipeline']
)

# Definir tareas
upload_task = PythonOperator(
    task_id='upload_data_to_s3',
    python_callable=upload_data_to_s3,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_s3_files',
    python_callable=process_s3_files,
    dag=dag
)

# Definir dependencias
upload_task >> process_task
```

**Almacenar claves API en variables cifradas para ser usadas desde el DAG**:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import hashlib

default_args = {
    'owner': 'api-integration-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_secure_api_data(**context):
    """Consume API externa usando claves cifradas almacenadas en Variables"""
    
    # Obtener claves API cifradas desde Variables de Airflow
    # Estas variables se configuran en Airflow UI marcadas como "encrypt"
    api_key = Variable.get("external_api_key")  # Variable cifrada
    api_secret = Variable.get("external_api_secret")  # Variable cifrada
    
    # Obtener configuraciones no sensibles
    api_base_url = Variable.get("external_api_base_url", default_var="https://api.example.com")
    max_records = int(Variable.get("api_max_records", default_var="1000"))
    
    # Generar token de autenticación (ejemplo con HMAC)
    timestamp = str(int(datetime.now().timestamp()))
    message = f"{api_key}{timestamp}"
    signature = hashlib.hmac(
        api_secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    
    # Configurar headers de autenticación
    headers = {
        'Authorization': f'Bearer {api_key}',
        'X-API-Timestamp': timestamp,
        'X-API-Signature': signature,
        'Content-Type': 'application/json',
        'User-Agent': 'Airflow-Data-Pipeline/2.0'
    }
    
    # Parámetros de la consulta
    params = {
        'limit': max_records,
        'format': 'json',
        'date_from': context['execution_date'].strftime('%Y-%m-%d'),
        'include_metadata': True
    }
    
    try:
        # Realizar petición usando requests directamente para mayor control
        response = requests.get(
            f"{api_base_url}/v1/data",
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()  # Lanza excepción si status != 2xx
        
        data = response.json()
        
        # Validar respuesta
        if 'error' in data:
            raise Exception(f"Error en API: {data['error']}")
        
        records = data.get('records', [])
        metadata = data.get('metadata', {})
        
        print(f"Datos obtenidos exitosamente:")
        print(f"  Registros: {len(records)}")
        print(f"  Página: {metadata.get('page', 1)}")
        print(f"  Total disponible: {metadata.get('total_count', 'N/A')}")
        
        # Procesar y limpiar datos
        processed_records = []
        for record in records:
            processed_record = {
                'id': record.get('id'),
                'timestamp': record.get('created_at'),
                'value': float(record.get('value', 0)),
                'category': record.get('category', 'unknown'),
                'status': record.get('status', 'active'),
                'metadata': json.dumps(record.get('additional_data', {}))
            }
            processed_records.append(processed_record)
        
        # Guardar en XCom para tareas posteriores
        context['task_instance'].xcom_push(
            key='api_data',
            value=processed_records
        )
        
        context['task_instance'].xcom_push(
            key='api_metadata',
            value=metadata
        )
        
        return {
            'records_count': len(processed_records),
            'api_response_time': response.elapsed.total_seconds(),
            'status_code': response.status_code
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión con la API: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error al decodificar respuesta JSON: {str(e)}")
        raise
    except Exception as e:
        print(f"Error general al consumir API: {str(e)}")
        raise

def validate_and_enrich_data(**context):
    """Valida y enriquece los datos obtenidos de la API"""
    
    # Obtener datos de la tarea anterior
    api_data = context['task_instance'].xcom_pull(
        task_ids='fetch_secure_api_data',
        key='api_data'
    )
    
    if not api_data:
        raise ValueError("No se recibieron datos de la API")
    
    # Obtener configuraciones de validación desde Variables
    min_value = float(Variable.get("validation_min_value", default_var="0"))
    max_value = float(Variable.get("validation_max_value", default_var="10000"))
    valid_categories = Variable.get("valid_categories", default_var="A,B,C").split(',')
    
    validated_records = []
    validation_errors = []
    
    for i, record in enumerate(api_data):
        errors = []
        
        # Validaciones
        if record['value'] < min_value or record['value'] > max_value:
            errors.append(f"Valor fuera de rango: {record['value']}")
        
        if record['category'] not in valid_categories:
            errors.append(f"Categoría inválida: {record['category']}")
        
        if not record['id']:
            errors.append("ID faltante")
        
        # Si hay errores, registrar y omitir registro
        if errors:
            validation_errors.append({
                'record_index': i,
                'record_id': record.get('id', 'N/A'),
                'errors': errors
            })
            continue
        
        # Enriquecer datos válidos
        enriched_record = record.copy()
        enriched_record['validation_timestamp'] = datetime.now().isoformat()
        enriched_record['value_category'] = (
            'low' if record['value'] < 100 else
            'medium' if record['value'] < 1000 else
            'high'
        )
        enriched_record['processing_batch'] = context['run_id']
        
        validated_records.append(enriched_record)
    
    print(f"Validación completada:")
    print(f"  Registros válidos: {len(validated_records)}")
    print(f"  Registros con errores: {len(validation_errors)}")
    
    if validation_errors:
        print("Errores de validación encontrados:")
        for error in validation_errors[:5]:  # Mostrar solo los primeros 5
            print(f"  Record {error['record_index']}: {', '.join(error['errors'])}")
    
    # Guardar resultados
    context['task_instance'].xcom_push(
        key='validated_data',
        value=validated_records
    )
    
    context['task_instance'].xcom_push(
        key='validation_errors',
        value=validation_errors
    )
    
    return {
        'valid_records': len(validated_records),
        'invalid_records': len(validation_errors),
        'validation_success_rate': len(validated_records) / len(api_data) * 100
    }

def store_processed_data(**context):
    """Almacena los datos procesados en la base de datos usando conexión configurada"""
    
    # Obtener datos validados
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_and_enrich_data',
        key='validated_data'
    )
    
    if not validated_data:
        print("No hay datos válidos para almacenar")
        return
    
    # Usar PostgresHook con conexión configurada
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data_warehouse')
    
    # Obtener configuración de tabla desde Variables
    target_table = Variable.get("target_table_name", default_var="api_data")
    
    try:
        # Preparar datos para inserción masiva
        insert_sql = f"""
        INSERT INTO {target_table} 
        (external_id, timestamp, value, category, status, metadata, 
         validation_timestamp, value_category, processing_batch)
        VALUES %s
        ON CONFLICT (external_id) DO UPDATE SET
            value = EXCLUDED.value,
            category = EXCLUDED.category,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata,
            validation_timestamp = EXCLUDED.validation_timestamp,
            value_category = EXCLUDED.value_category,
            processing_batch = EXCLUDED.processing_batch;
        """
        
        # Preparar tuplas de datos
        data_tuples = []
        for record in validated_data:
            data_tuple = (
                record['id'],
                record['timestamp'],
                record['value'],
                record['category'],
                record['status'],
                record['metadata'],
                record['validation_timestamp'],
                record['value_category'],
                record['processing_batch']
            )
            data_tuples.append(data_tuple)
        
        # Ejecutar inserción masiva usando extras de psycopg2
        from psycopg2.extras import execute_values
        
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_sql, data_tuples)
                conn.commit()
        
        print(f"Almacenados {len(data_tuples)} registros en {target_table}")
        
        # Registrar estadísticas
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_records,
            AVG(value) as avg_value,
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp
        FROM {target_table}
        WHERE processing_batch = %s;
        """
        
        stats = postgres_hook.get_first(stats_sql, parameters=[context['run_id']])
        
        print("Estadísticas del lote procesado:")
        print(f"  Total de registros: {stats[0]}")
        print(f"  Valor promedio: {stats[1]:.2f}")
        print(f"  Rango temporal: {stats[2]} - {stats[3]}")
        
        return {
            'records_stored': len(data_tuples),
            'table_name': target_table,
            'batch_id': context['run_id']
        }
        
    except Exception as e:
        print(f"Error al almacenar datos: {str(e)}")
        raise

# Definir el DAG
dag = DAG(
    'secure_api_integration',
    default_args=default_args,
    description='Pipeline seguro para integración de API externa con variables cifradas',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['api', 'security', 'encryption', 'variables']
)

# Definir tareas
fetch_task = PythonOperator(
    task_id='fetch_secure_api_data',
    python_callable=fetch_secure_api_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_and_enrich_data',
    python_callable=validate_and_enrich_data,
    dag=dag
)

store_task = PythonOperator(
    task_id='store_processed_data',
    python_callable=store_processed_data,
    dag=dag
)

# Definir dependencias
fetch_task >> validate_task >> store_task
```

**Configuración de Connections y Variables en Airflow UI**:

Para que estos ejemplos funcionen correctamente, es necesario configurar las siguientes conexiones y variables en la interfaz de Airflow:

**Connections (Admin → Connections)**:

- `postgres_sales_db`: Conexión PostgreSQL para base de datos de ventas
- `weather_api_conn`: Conexión HTTP para API meteorológica
- `aws_s3_conn`: Conexión AWS para acceso a S3
- `postgres_data_warehouse`: Conexión PostgreSQL para almacén de datos

**Variables (Admin → Variables)**:

- `weather_city`: Ciudad para datos meteorológicos
- `s3_data_bucket`: Nombre del bucket S3
- `s3_data_prefix`: Prefijo para archivos en S3
- `external_api_key`: Clave API (marcada como cifrada)
- `external_api_secret`: Secreto API (marcada como cifrada)
- `validation_min_value`: Valor mínimo para validación
- `validation_max_value`: Valor máximo para validación
- `valid_categories`: Categorías válidas separadas por coma
- `target_table_name`: Nombre de la tabla destino

##### Mejores Prácticas para Hooks y Connections

**Gestión de Conexiones**:

1. **Separación por entorno**: Utiliza diferentes conexiones para desarrollo, testing y producción
2. **Nomenclatura consistente**: Usa un patrón claro como `{env}_{service}_{purpose}_conn`
3. **Principio de menor privilegio**: Configura conexiones con los permisos mínimos necesarios
4. **Rotación de credenciales**: Implementa un proceso regular de rotación de passwords y tokens
5. **Monitoreo**: Registra y monitorea el uso de conexiones para detectar anomalías

**Gestión de Variables**:

1. **Cifrado de datos sensibles**: Siempre marca como "encrypt" las variables que contienen información sensible
2. **Versionado**: Mantén un registro de cambios en variables críticas
3. **Valores por defecto**: Proporciona valores por defecto sensatos para variables opcionales
4. **Documentación**: Documenta el propósito y formato esperado de cada variable
5. **Validación**: Implementa validación de formato y rango para variables críticas

**Optimización de Rendimiento**:

1. **Reutilización de conexiones**: Los Hooks automáticamente reutilizan conexiones dentro de una tarea
2. **Pool de conexiones**: Configura pools de conexiones para sistemas de alta concurrencia
3. **Timeouts apropiados**: Establece timeouts adecuados para evitar tareas colgadas
4. **Batch operations**: Prefiere operaciones por lotes sobre múltiples operaciones individuales
5. **Limpieza de recursos**: Asegúrate de que las conexiones se cierren adecuadamente

**Manejo de Errores y Reintentos**:

1. **Excepciones específicas**: Captura y maneja excepciones específicas según el tipo de error
2. **Logging detallado**: Registra información suficiente para diagnosticar problemas
3. **Reintentos inteligentes**: Configura diferentes estrategias de reintento según el tipo de falla
4. **Alertas apropiadas**: Configura alertas para fallos críticos vs. fallos recuperables
5. **Fallback mechanisms**: Implementa mecanismos de respaldo cuando sea posible

**Seguridad**:

1. **Validación de entrada**: Siempre valida y sanitiza datos de entrada
2. **Auditoría**: Mantén logs de auditoría para accesos a sistemas sensibles
3. **Cifrado en tránsito**: Usa siempre conexiones cifradas (HTTPS, SSL/TLS)
4. **Segregación de redes**: Utiliza redes privadas para comunicación entre servicios
5. **Secretos externos**: Considera usar servicios externos de gestión de secretos (AWS Secrets Manager, HashiCorp Vault)

##### Ejemplo Avanzado: Pipeline Completo con Múltiples Hooks

Este ejemplo avanzado demuestra cómo integrar múltiples Hooks y Connections en un pipeline completo de datos, incluyendo manejo de errores, notificaciones, y mejores prácticas de producción.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pandas as pd
import json
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-platform-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # Usaremos Slack en su lugar
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: send_failure_notification(context)
}

def send_failure_notification(context):
    """Envía notificación de fallo a Slack"""
    try:
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_webhook_conn',
            webhook_token=Variable.get('slack_webhook_token')
        )
        
        message = f"""
        *Fallo en Pipeline de Datos*
        
        *DAG:* {context['dag'].dag_id}
        *Tarea:* {context['task'].task_id}
        *Fecha de ejecución:* {context['execution_date']}
        *Error:* {context.get('exception', 'Error desconocido')}
        
        Por favor revisar los logs para más detalles.
        """
        
        slack_hook.send_text(message)
        
    except Exception as e:
        logger.error(f"Error enviando notificación a Slack: {str(e)}")

def extract_from_multiple_sources(**context):
    """Extrae datos de múltiples fuentes usando diferentes Hooks"""
    
    results = {}
    
    # 1. Extraer desde PostgreSQL
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_source_db')
        
        sql_query = """
        SELECT 
            id, 
            created_date,
            amount,
            customer_id,
            product_category
        FROM transactions 
        WHERE DATE(created_date) = CURRENT_DATE - INTERVAL '1 day'
        """
        
        transactions_df = postgres_hook.get_pandas_df(sql_query)
        results['transactions'] = {
            'count': len(transactions_df),
            'data': transactions_df.to_dict('records')
        }
        
        logger.info(f"Extraídas {len(transactions_df)} transacciones de PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error extrayendo de PostgreSQL: {str(e)}")
        results['transactions'] = {'count': 0, 'data': [], 'error': str(e)}
    
    # 2. Extraer desde API externa
    try:
        http_hook = HttpHook(
            method='GET',
            http_conn_id='external_api_conn'
        )
        
        # Obtener datos de referencia (catálogo de productos)
        response = http_hook.run(
            endpoint='/api/v1/products',
            headers={
                'Authorization': f"Bearer {Variable.get('api_token')}",
                'Content-Type': 'application/json'
            }
        )
        
        api_data = json.loads(response.content)
        results['products'] = {
            'count': len(api_data.get('products', [])),
            'data': api_data.get('products', [])
        }
        
        logger.info(f"Extraídos {len(api_data.get('products', []))} productos de API")
        
    except Exception as e:
        logger.error(f"Error extrayendo de API: {str(e)}")
        results['products'] = {'count': 0, 'data': [], 'error': str(e)}
    
    # 3. Extraer archivo desde S3
    try:
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        bucket_name = Variable.get('source_data_bucket')
        
        # Buscar archivo más reciente
        yesterday = (context['execution_date'] - timedelta(days=1)).strftime('%Y%m%d')
        s3_key = f"customer-data/customers_{yesterday}.csv"
        
        if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
            file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
            customers_df = pd.read_csv(pd.io.common.StringIO(file_content))
            
            results['customers'] = {
                'count': len(customers_df),
                'data': customers_df.to_dict('records')
            }
            
            logger.info(f"Extraídos {len(customers_df)} clientes de S3")
        else:
            logger.warning(f"Archivo {s3_key} no encontrado en S3")
            results['customers'] = {'count': 0, 'data': [], 'error': 'Archivo no encontrado'}
            
    except Exception as e:
        logger.error(f"Error extrayendo de S3: {str(e)}")
        results['customers'] = {'count': 0, 'data': [], 'error': str(e)}
    
    # Guardar resultados en XCom
    context['task_instance'].xcom_push(key='extraction_results', value=results)
    
    return results

def transform_and_join_data(**context):
    """Transforma y combina datos de múltiples fuentes"""
    
    # Obtener datos extraídos
    extraction_results = context['task_instance'].xcom_pull(
        task_ids='extract_from_multiple_sources',
        key='extraction_results'
    )
    
    if not extraction_results:
        raise ValueError("No se recibieron datos de extracción")
    
    # Convertir a DataFrames
    transactions_df = pd.DataFrame(extraction_results['transactions']['data'])
    products_df = pd.DataFrame(extraction_results['products']['data'])
    customers_df = pd.DataFrame(extraction_results['customers']['data'])
    
    logger.info(f"Procesando {len(transactions_df)} transacciones")
    
    if transactions_df.empty:
        logger.warning("No hay transacciones para procesar")
        return {'processed_records': 0}
    
    # Realizar joins
    if not products_df.empty:
        # Join con productos para obtener información adicional
        transactions_df = transactions_df.merge(
            products_df[['id', 'name', 'price', 'margin']],
            left_on='product_category',
            right_on='id',
            how='left',
            suffixes=('', '_product')
        )
        logger.info("Join con productos completado")
    
    if not customers_df.empty:
        # Join con clientes para segmentación
        transactions_df = transactions_df.merge(
            customers_df[['id', 'segment', 'region', 'registration_date']],
            left_on='customer_id',
            right_on='id',
            how='left',
            suffixes=('', '_customer')
        )
        logger.info("Join con clientes completado")
    
    # Transformaciones adicionales
    transactions_df['profit'] = transactions_df['amount'] * transactions_df.get('margin', 0.2)
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['created_date'])
    transactions_df['month_year'] = transactions_df['transaction_date'].dt.to_period('M')
    
    # Calcular métricas agregadas
    summary_stats = {
        'total_transactions': len(transactions_df),
        'total_revenue': transactions_df['amount'].sum(),
        'total_profit': transactions_df['profit'].sum(),
        'avg_transaction_amount': transactions_df['amount'].mean(),
        'unique_customers': transactions_df['customer_id'].nunique() if 'customer_id' in transactions_df.columns else 0,
        'processing_timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"Transformación completada: {summary_stats}")
    
    # Guardar datos transformados
    processed_data = transactions_df.to_dict('records')
    
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    context['task_instance'].xcom_push(key='summary_stats', value=summary_stats)
    
    return summary_stats

def load_to_data_warehouse(**context):
    """Carga datos transformados al data warehouse"""
    
    # Obtener datos procesados
    processed_data = context['task_instance'].xcom_pull(
        task_ids='transform_and_join_data',
        key='processed_data'
    )
    
    summary_stats = context['task_instance'].xcom_pull(
        task_ids='transform_and_join_data',
        key='summary_stats'
    )
    
    if not processed_data:
        logger.warning("No hay datos procesados para cargar")
        return
    
    # Conectar al data warehouse
    dw_hook = PostgresHook(postgres_conn_id='postgres_data_warehouse')
    
    try:
        # Crear tabla temporal para la carga
        temp_table = f"temp_transactions_{context['run_id'].replace('-', '_')}"
        
        create_temp_table_sql = f"""
        CREATE TEMP TABLE {temp_table} (
            transaction_id BIGINT,
            transaction_date TIMESTAMP,
            amount DECIMAL(10,2),
            profit DECIMAL(10,2),
            customer_id BIGINT,
            customer_segment VARCHAR(50),
            customer_region VARCHAR(50),
            product_name VARCHAR(200),
            product_category VARCHAR(100),
            month_year VARCHAR(10),
            processing_timestamp TIMESTAMP
        );
        """
        
        dw_hook.run(create_temp_table_sql)
        logger.info(f"Tabla temporal {temp_table} creada")
        
        # Insertar datos en tabla temporal
        insert_count = 0
        batch_size = 1000
        
        for i in range(0, len(processed_data), batch_size):
            batch = processed_data[i:i + batch_size]
            
            # Preparar valores para inserción
            values = []
            for record in batch:
                values.append((
                    record.get('id'),
                    record.get('created_date'),
                    record.get('amount', 0),
                    record.get('profit', 0),
                    record.get('customer_id'),
                    record.get('segment'),
                    record.get('region'),
                    record.get('name'),
                    record.get('product_category'),
                    record.get('month_year'),
                    summary_stats['processing_timestamp']
                ))
            
            # Inserción por lotes
            insert_sql = f"""
            INSERT INTO {temp_table} VALUES %s
            """
            
            dw_hook.run(insert_sql, parameters=[values])
            insert_count += len(batch)
            
            logger.info(f"Insertados {insert_count}/{len(processed_data)} registros")
        
        # Mover datos de tabla temporal a tabla final
        merge_sql = f"""
        INSERT INTO fact_transactions 
        SELECT * FROM {temp_table}
        ON CONFLICT (transaction_id) DO UPDATE SET
            amount = EXCLUDED.amount,
            profit = EXCLUDED.profit,
            processing_timestamp = EXCLUDED.processing_timestamp;
        """
        
        dw_hook.run(merge_sql)
        
        # Actualizar tabla de métricas diarias
        metrics_sql = """
        INSERT INTO daily_metrics (date, total_transactions, total_revenue, total_profit, avg_transaction_amount)
        VALUES (CURRENT_DATE, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            total_transactions = EXCLUDED.total_transactions,
            total_revenue = EXCLUDED.total_revenue,
            total_profit = EXCLUDED.total_profit,
            avg_transaction_amount = EXCLUDED.avg_transaction_amount;
        """
        
        dw_hook.run(metrics_sql, parameters=[
            summary_stats['total_transactions'],
            summary_stats['total_revenue'],
            summary_stats['total_profit'],
            summary_stats['avg_transaction_amount']
        ])
        
        logger.info(f"Carga completada: {insert_count} registros procesados")
        
        return {
            'loaded_records': insert_count,
            'load_timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error en carga al data warehouse: {str(e)}")
        raise

def send_success_notification(**context):
    """Envía notificación de éxito con resumen del pipeline"""
    
    summary_stats = context['task_instance'].xcom_pull(
        task_ids='transform_and_join_data',
        key='summary_stats'
    )
    
    try:
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_webhook_conn',
            webhook_token=Variable.get('slack_webhook_token')
        )
        
        message = f"""
        ✅ *Pipeline de Datos Completado Exitosamente*
        
        *DAG:* {context['dag'].dag_id}
        *Fecha de ejecución:* {context['execution_date']}
        
        *Resumen:*
        • Transacciones procesadas: {summary_stats.get('total_transactions', 0):,}
        • Ingresos totales: ${summary_stats.get('total_revenue', 0):,.2f}
        • Ganancia total: ${summary_stats.get('total_profit', 0):,.2f}
        • Promedio por transacción: ${summary_stats.get('avg_transaction_amount', 0):,.2f}
        • Clientes únicos: {summary_stats.get('unique_customers', 0):,}
        
        El pipeline se ejecutó sin errores. ✨
        """
        
        slack_hook.send_text(message)
        logger.info("Notificación de éxito enviada a Slack")
        
    except Exception as e:
        logger.error(f"Error enviando notificación de éxito: {str(e)}")

# Definir el DAG
dag = DAG(
    'comprehensive_data_pipeline',
    default_args=default_args,
    description='Pipeline completo de datos usando múltiples Hooks y Connections',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'multi-source', 'production', 'comprehensive']
)

# Definir tareas principales
extract_task = PythonOperator(
    task_id='extract_from_multiple_sources',
    python_callable=extract_from_multiple_sources,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_join_data',
    python_callable=transform_and_join_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_data_warehouse',
    python_callable=load_to_data_warehouse,
    dag=dag
)

# Tarea de validación de calidad de datos
quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command="""
    python /opt/airflow/scripts/data_quality_check.py \
        --date {{ ds }} \
        --threshold 0.95
    """,
    dag=dag
)

notify_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='all_success'
)

# Definir dependencias
extract_task >> transform_task >> load_task >> quality_check >> notify_success
```

### 3.2.4 Lectura incremental vs. completa

La estrategia de carga de datos es fundamental para optimizar el rendimiento, minimizar el uso de recursos y garantizar la consistencia de los datos. La elección entre lectura completa e incremental depende de factores como el volumen de datos, la frecuencia de cambios, los recursos disponibles y los requisitos de latencia del negocio.

##### Lectura completa (full load)

La lectura completa implica extraer y procesar todo el conjunto de datos desde la fuente en cada ejecución, independientemente de si los datos han cambiado o no. Esta estrategia es apropiada cuando el volumen de datos es manejable, no existe un mecanismo confiable para identificar cambios, o cuando se requiere una reconstrucción completa del dataset por motivos de integridad o corrección de errores.

| Ventajas | Desventajas |
|:--|:--|
|Simplicidad en la implementación y lógica de procesamiento|Alto consumo de recursos computacionales y de red|
|Garantiza consistencia completa de los datos|Mayor tiempo de procesamiento|
|No requiere mecanismos de control de cambios en la fuente|Impacto en sistemas fuente por carga completa repetitiva|
|Ideal para datasets pequeños a medianos||

**Cargar todo un catálogo de productos cada noche**

Útil cuando el catálogo es relativamente pequeño y se requiere una vista completa y consistente para análisis o sincronización con otros sistemas.

```python
# Ejemplo con PySpark - Carga completa de catálogo de productos
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def full_load_product_catalog():
    spark = SparkSession.builder \
        .appName("ProductCatalogFullLoad") \
        .getOrCreate()
    
    # Configuración de conexión a base de datos fuente
    jdbc_url = "jdbc:postgresql://prod-db:5432/ecommerce"
    connection_properties = {
        "user": "etl_user",
        "password": "secure_password",
        "driver": "org.postgresql.Driver"
    }
    
    # Lectura completa del catálogo
    products_df = spark.read \
        .jdbc(url=jdbc_url, 
              table="products", 
              properties=connection_properties)
    
    # Agregar timestamp de procesamiento
    products_with_timestamp = products_df.withColumn(
        "load_timestamp", 
        current_timestamp()
    )
    
    # Escribir a data lake (sobrescribiendo completamente)
    products_with_timestamp.write \
        .mode("overwrite") \
        .option("path", "s3a://datalake/bronze/products/") \
        .saveAsTable("bronze.products")
    
    print(f"Carga completa finalizada. Registros procesados: {products_df.count()}")
    spark.stop()

# Programación en Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'product_catalog_full_load',
    default_args=default_args,
    description='Carga completa diaria del catálogo de productos',
    schedule_interval='0 2 * * *',  # Diario a las 2 AM
    catchup=False
)

full_load_task = PythonOperator(
    task_id='load_complete_catalog',
    python_callable=full_load_product_catalog,
    dag=dag
)
```

**Reprocesar un histórico completo de transacciones por corrección**

Necesario cuando se detectan errores en el procesamiento previo o cuando se implementan nuevas reglas de negocio que requieren recalcular todo el histórico.

```python
# Ejemplo con PySpark - Reprocesamiento completo de transacciones
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def reprocess_complete_transaction_history():
    spark = SparkSession.builder \
        .appName("TransactionHistoryReprocess") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Lectura completa del histórico de transacciones
    transactions_df = spark.read \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .parquet("s3a://datalake/raw/transactions/")
    
    # Aplicar nuevas reglas de negocio y correcciones
    corrected_transactions = transactions_df \
        .withColumn("amount_usd", 
                   when(col("currency") == "EUR", col("amount") * 1.1)
                   .when(col("currency") == "GBP", col("amount") * 1.25)
                   .otherwise(col("amount"))) \
        .withColumn("transaction_category",
                   when(col("merchant_category").isin(["grocery", "supermarket"]), "essential")
                   .when(col("merchant_category").isin(["entertainment", "dining"]), "lifestyle")
                   .otherwise("other")) \
        .withColumn("reprocessed_at", current_timestamp()) \
        .withColumn("processing_version", lit("v2.1"))
    
    # Escribir datos corregidos (particionado por año-mes para mejor performance)
    corrected_transactions \
        .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM")) \
        .write \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .option("path", "s3a://datalake/silver/transactions_corrected/") \
        .saveAsTable("silver.transactions_corrected")
    
    # Generar métricas de reprocesamiento
    total_records = corrected_transactions.count()
    date_range = corrected_transactions.agg(
        min("transaction_date").alias("min_date"),
        max("transaction_date").alias("max_date")
    ).collect()[0]
    
    print(f"Reprocesamiento completo finalizado:")
    print(f"- Registros procesados: {total_records}")
    print(f"- Rango de fechas: {date_range['min_date']} a {date_range['max_date']}")
    
    spark.stop()

# DAG de Airflow para reprocesamiento manual
reprocess_dag = DAG(
    'transaction_history_reprocess',
    default_args=default_args,
    description='Reprocesamiento completo del histórico de transacciones',
    schedule_interval=None,  # Ejecución manual solamente
    catchup=False
)

reprocess_task = PythonOperator(
    task_id='reprocess_transaction_history',
    python_callable=reprocess_complete_transaction_history,
    dag=reprocess_dag
)
```

##### Lectura incremental

La lectura incremental procesa únicamente los datos nuevos o modificados desde la última ejecución, utilizando mecanismos de control como timestamps, IDs auto-incrementales, flags de estado o columnas de versionado. Esta estrategia es esencial para sistemas de alto volumen donde procesar todos los datos en cada ejecución sería ineficiente o impracticable.

| Ventajas | Desventajas |
|:--|:--|
|Significativa reducción en tiempo de procesamiento y uso de recursos|Mayor complejidad en la implementación|
|Menor impacto en sistemas fuente|Requiere mecanismos confiables de control de cambios|
|Permite procesamiento en tiempo real o near-real-time|Necesidad de manejar datos duplicados o fuera de orden|
|Escalabilidad mejorada para grandes volúmenes de datos|Potencial pérdida de datos si falla el mecanismo de control|

**Cargar órdenes nuevas con base en un campo `created_at`**

Estrategia común para sistemas transaccionales donde se procesan solo las órdenes creadas desde la última ejecución.

```python
# Ejemplo con PySpark - Carga incremental de órdenes
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import boto3

def incremental_orders_load():
    spark = SparkSession.builder \
        .appName("OrdersIncrementalLoad") \
        .getOrCreate()
    
    # Obtener último timestamp procesado desde metastore o archivo de control
    def get_last_processed_timestamp():
        try:
            last_run_df = spark.read.table("control.incremental_loads") \
                .filter(col("table_name") == "orders") \
                .orderBy(col("last_processed_timestamp").desc()) \
                .limit(1)
            
            if last_run_df.count() > 0:
                return last_run_df.collect()[0]["last_processed_timestamp"]
            else:
                # Si es la primera ejecución, usar fecha de inicio del proyecto
                return datetime(2024, 1, 1)
        except:
            return datetime(2024, 1, 1)
    
    last_timestamp = get_last_processed_timestamp()
    current_timestamp = datetime.now()
    
    print(f"Procesando órdenes desde: {last_timestamp}")
    
    # Configuración de conexión
    jdbc_url = "jdbc:mysql://orders-db:3306/ecommerce"
    connection_properties = {
        "user": "etl_user",
        "password": "secure_password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    # Query incremental con filtro por timestamp
    incremental_query = f"""
    (SELECT order_id, customer_id, order_date, total_amount, status, created_at, updated_at
     FROM orders 
     WHERE created_at > '{last_timestamp}' 
     AND created_at <= '{current_timestamp}'
     ORDER BY created_at) as incremental_orders
    """
    
    # Lectura incremental
    new_orders_df = spark.read \
        .jdbc(url=jdbc_url, 
              table=incremental_query, 
              properties=connection_properties)
    
    if new_orders_df.count() > 0:
        # Procesamiento y enriquecimiento de datos
        processed_orders = new_orders_df \
            .withColumn("load_timestamp", lit(current_timestamp)) \
            .withColumn("order_year_month", date_format(col("order_date"), "yyyy-MM"))
        
        # Escribir nuevos datos (append mode)
        processed_orders.write \
            .mode("append") \
            .partitionBy("order_year_month") \
            .option("path", "s3a://datalake/bronze/orders/") \
            .saveAsTable("bronze.orders")
        
        # Actualizar tabla de control
        control_record = spark.createDataFrame([{
            "table_name": "orders",
            "last_processed_timestamp": current_timestamp,
            "records_processed": new_orders_df.count(),
            "processing_timestamp": datetime.now()
        }])
        
        control_record.write \
            .mode("append") \
            .saveAsTable("control.incremental_loads")
        
        print(f"Carga incremental completada. Nuevos registros: {new_orders_df.count()}")
    else:
        print("No hay nuevos registros para procesar")
    
    spark.stop()

# DAG de Airflow para carga incremental
incremental_dag = DAG(
    'orders_incremental_load',
    default_args=default_args,
    description='Carga incremental de órdenes cada 15 minutos',
    schedule_interval='*/15 * * * *',  # Cada 15 minutos
    catchup=True  # Permite recuperar ejecuciones perdidas
)

incremental_task = PythonOperator(
    task_id='load_incremental_orders',
    python_callable=incremental_orders_load,
    dag=incremental_dag
)
```

**Usar `watermark` en Spark para carga de eventos recientes desde Kafka**

Técnica avanzada para procesamiento de streaming que maneja eventos tardíos y garantiza procesamiento exactly-once.

```python
# Ejemplo con Spark Structured Streaming - Watermark desde Kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def kafka_streaming_with_watermark():
    spark = SparkSession.builder \
        .appName("KafkaStreamingWatermark") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://checkpoints/kafka-events/") \
        .getOrCreate()
    
    # Schema para eventos de Kafka
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])
    
    # Lectura desde Kafka con configuración de consumer
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("kafka.group.id", "spark-streaming-consumer") \
        .load()
    
    # Parsear JSON y extraer timestamp
    parsed_events = kafka_stream \
        .select(
            from_json(col("value").cast("string"), event_schema).alias("event"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset"),
            col("partition")
        ) \
        .select("event.*", "kafka_timestamp", "offset", "partition")
    
    # Aplicar watermark para manejar eventos tardíos
    # Permite eventos hasta 5 minutos tarde
    watermarked_events = parsed_events \
        .withWatermark("event_timestamp", "5 minutes")
    
    # Agregaciones por ventana de tiempo con watermark
    windowed_aggregations = watermarked_events \
        .groupBy(
            window(col("event_timestamp"), "1 minute", "30 seconds"),  # Ventana deslizante
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            collect_set("user_id").alias("user_list")
        ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    
    # Configurar sink para escritura incremental
    query = windowed_aggregations.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("path", "s3a://datalake/silver/event_aggregations/") \
        .option("checkpointLocation", "s3a://checkpoints/event-aggregations/") \
        .partitionBy("event_type") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    # También escribir eventos raw para auditoría
    raw_events_query = parsed_events.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("path", "s3a://datalake/bronze/raw_events/") \
        .option("checkpointLocation", "s3a://checkpoints/raw-events/") \
        .partitionBy(date_format(col("event_timestamp"), "yyyy-MM-dd")) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Monitoreo del stream
    print("Streaming iniciado. Estadísticas del stream:")
    print(f"- Checkpoint location: {query.lastProgress}")
    
    return query, raw_events_query

# Integración con Airflow para monitoreo y control
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

streaming_dag = DAG(
    'kafka_streaming_monitor',
    default_args=default_args,
    description='Monitoreo y control de streaming desde Kafka',
    schedule_interval='*/5 * * * *',  # Verificación cada 5 minutos
    catchup=False
)

# Sensor para verificar que el checkpoint existe
checkpoint_sensor = FileSensor(
    task_id='check_streaming_checkpoint',
    filepath='s3a://checkpoints/kafka-events/_metadata',
    fs_conn_id='aws_default',
    poke_interval=60,
    timeout=300,
    dag=streaming_dag
)

# Tarea para verificar salud del streaming job
health_check = BashOperator(
    task_id='streaming_health_check',
    bash_command='''
    # Verificar que los archivos de checkpoint se están actualizando
    latest_checkpoint=$(aws s3 ls s3://checkpoints/kafka-events/commits/ --recursive | tail -1 | awk '{print $1" "$2}')
    current_time=$(date -u +"%Y-%m-%d %H:%M:%S")
    
    # Calcular diferencia en minutos
    checkpoint_age=$(( ($(date -d "$current_time" +%s) - $(date -d "$latest_checkpoint" +%s)) / 60 ))
    
    if [ $checkpoint_age -gt 10 ]; then
        echo "WARNING: Streaming job checkpoint is $checkpoint_age minutes old"
        exit 1
    else
        echo "Streaming job is healthy. Checkpoint age: $checkpoint_age minutes"
    fi
    ''',
    dag=streaming_dag
)

checkpoint_sensor >> health_check
```

Esta implementación completa muestra cómo ambas estrategias pueden ser efectivamente utilizadas según los requisitos específicos del caso de uso, considerando factores como volumen de datos, latencia requerida, y recursos disponibles.

### 3.2.5 Gestión de autenticación y configuración segura

Proteger accesos y configurar credenciales de forma segura es una práctica obligatoria en entornos productivos. La gestión inadecuada de secretos y credenciales representa uno de los principales vectores de ataque en sistemas de datos modernos, por lo que implementar estrategias robustas de autenticación es fundamental para mantener la integridad y confidencialidad de los pipelines de datos.

##### Uso de variables y secretos

La externalización de credenciales mediante sistemas especializados de gestión de secretos permite eliminar completamente las credenciales hardcodeadas del código fuente. Estas herramientas proporcionan cifrado en reposo y en tránsito, control de acceso granular, rotación automática de credenciales y auditoría completa de accesos. Airflow ofrece múltiples backends de secretos que se integran nativamente con servicios como AWS Secrets Manager, Azure Key Vault, Google Secret Manager y HashiCorp Vault.

**Guardar contraseñas de bases de datos en Airflow usando `Variable.get("db_password")`.**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

def connect_to_database(**context):
    """
    Función que establece conexión segura a PostgreSQL usando variables de Airflow
    """
    try:
        # Obtener credenciales de forma segura desde Variables de Airflow
        db_host = Variable.get("postgres_host")
        db_port = Variable.get("postgres_port", default_var="5432")
        db_name = Variable.get("postgres_database")
        db_user = Variable.get("postgres_username")
        db_password = Variable.get("postgres_password")  # Marcada como sensitive en UI
        
        # Crear conexión usando PostgresHook
        postgres_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema=db_name
        )
        
        # Alternativa: conexión manual con credenciales desde variables
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Ejecutar consulta de prueba
        records = postgres_hook.get_records("SELECT version();")
        logging.info(f"Conexión exitosa. Versión de PostgreSQL: {records[0][0]}")
        
        return {"status": "success", "connection": "established"}
        
    except Exception as e:
        logging.error(f"Error conectando a la base de datos: {str(e)}")
        raise

# Configuración segura usando Airflow Secrets Backend
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'secure_database_connection',
    default_args=default_args,
    description='Ejemplo de conexión segura a base de datos',
    schedule_interval='@daily',
    catchup=False,
    tags=['security', 'database']
)

# Task que utiliza credenciales seguras
secure_db_task = PythonOperator(
    task_id='connect_secure_database',
    python_callable=connect_to_database,
    dag=dag
)
```

**Usar Azure Key Vault para proteger secretos de conexión a Blob Storage.**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.key_vault import AzureKeyVaultHook
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import logging

def process_blob_data_securely(**context):
    """
    Función que accede a Azure Blob Storage usando secretos desde Key Vault
    """
    try:
        # Configurar Azure Key Vault Hook
        key_vault_hook = AzureKeyVaultHook(
            key_vault_name="your-key-vault-name",
            key_vault_conn_id="azure_key_vault_default"
        )
        
        # Obtener secretos del Key Vault
        storage_account_name = key_vault_hook.get_secret("storage-account-name")
        storage_account_key = key_vault_hook.get_secret("storage-account-key")
        container_name = key_vault_hook.get_secret("blob-container-name")
        
        # Crear cliente de Blob Storage con credenciales seguras
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Listar blobs en el contenedor
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = list(container_client.list_blobs())
        
        logging.info(f"Encontrados {len(blob_list)} blobs en el contenedor {container_name}")
        
        # Procesar cada blob (ejemplo: leer archivos CSV)
        processed_files = []
        for blob in blob_list[:5]:  # Procesar solo los primeros 5 archivos
            if blob.name.endswith('.csv'):
                blob_client = blob_service_client.get_blob_client(
                    container=container_name, 
                    blob=blob.name
                )
                
                # Descargar contenido del blob
                blob_data = blob_client.download_blob()
                content = blob_data.readall().decode('utf-8')
                
                logging.info(f"Procesado archivo: {blob.name}, tamaño: {len(content)} caracteres")
                processed_files.append({
                    'filename': blob.name,
                    'size': blob.size,
                    'last_modified': blob.last_modified
                })
        
        return {
            "status": "success",
            "processed_files": processed_files,
            "total_blobs": len(blob_list)
        }
        
    except Exception as e:
        logging.error(f"Error accediendo a Azure Blob Storage: {str(e)}")
        raise

# Configuración del DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'azure_blob_secure_access',
    default_args=default_args,
    description='Acceso seguro a Azure Blob Storage con Key Vault',
    schedule_interval='@hourly',
    catchup=False,
    tags=['azure', 'security', 'blob-storage']
)

# Task principal
secure_blob_task = PythonOperator(
    task_id='process_blob_data_secure',
    python_callable=process_blob_data_securely,
    dag=dag
)
```

##### Autenticación en servicios y APIs

La autenticación en servicios externos requiere implementar protocolos estándar como OAuth2, JWT, o sistemas de roles basados en la nube (IAM). Estos mecanismos proporcionan acceso granular y temporal, permitiendo la rotación automática de credenciales y el principio de menor privilegio. La integración con proveedores de identidad corporativos facilita la gestión centralizada de accesos y el cumplimiento de políticas de seguridad organizacionales.

**Conectarse a un API de terceros mediante OAuth2.**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from requests_oauthlib import OAuth2Session
import requests
import json
from datetime import datetime, timedelta
import logging

def authenticate_and_call_api(**context):
    """
    Función que implementa flujo OAuth2 para acceder a API externa
    """
    try:
        # Obtener credenciales OAuth2 desde Variables de Airflow
        client_id = Variable.get("oauth2_client_id")
        client_secret = Variable.get("oauth2_client_secret")
        token_url = Variable.get("oauth2_token_url")
        api_base_url = Variable.get("api_base_url")
        
        # Configurar datos para solicitud de token
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'read write'  # Scopes requeridos
        }
        
        # Solicitar token de acceso
        token_response = requests.post(
            token_url,
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if token_response.status_code != 200:
            raise Exception(f"Error obteniendo token: {token_response.text}")
        
        token_info = token_response.json()
        access_token = token_info['access_token']
        
        logging.info("Token OAuth2 obtenido exitosamente")
        
        # Usar token para llamadas a la API
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Ejemplo: obtener datos de usuarios
        users_response = requests.get(
            f"{api_base_url}/users",
            headers=headers
        )
        
        if users_response.status_code == 200:
            users_data = users_response.json()
            logging.info(f"Obtenidos {len(users_data)} usuarios de la API")
            
            # Ejemplo: crear un nuevo recurso
            new_resource = {
                'name': 'Data Pipeline Resource',
                'type': 'automated',
                'created_by': 'airflow'
            }
            
            create_response = requests.post(
                f"{api_base_url}/resources",
                headers=headers,
                json=new_resource
            )
            
            if create_response.status_code == 201:
                created_resource = create_response.json()
                logging.info(f"Recurso creado con ID: {created_resource.get('id')}")
            
            return {
                "status": "success",
                "users_count": len(users_data),
                "resource_created": create_response.status_code == 201
            }
        else:
            raise Exception(f"Error en API call: {users_response.text}")
            
    except Exception as e:
        logging.error(f"Error en autenticación OAuth2: {str(e)}")
        raise

# Configuración del DAG
default_args = {
    'owner': 'api_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'oauth2_api_integration',
    default_args=default_args,
    description='Integración con API externa usando OAuth2',
    schedule_interval='@daily',
    catchup=False,
    tags=['api', 'oauth2', 'integration']
)

oauth2_task = PythonOperator(
    task_id='authenticate_and_call_external_api',
    python_callable=authenticate_and_call_api,
    dag=dag
)
```

**Usar roles IAM en AWS para acceso a buckets S3 sin credenciales explícitas.**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
from datetime import datetime, timedelta
import logging
import io

def process_s3_data_with_iam_role(**context):
    """
    Función que accede a S3 usando roles IAM sin credenciales explícitas
    """
    try:
        # Usar S3Hook de Airflow (utiliza roles IAM automáticamente)
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        # Configuración de bucket y prefijos
        bucket_name = 'your-data-bucket'
        input_prefix = 'raw-data/'
        output_prefix = 'processed-data/'
        
        # Listar objetos en el bucket
        objects = s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=input_prefix
        )
        
        logging.info(f"Encontrados {len(objects)} objetos en {bucket_name}/{input_prefix}")
        
        # Procesar archivos CSV
        processed_files = []
        
        for obj_key in objects:
            if obj_key.endswith('.csv'):
                try:
                    # Leer archivo CSV desde S3
                    csv_content = s3_hook.read_key(
                        key=obj_key,
                        bucket_name=bucket_name
                    )
                    
                    # Procesar con pandas
                    df = pd.read_csv(io.StringIO(csv_content))
                    
                    # Realizar transformaciones (ejemplo)
                    df_processed = df.copy()
                    df_processed['processed_date'] = datetime.now().strftime('%Y-%m-%d')
                    df_processed['record_count'] = len(df)
                    
                    # Guardar archivo procesado de vuelta a S3
                    output_key = obj_key.replace(input_prefix, output_prefix).replace('.csv', '_processed.csv')
                    
                    csv_buffer = io.StringIO()
                    df_processed.to_csv(csv_buffer, index=False)
                    
                    s3_hook.load_string(
                        string_data=csv_buffer.getvalue(),
                        key=output_key,
                        bucket_name=bucket_name,
                        replace=True
                    )
                    
                    logging.info(f"Procesado: {obj_key} -> {output_key}")
                    
                    processed_files.append({
                        'input_file': obj_key,
                        'output_file': output_key,
                        'records_processed': len(df),
                        'file_size_bytes': len(csv_content)
                    })
                    
                except Exception as file_error:
                    logging.error(f"Error procesando {obj_key}: {str(file_error)}")
                    continue
        
        # Crear reporte de procesamiento
        report_data = {
            'processing_date': datetime.now().isoformat(),
            'bucket': bucket_name,
            'total_files_processed': len(processed_files),
            'files_detail': processed_files
        }
        
        # Guardar reporte en S3
        report_key = f"reports/processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3_hook.load_string(
            string_data=json.dumps(report_data, indent=2),
            key=report_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        return {
            'status': 'success',
            'files_processed': len(processed_files),
            'report_location': f's3://{bucket_name}/{report_key}'
        }
        
    except NoCredentialsError:
        logging.error("No se encontraron credenciales AWS. Verificar configuración de roles IAM.")
        raise
    except ClientError as e:
        logging.error(f"Error de cliente AWS: {e.response['Error']['Message']}")
        raise
    except Exception as e:
        logging.error(f"Error general procesando datos S3: {str(e)}")
        raise

def verify_iam_permissions(**context):
    """
    Función auxiliar para verificar permisos IAM
    """
    try:
        # Crear cliente STS para verificar identidad
        sts_client = boto3.client('sts')
        identity = sts_client.get_caller_identity()
        
        logging.info(f"Ejecutando como: {identity.get('Arn')}")
        logging.info(f"Account ID: {identity.get('Account')}")
        
        # Verificar acceso básico a S3
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()
        
        logging.info(f"Acceso a S3 verificado. Buckets accesibles: {len(response['Buckets'])}")
        
        return {
            'iam_role': identity.get('Arn'),
            'account_id': identity.get('Account'),
            's3_access': True
        }
        
    except Exception as e:
        logging.error(f"Error verificando permisos IAM: {str(e)}")
        raise

# Configuración del DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    's3_iam_role_processing',
    default_args=default_args,
    description='Procesamiento de datos S3 usando roles IAM',
    schedule_interval='@daily',
    catchup=False,
    tags=['aws', 's3', 'iam', 'data-processing']
)

# Tasks del pipeline
verify_permissions_task = PythonOperator(
    task_id='verify_iam_permissions',
    python_callable=verify_iam_permissions,
    dag=dag
)

process_s3_data_task = PythonOperator(
    task_id='process_s3_data_with_iam',
    python_callable=process_s3_data_with_iam_role,
    dag=dag
)

# Definir dependencias
verify_permissions_task >> process_s3_data_task
```

## Tarea

**Desarrolla los siguientes ejercicios prácticos relacionados con el tema 3.2**:

1. Crea un pipeline en Apache Spark que lea datos de una base de datos PostgreSQL usando JDBC y los almacene como archivos Parquet.
2. Implementa un DAG en Airflow que consuma una API REST con `HttpHook` y almacene los resultados en un archivo JSON en S3.
3. Compara el rendimiento entre una lectura completa y una lectura incremental sobre una tabla con campo `updated_at` en MySQL.
4. Configura una conexión segura en Airflow utilizando variables y crea un DAG que lea de MongoDB.
5. Diseña un pipeline en Spark que combine datos de una API y archivos CSV, y cargue el resultado en un Delta Lake particionado por fecha.

