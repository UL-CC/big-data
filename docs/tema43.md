# 4. Automatización y Orquestación con Apache Airflow

## 4.3. Integración con ecosistema Big Data

**Objetivo:**

Comprender y aplicar la integración de Apache Airflow con herramientas y servicios del ecosistema Big Data, incluyendo motores de procesamiento como Spark, sistemas distribuidos como Hadoop y Hive, almacenamiento como HDFS, conectores en la nube (AWS, GCP, Azure) y operadores para bases de datos y APIs, permitiendo la orquestación automatizada de flujos de datos complejos en arquitecturas modernas.

**Introducción:**

En los entornos de Big Data, los flujos de trabajo rara vez están aislados. Normalmente interactúan con múltiples plataformas, motores de procesamiento y servicios en la nube. Apache Airflow, como herramienta de orquestación, proporciona operadores, hooks y mecanismos de conexión para facilitar esta integración, permitiendo ejecutar tareas en Spark, consultar datos en Hive, mover archivos en HDFS y conectar con servicios en AWS, GCP o Azure.

**Desarrollo:**

La integración de Airflow con el ecosistema Big Data permite automatizar procesos complejos distribuidos, reduciendo errores manuales y mejorando la eficiencia. Usando operadores específicos, Airflow se convierte en un controlador central capaz de invocar jobs de Spark, consultar tablas Hive, interactuar con buckets de almacenamiento en la nube o consumir APIs externas. Esta capacidad de integración lo convierte en una herramienta esencial para arquitecturas de datos modernas orientadas a pipelines escalables y mantenibles.


### 4.3.1 SparkSubmitOperator y configuración con Spark

**Apache Spark** es un motor unificado de análisis de datos de código abierto, diseñado para el procesamiento de datos a gran escala. Es ampliamente utilizado en diversas aplicaciones, incluyendo la **extracción, transformación y carga (ETL)** de datos, el procesamiento de *streaming* en tiempo real, el aprendizaje automático (**Machine Learning**) y los gráficos. Su capacidad para manejar grandes volúmenes de datos de manera distribuida y eficiente lo convierte en una herramienta fundamental en los ecosistemas de *big data*.

**Apache Airflow**, como orquestador de flujos de trabajo, ofrece una integración nativa y robusta con Spark, permitiendo que tus **DAGs** (Directed Acyclic Graphs) lancen y gestionen **trabajos Spark** directamente. Esta sinergia es crucial para construir *pipelines* de datos complejos donde el procesamiento distribuido es un requisito. Airflow proporciona el **`SparkSubmitOperator`**, una herramienta especializada para esta tarea.

##### Configuración del SparkSubmitOperator

El `SparkSubmitOperator` en Airflow está diseñado para emular la funcionalidad del comando de línea de comandos `spark-submit`. Este comando es el principal *script* utilizado para enviar aplicaciones (escritas en Scala, Java, Python o R) a un clúster Spark.

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_task = SparkSubmitOperator(
    task_id='run_spark_job',
    application='/path/to/app.py',
    conn_id='spark_default',
    executor_memory='2g',
    total_executor_cores=4,
    name='example_spark_job',
    dag=dag
)
```

##### Orquestación de un flujo de limpieza de datos con PySpark

Imagina que tienes un flujo de trabajo de datos que requiere una fase crucial de **limpieza y preparación de datos** a gran escala. Para manejar el volumen y la complejidad de estos datos, has desarrollado un *script* de Python llamado `data_cleaning.py` que utiliza las capacidades de procesamiento distribuido de **Apache Spark**. Este *script* está diseñado para ejecutarse en un **clúster Spark**, ya sea local o remoto (como en un entorno de producción).

Tu objetivo es integrar la ejecución de este *script* Spark en tu **DAG de Airflow**, de modo que se ejecute como una tarea más dentro de tu *pipeline* general de datos. Aquí es donde el **`SparkSubmitOperator`** de Airflow se vuelve invaluable.

```python
spark_cleaning = SparkSubmitOperator(
    task_id='clean_data',
    application='/opt/airflow/scripts/data_cleaning.py',
    conn_id='spark_local',
    application_args=['--input', 'hdfs://data/raw', '--output', 'hdfs://data/clean'],
    dag=dag
)
```

Este enfoque permite que el **procesamiento distribuido de Spark** se convierta en una **parte integral y orquestada** de una cadena de tareas más amplia dentro de un DAG de Airflow. Los beneficios clave incluyen:

* **Orquestación Centralizada**: Airflow gestiona cuándo y cómo se ejecuta el trabajo Spark, asegurando que ocurra en el momento adecuado, después de que las dependencias previas (por ejemplo, la ingesta de datos) se hayan completado y antes de que las tareas posteriores (por ejemplo, la carga a un *data warehouse* o la ejecución de un modelo de ML) comiencen.
* **Reintentos Automáticos y Manejo de Errores**: Si el trabajo Spark falla, Airflow puede configurarse para reintentar la tarea automáticamente, y su robusto sistema de registro facilita la depuración de cualquier problema.
* **Visibilidad y Monitoreo**: Puedes monitorear el estado de tu trabajo Spark directamente desde la interfaz de usuario de Airflow, viendo si está en ejecución, ha tenido éxito o ha fallado, e incluso acceder a los registros del *driver* de Spark.
* **Parametrización Dinámica**: Utilizando el templating de Jinja2 o pasando argumentos dinámicamente, puedes hacer que tus trabajos Spark sean aún más flexibles, por ejemplo, procesando datos para una fecha específica (`{{ ds }}`) o para un entorno particular.


### 4.3.2 Integración con Hadoop, Hive, HDFS

Apache Airflow puede interactuar con el ecosistema Hadoop gracias a sus hooks y operadores especializados, facilitando tareas como mover archivos en HDFS o ejecutar queries en Hive.

##### Hadoop y HDFS

Airflow puede conectarse con HDFS usando el `HdfsHook` y `HdfsSensor` para detectar archivos o mover datos entre sistemas.

```python
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

hdfs_check = HdfsSensor(
    task_id='check_file_in_hdfs',
    filepath='/data/input/file.csv',
    hdfs_conn_id='hdfs_default',
    dag=dag
)
```

Este sensor espera hasta que el archivo esté disponible para iniciar tareas posteriores.

##### Hive

Para consultas SQL en Hive, Airflow ofrece el `HiveOperator`. También se puede usar `HivePartitionSensor` para esperar hasta que se cree una partición.

```python
from airflow.providers.apache.hive.operators.hive import HiveOperator

hive_query = HiveOperator(
    task_id='run_hive_query',
    hql='SELECT COUNT(*) FROM transactions WHERE amount > 1000;',
    hive_cli_conn_id='hive_conn',
    dag=dag
)
```

### 4.3.3 Conectores cloud

Airflow incluye conectores y operadores especializados para interactuar con servicios cloud como AWS, GCP y Azure.

##### AWS

Airflow proporciona operadores para S3, Redshift, Athena, EMR, entre otros.

```python
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

upload_to_s3 = S3CreateObjectOperator(
    task_id='upload_file',
    s3_bucket='my-bucket',
    s3_key='data/output.json',
    data='{"status": "ok"}',
    aws_conn_id='aws_default',
    dag=dag
)
```

##### GCP

Airflow permite orquestar servicios como BigQuery, GCS, Cloud Functions, Dataproc.

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

bq_query = BigQueryInsertJobOperator(
    task_id="run_bq_query",
    configuration={
        "query": {
            "query": "SELECT * FROM my_dataset.table WHERE active = TRUE",
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)
```

##### Azure

Airflow también soporta Blob Storage, Data Lake y Synapse Analytics.

```python
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator

delete_blob = WasbDeleteBlobOperator(
    task_id='delete_blob',
    container_name='datalake',
    blob_name='old/file.csv',
    wasb_conn_id='azure_blob_conn',
    dag=dag
)
```


### 4.3.4 Operadores de bases de datos y APIs

Airflow facilita el acceso a bases de datos relacionales y APIs mediante operadores y hooks genéricos y específicos.

##### Operadores de bases de datos

Operadores como `PostgresOperator`, `MySqlOperator`, `SqliteOperator`, entre otros, permiten ejecutar sentencias SQL como parte de un flujo.

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

run_sql = PostgresOperator(
    task_id='insert_data',
    sql='INSERT INTO logs (message) VALUES (\'Pipeline success\');',
    postgres_conn_id='postgres_default',
    dag=dag
)
```

##### APIs con HttpOperator

Airflow también permite interactuar con APIs REST.

```python
from airflow.providers.http.operators.http import SimpleHttpOperator

call_api = SimpleHttpOperator(
    task_id='call_rest_api',
    http_conn_id='api_service',
    endpoint='/status',
    method='GET',
    dag=dag
)
```

## Tarea

Realiza los siguientes ejercicios prácticos para afianzar los conocimientos del tema 4.3:

1. **Construye un DAG que lance un trabajo PySpark usando `SparkSubmitOperator`** con parámetros dinámicos enviados desde `Variable` o `Jinja2`.
2. **Diseña un DAG que espere un archivo en HDFS** con `HdfsSensor` y luego ejecute una consulta sobre una tabla Hive.
3. **Crea un DAG con dos tareas:** la primera debe cargar un archivo JSON a un bucket de S3, y la segunda debe notificar por API cuando el archivo haya sido subido correctamente.
4. **Integra Airflow con BigQuery y diseña una consulta parametrizada** que se ejecute todos los días y almacene los resultados en una tabla destino.
5. **Crea un DAG que consuma una API pública**, almacene la respuesta en una base de datos Postgres y registre el evento en una tabla de auditoría.

