# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.5. Monitorización y Troubleshooting de Pipelines

**Objetivo**:

Detectar, analizar y resolver errores operativos en pipelines ETL mediante herramientas de observabilidad, métricas clave, interfaces gráficas y estrategias de recuperación automatizada, asegurando la confiabilidad y eficiencia del procesamiento de datos.

**Introducción**:

La creación de pipelines ETL escalables y funcionales no es suficiente si no se cuenta con mecanismos adecuados para su supervisión y mantenimiento. La monitorización permite identificar cuellos de botella, errores y problemas de rendimiento, mientras que el troubleshooting proporciona los medios para analizarlos y resolverlos. Este tema aborda herramientas como Spark UI y Airflow UI, el uso de logs y métricas, y estrategias prácticas para prevenir y remediar fallos comunes.

**Desarrollo**:

La observabilidad de pipelines ETL es un componente crítico en cualquier arquitectura de datos moderna. Los ingenieros de datos deben dominar no solo la construcción de flujos, sino también su operación continua. Este tema desarrolla las competencias necesarias para leer y configurar logs, interpretar interfaces de usuario de ejecución, utilizar métricas relevantes, automatizar alertas y aplicar soluciones efectivas a problemas frecuentes que afectan el desempeño de los pipelines.

### 3.5.1 Configuración y lectura de logs en Spark y Airflow

La inspección de logs es crucial para entender el comportamiento interno de tus **jobs de Spark** y **DAGs de Airflow**. Al monitorear los logs, puedes identificar rápidamente errores, cuellos de botella de rendimiento y problemas de configuración que podrían afectar la eficiencia de tus pipelines de datos. Esto te permite mantener la estabilidad y el rendimiento de tus sistemas de Big Data.

##### Configuración de logs en Apache Spark y Airflow

Tanto Spark como Airflow ofrecen opciones robustas para personalizar el nivel de logging, lo que te permite ajustar la verbosidad de los logs según tus necesidades específicas (por ejemplo, `INFO`, `DEBUG`, `WARN`, `ERROR`, `FATAL`). Una configuración adecuada asegura que obtengas la trazabilidad necesaria sin sobrecargar tus sistemas de almacenamiento de logs.

**Configuración de logs en Apache Spark**:

El archivo `log4j.properties` es fundamental para controlar el comportamiento del logging en Spark. Puedes ubicarlo en el directorio `conf` de tu instalación de Spark. Dentro de este archivo, puedes definir los niveles de logging para diferentes paquetes de Spark (por ejemplo, `org.apache.spark`, `org.eclipse.jetty`) y configurar los **appenders** para especificar dónde se escribirán los logs (por ejemplo, consola, archivos, syslog).

Para reducir la verbosidad de los logs de Spark a `WARN` (solo advertencias y errores) y ver los logs en la consola, añadirías las siguientes líneas a `log4j.properties`:

```properties
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
```

Para habilitar el logging a nivel `DEBUG` para un paquete específico, por ejemplo, para la configuración de Spark SQL:

```properties
log4j.logger.org.apache.spark.sql.execution.SparkSqlParser=DEBUG
```

**Configuración de logs en Apache Airflow**:

El archivo `airflow.cfg` es el corazón de la configuración de Airflow y te permite definir cómo y dónde se almacenan los logs de tus DAGs y tareas. Dentro de la sección `[logging]`, puedes especificar el **`remote_logging`** para enviar logs a servicios de almacenamiento en la nube, el **`log_level`** global y la ruta **`base_log_folder`** para los logs locales.

Para un control más granular, puedes definir una clase Python personalizada `logging_config_class` que herede de `airflow.utils.log.logging_mixin.LoggingMixin` y especifique reglas de logging avanzadas. Esto es especialmente útil para integrar sistemas de logging personalizados o para configurar loggers de forma dinámica.

En `airflow.cfg`, puedes establecer el nivel de log global y la ruta local:

```ini
[logging]
log_level = INFO
base_log_folder = /opt/airflow/logs
```

Para centralizar los logs en un servicio como Amazon CloudWatch, puedes configurar el `remote_logging` y especificar la clase de logging:

```ini
[logging]
remote_logging = True
remote_base_log_folder = s3://your-airflow-logs-bucket/
remote_log_conn_id = aws_default
log_level = INFO
logging_config_class = my_project.config.cloud_logging.CloudLoggingConfig
```

Donde `CloudLoggingConfig` sería una clase Python personalizada que manejaría la integración con CloudWatch o un servicio similar.

* **Centralización de logs en entornos en la nube**:

En entornos de producción basados en la nube, es una práctica recomendada centralizar los logs para facilitar su análisis, monitoreo y archivo.

* **Amazon CloudWatch / S3**: Para AWS, puedes enviar logs de Spark directamente a CloudWatch Logs o almacenar los logs de Airflow en S3, lo que permite un acceso unificado y el uso de servicios como CloudWatch Logs Insights para consultas.
* **Google Cloud Logging / Google Cloud Storage**: En GCP, los logs pueden ser enviados a Cloud Logging, que ofrece capacidades de búsqueda y análisis potentes, y los logs a largo plazo pueden archivarse en Google Cloud Storage.
* **Elastic Stack (ELK)**: Para una solución autoalojada o en la nube, Elasticsearch, Logstash y Kibana proporcionan una plataforma robusta para la ingesta, almacenamiento, indexación y visualización de logs de Spark y Airflow.


##### Lectura e interpretación de logs

Interpretar logs de Spark y Airflow requiere familiaridad con los patrones de mensajes y la estructura jerárquica de la ejecución de trabajos y tareas. Reconocer los errores comunes y sus causas subyacentes te ayudará a diagnosticar problemas de manera más eficiente.

**Identificación de errores comunes en Spark**:

* Un error `Stage Failed` indica que una etapa (Stage) de tu job de Spark no pudo completarse. Esto puede ser debido a fallos en las tareas, problemas de memoria, errores de datos o configuración incorrecta.

    Si ves un mensaje como `Stage 2 failed in 20.0 s`, debes buscar en los logs de las tareas (Tasks) dentro de esa etapa para identificar el error específico, como `java.lang.OutOfMemoryError` o `org.apache.spark.SparkException: Task failed while writing rows`.

* El error*`TaskKilled` ocurre cuando una tarea es terminada prematuramente. Las razones pueden ser un tiempo de espera excedido (`timeout`), errores de memoria en el executor, o si el propio executor fue terminado.

    `TaskKilled (killed intentionally)` o `TaskKilled (killed due to memory limit exceeding)` indican que la tarea fue eliminada. Debes revisar los logs del executor para entender la causa raíz, como la configuración de `spark.executor.memory`.

* El error `Executor Lost` significa que un nodo de trabajo que aloja a un executor de Spark se ha perdido o ha fallado. Esto puede ser causado por problemas de red, fallos de hardware, problemas de memoria en el nodo o configuraciones incorrectas de recursos.

    `ERROR Driver: Lost executor 1 on <hostname>` sugiere que el executor se desconectó. Necesitas investigar los logs del nodo donde se ejecutaba el executor para encontrar la causa, como falta de memoria RAM o problemas de conectividad de red.

**Identificación de errores comunes en Airflow**:

* **Logs de operadores (tasks) con fallos de conexión**: Muchos DAGs interactúan con bases de datos, APIs o sistemas de archivos remotos. Los errores de conexión son comunes y suelen aparecer como excepciones de red o autenticación.

    Un log de una tarea de `PostgresOperator` mostrando `psycopg2.OperationalError: could not connect to server: Connection refused` indica un problema con las credenciales, la IP o el puerto de la base de datos.

* **Retries fallidos (`retries exhausted`)**: Airflow permite configurar reintentos para las tareas. Si una tarea agota todos sus reintentos, su estado final será `failed`.

    Ver `Task "my_task" failed after 3 retries` en los logs del scheduler o worker significa que la tarea no pudo completarse con éxito después de múltiples intentos. Debes analizar los logs de cada intento para identificar el error recurrente.

* **Fallos por `timeout`**: Si una tarea excede el tiempo de ejecución configurado (`execution_timeout`), Airflow la terminará.
    
    `[2025-06-06 19:13:51,123] {taskinstance.py:1150} ERROR - Task timed out: my_long_running_task` indica que la tarea tardó más de lo permitido. Es posible que necesites optimizar la tarea o aumentar el `execution_timeout` si el tiempo de ejecución es aceptable.

**Búsqueda de trazas que indiquen problemas con la configuración del cluster o variables mal definidas**:

* **Errores de classpath**: Problemas al cargar librerías o dependencias pueden causar que Spark jobs fallen al inicio.

    `java.lang.ClassNotFoundException: com.example.MyCustomClass` en los logs del driver o executor indica que una clase necesaria no está disponible en el classpath del cluster.

* **Variables de entorno incorrectas**: Una variable de entorno mal configurada puede afectar tanto a Spark como a Airflow.

    Un DAG de Airflow que depende de una variable de entorno `DB_PASSWORD` y falla con un `AuthenticationFailed` o similar, podría indicar que la variable no está definida o es incorrecta.

* **Configuración de recursos (`memory`, `cores`)**: Valores incorrectos en la configuración de memoria o CPU para Spark executors pueden llevar a errores de `OutOfMemoryError` o a un rendimiento deficiente.

    Mensajes como `Container killed by YARN for exceeding memory limits` sugieren que los recursos asignados no son suficientes para la carga de trabajo, y necesitas ajustar `spark.executor.memory` o `spark.driver.memory`.

La clave para una interpretación efectiva de los logs es la práctica y la familiaridad con tus propios pipelines y la infraestructura subyacente. Herramientas de visualización y agregación de logs pueden ser de gran ayuda para identificar patrones y correlacionar eventos.

### 3.5.2 Uso de interfaces gráficas (Airflow UI, Spark UI) para diagnóstico

Las **interfaces visuales** son herramientas cruciales para el monitoreo, diagnóstico y optimización de flujos de trabajo de Big Data. Proporcionan una vista estructurada y fácilmente navegable del estado de ejecución, los tiempos de procesamiento, las dependencias entre componentes y la identificación de posibles errores o cuellos de botella. Estas interfaces transforman datos complejos de logs y métricas en representaciones gráficas intuitivas, lo que facilita la toma de decisiones y la resolución de problemas.

##### Airflow UI

La interfaz de usuario de **Apache Airflow** es una herramienta esencial para la orquestación y monitoreo de flujos de trabajo (DAGs). Permite a los desarrolladores y operadores una visibilidad completa sobre el ciclo de vida de las tareas, desde su programación hasta su finalización o fallo.

**Utilizar el Tree View y Graph View para entender dependencias entre tareas.**

* **Tree View**: Esta vista ofrece una perspectiva cronológica del estado de cada instancia de tarea a lo largo del tiempo. Es ideal para identificar patrones de fallos recurrentes o para ver el historial de ejecuciones de un DAG. Por ejemplo, puedes observar si una tarea específica siempre falla en la misma hora del día o después de ciertas condiciones, lo que podría indicar un problema de recursos o una dependencia externa.
* **Graph View**: Proporciona una representación visual de las relaciones de dependencia entre las tareas dentro de un DAG. Es fundamental para entender el flujo lógico del trabajo. Si una tarea está tardando más de lo esperado, puedes ver rápidamente qué tareas están esperando su finalización y cómo esto impacta el flujo completo. Por ejemplo, si la tarea "transformar_datos" falla, el Graph View mostrará claramente que las tareas "cargar_a_dw" y "generar_reporte" no se ejecutarán hasta que "transformar_datos" se complete exitosamente.

**Revisar logs por tarea desde la interfaz y aplicar retries manuales.**

* Cada instancia de tarea en Airflow genera logs detallados que son accesibles directamente desde la UI. Estos logs son vitales para depurar errores, ya que contienen la salida estándar y los mensajes de error generados por la tarea. Por ejemplo, si una tarea de carga de datos falla, los logs podrían indicar un problema de conectividad con la base de datos o un error en el formato de los datos.
* Cuando una tarea falla debido a un problema transitorio (por ejemplo, un problema de red momentáneo), Airflow permite realizar **retries manuales** directamente desde la interfaz. Esto evita tener que esperar a la próxima ejecución programada o modificar el DAG, agilizando la recuperación de flujos de trabajo.

**Verificar el SLA Misses y la duración de ejecución por instancia.**

* **SLA Misses (Service Level Agreement Misses)**: Airflow permite definir SLAs para las tareas, lo que significa que esperas que una tarea se complete dentro de un tiempo determinado. Si una tarea excede este tiempo, Airflow lo marca como un "SLA Miss". La UI te permite ver un resumen de estas infracciones, lo que es crucial para identificar tareas que están afectando el rendimiento general o que necesitan optimización urgente. Por ejemplo, si la tarea "procesar_ventas_diarias" tiene un SLA de 30 minutos y consistentemente tarda 45 minutos, verás un SLA Miss, indicando que el proceso está tardando demasiado.
* **Duración de ejecución por instancia**: La Airflow UI muestra el tiempo que cada instancia de una tarea tardó en completarse. Al analizar estas duraciones a lo largo de varias ejecuciones, puedes identificar tendencias de rendimiento. Si una tarea que solía tardar 5 minutos ahora tarda 20, podría indicar un aumento en el volumen de datos, una degradación del rendimiento del sistema subyacente o un problema en el código de la tarea.

##### Spark UI

La **Spark UI** es la herramienta de monitoreo y diagnóstico por excelencia para aplicaciones Apache Spark. Proporciona una visión granular del progreso de los jobs, las etapas, las tareas y la utilización de recursos en el clúster, lo que es indispensable para la optimización del rendimiento y la depuración de problemas en el procesamiento de Big Data.

**Analizar el DAG físico y lógico del job para identificar etapas lentas.**

* **DAG Lógico**: La Spark UI muestra el plan lógico de ejecución de tu job, representando las transformaciones de RDDs o DataFrames. Esto te ayuda a entender cómo Spark interpreta tu código.
* **DAG Físico**: Esta es una representación de las etapas y tareas reales que Spark generará para ejecutar el plan lógico. Cada etapa se corresponde con una o más operaciones de "shuffle" o con la lectura de datos. La Spark UI permite visualizar el tiempo de ejecución de cada etapa. Si identificas una etapa que consume una cantidad desproporcionada de tiempo (por ejemplo, una etapa de `join` o `groupByKey`), esto indica un posible cuello de botella. Por ejemplo, una etapa de `shuffle` excepcionalmente lenta puede indicar un problema de particionamiento de datos o una red saturada.

**Identificar tareas que consumen más recursos o que están desbalanceadas.**

* Dentro de cada etapa, la Spark UI desglosa el rendimiento a nivel de **tarea**. Puedes ver métricas como el tiempo de ejecución de cada tarea, la cantidad de datos leídos/escritos, y la utilización de memoria y CPU.
* **Tareas desbalanceadas (Skew)**: Si observas que la mayoría de las tareas en una etapa se completan rápidamente, pero unas pocas tardan significativamente más (por ejemplo, una tarea tarda 5 minutos mientras que el resto tarda 10 segundos), esto es un claro indicio de **sesgo de datos (data skew)**. Esto significa que algunos ejecutores están procesando una cantidad desproporcionadamente grande de datos debido a valores clave no distribuidos uniformemente. La Spark UI te permite identificar estas tareas "lentas" para luego aplicar técnicas de optimización como salting o re-particionamiento.

**Revisar el Event Timeline para ver eventos críticos de ejecución.**

* El **Event Timeline** de la Spark UI es una representación visual cronológica de los eventos clave que ocurren durante la ejecución de un job Spark. Esto incluye el inicio y fin de ejecutores, la asignación y liberación de recursos, el inicio y fin de etapas, y los eventos de shuffle.
* Analizar esta línea de tiempo es crucial para detectar problemas a nivel de clúster. Por ejemplo, si observas un gran número de eventos de "Executor Lost" (ejecutor perdido), podría indicar problemas de memoria en los nodos del clúster o inestabilidad de la red. Si hay largos periodos de inactividad entre etapas, podría sugerir problemas con la asignación de recursos o con la disponibilidad de datos. Esta vista te ayuda a entender cómo los recursos del clúster están siendo utilizados a lo largo del tiempo y a identificar periodos de inactividad o contención.

### 3.5.3 Métricas clave: tiempo de ejecución, throughput, retries

El seguimiento de métricas clave es esencial para establecer la salud operativa, la eficiencia y la confiabilidad de los pipelines de datos. Estas métricas nos permiten identificar cuellos de botella, predecir fallos y optimizar el uso de recursos.

##### Métricas de rendimiento

Las métricas de rendimiento nos indican qué tan rápido y eficientemente se están ejecutando nuestros pipelines.

**Tiempo de ejecución total del DAG/job y por tarea individual**:

Se refiere al tiempo que tarda un flujo completo de trabajo (DAG o job) en finalizar, así como el tiempo que consume cada una de las tareas que lo componen. Monitorear ambos niveles nos permite identificar tareas que están tardando más de lo esperado y que podrían ser optimizadas.

* Un pipeline de ETL que carga datos de un sistema transaccional a un data warehouse. Si el tiempo total de ejecución aumenta de 30 minutos a 2 horas, necesitamos investigar. Al revisar los tiempos individuales de las tareas, podríamos descubrir que una tarea de `JOIN` en Spark ha pasado de 5 minutos a 1 hora.

  En Spark, el `Spark UI` proporciona detalles sobre el tiempo de ejecución de cada etapa y tarea. A nivel de código, puedes registrar el tiempo:

```python
from datetime import datetime

start_time_job = datetime.now()

# ... código de tu job Spark ...
df_transformed = spark.sql("SELECT ... FROM large_table JOIN another_table ON ...")
df_transformed.write.parquet("s3://data-lake/processed/data.parquet")
# ...

end_time_job = datetime.now()
print(f"Tiempo total de ejecución del job: {end_time_job - start_time_job}")

# Para una tarea específica (ej. una transformación costosa)
start_time_task_join = datetime.now()
df_joined = large_df.join(small_df, "key")
end_time_task_join = datetime.now()
print(f"Tiempo de ejecución de la tarea JOIN: {end_time_task_join - start_time_task_join}")
```

**Throughput (datos procesados por unidad de tiempo)**:

Representa la cantidad de datos que el pipeline es capaz de procesar en un determinado período. Es una métrica crucial para entender la capacidad y la escalabilidad del sistema. Se puede medir en filas por segundo, megabytes por segundo, etc.

* Un pipeline de ingesta de logs en tiempo real que procesa 10 GB de datos por hora. Si este throughput disminuye a 2 GB por hora, indica un problema en la ingesta o en el procesamiento.

  Para una carga de datos:

```python
# Suponiendo que 'rows_processed' es el número total de filas procesadas
# y 'execution_time_seconds' es el tiempo total de ejecución en segundos
rows_processed = 1_000_000
execution_time_seconds = 60 # 1 minuto

throughput_rows_per_second = rows_processed / execution_time_seconds
print(f"Throughput: {throughput_rows_per_second:.2f} filas/segundo")

# Para datos en bytes (ej. leyendo un archivo grande)
file_size_bytes = 10 * 1024 * 1024 # 10 MB
throughput_mb_per_second = (file_size_bytes / (1024 * 1024)) / execution_time_seconds
print(f"Throughput: {throughput_mb_per_second:.2f} MB/segundo")
```

**Latencia entre tareas, importante en flujos encadenados**:

Se refiere al tiempo de espera entre la finalización de una tarea y el inicio de la siguiente. En pipelines donde las tareas dependen secuencialmente unas de otras, una alta latencia puede acumularse y prolongar significativamente el tiempo total de ejecución del DAG. Esto es especialmente crítico en arquitecturas de microservicios o pipelines de streaming.

* En un pipeline de Airflow, si la `Task A` finaliza a las 10:00 AM y la `Task B` (que depende de `Task A`) comienza a las 10:15 AM, la latencia entre tareas es de 15 minutos. Un monitoreo de esta métrica puede revelar que los recursos del worker o el scheduler están saturados.

  Airflow registra los tiempos de inicio y fin de cada tarea. La latencia se calcularía como:

  `start_time(Task B) - end_time(Task A)`

  Puedes obtener estos datos de la base de datos de metadatos de Airflow o a través de su API. Herramientas de monitoreo como Prometheus y Grafana se integrarían para visualizar estas brechas.

##### Métricas de confiabilidad y robustez

Estas métricas nos informan sobre la estabilidad y la capacidad de nuestro sistema para manejar errores y recuperarse de ellos.

**Número de reintentos (retries) y su tasa de éxito**:

Indica cuántas veces una tarea tuvo que ser reejecutada debido a un fallo transitorio. Un alto número de reintentos puede indicar problemas subyacentes (ej. inestabilidad de la red, picos de carga en sistemas externos) que, aunque se recuperen, afectan la eficiencia y la latencia. La tasa de éxito de los reintentos (cuántos reintentos finalmente lograron que la tarea se completara con éxito) es crucial para entender la eficacia de nuestra estrategia de reintentos.

* Si una tarea que escribe a una base de datos externa falla intermitentemente y necesita 3 reintentos para completarse en el 80% de los casos, esto es aceptable. Sin embargo, si en el 50% de los casos agota todos los reintentos y falla definitivamente, indica un problema grave.

```python
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='example_retries',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 3,  # Número de reintentos
        'retry_delay': timedelta(minutes=5), # Retraso entre reintentos
    }
) as dag:
    start_task = DummyOperator(task_id='start')
    failing_task = DummyOperator(task_id='potentially_failing_task') # Esta tarea simularía un fallo
    end_task = DummyOperator(task_id='end')

    start_task >> failing_task >> end_task
```

Las plataformas como Airflow exponen el número de reintentos en su interfaz de usuario y en sus logs.

**Errores por tipo (conexión, permisos, timeouts)**:

Clasificar los errores por su naturaleza es fundamental para una depuración efectiva y para identificar patrones.

* **Errores de conexión**: Indican problemas de red, bases de datos no disponibles o servicios externos caídos.
  * `Connection refused` al intentar conectar con una API, o `Network unreachable` al acceder a un bucket S3.

* **Errores de permisos**: Apuntan a problemas de autorización o configuración de seguridad.
  * `Access Denied` al intentar leer de un bucket de S3 sin los permisos adecuados, o `Permission denied` al escribir en un directorio.

* **Timeouts**: Sugieren que una operación está tardando demasiado en responder, lo que podría deberse a cuellos de botella en el sistema llamado, a una alta latencia de red o a una configuración de timeout demasiado baja.
  * Una consulta a una base de datos que excede el tiempo límite de espera configurado (`Query timeout expired`).

Estos errores se capturan en los logs de las aplicaciones (Spark, Python scripts, etc.). Utilizar sistemas de log management (ELK Stack, Splunk, Datadog) con análisis de patrones y alertas es crucial.

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Simulación de un error de conexión
    # requests.get("http://nonexistent-service.com", timeout=1)
    raise ConnectionError("Fallo de conexión al servicio externo.")
except ConnectionError as e:
    logger.error(f"Error de conexión: {e}")
except PermissionError as e:
    logger.error(f"Error de permisos: {e}")
except Exception as e:
    logger.error(f"Error inesperado: {type(e).__name__} - {e}")
```

**Disponibilidad y uptime del scheduler (Airflow)**:

En sistemas orquestadores como Airflow, la disponibilidad del scheduler es crítica, ya que es el componente encargado de determinar cuándo deben ejecutarse los DAGs y sus tareas. Un scheduler caído o inestable detendrá por completo la ejecución de todos los pipelines. El uptime es el porcentaje de tiempo que el scheduler ha estado operativo.

* Si el scheduler de Airflow estuvo caído durante 2 horas en un día, su disponibilidad ese día fue de `(22/24) * 100 = 91.67%`, lo cual es inaceptable para la mayoría de entornos de producción.

  Monitoreo a nivel de sistema operativo (CPU, memoria, procesos) del servidor donde corre el scheduler. Herramientas de monitoreo de infraestructura (Prometheus, Grafana, Datadog) pueden rastrear el estado del proceso del scheduler y generar alertas si se detiene o si sus métricas de salud (como la cola de tareas pendientes) son inusuales. Airflow también tiene métricas internas que pueden exportarse a sistemas de monitoreo.

### 3.5.4 Alertas, notificaciones y automatización de recuperación

El monitoreo proactivo es fundamental en cualquier pipeline de datos robusto. Permite no solo detectar problemas de manera temprana, sino también reaccionar ante errores de forma automatizada, minimizando la intervención humana directa y reduciendo el tiempo de inactividad. Esto es crucial para mantener la integridad y disponibilidad de los datos.

##### Configuración de alertas y notificaciones

La configuración de alertas y notificaciones es el primer paso para un monitoreo proactivo eficaz. Permite que los equipos sean informados de inmediato sobre cualquier anomalía o fallo en los pipelines de datos.

**Integración de notificaciones desde Airflow**:

Airflow, siendo una orquestador de ETL ampliamente utilizado, ofrece operadores predefinidos para enviar notificaciones a diferentes canales.

* **Notificaciones vía correo electrónico (EmailOperator)**: Es una forma estándar y efectiva de alertar a los equipos. Se puede configurar para enviar correos electrónicos al fracaso o éxito de una tarea o DAG.

```python
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email

# ... dentro de la definición de un DAG o tarea
send_email_on_failure = EmailOperator(
    task_id='email_on_failure',
    to='equipo_data@example.com',
    subject='Alerta Airflow: Tarea {{ ti.task_id }} falló en DAG {{ ti.dag_id }}',
    html_content="""
    <h3>Error en Pipeline de Datos</h3>
    <p>La tarea <b>{{ ti.task_id }}</b> del DAG <b>{{ ti.dag_id }}</b> ha fallado.</p>
    <p>Revisar logs para más detalles: {{ ti.log_url }}</p>
    """,
    dag=my_dag, # Asociar al DAG si es una tarea separada, o parte de la configuración de default_args
    trigger_rule='one_failed' # Se dispara solo si alguna tarea en el DAG falla
)
```

Alternativamente, se pueden configurar `default_args` para enviar correos electrónicos en caso de fallo:

```python
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['equipo_data@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,
}
```

**Notificaciones vía Slack (SlackWebhookOperator)**:

Slack es una herramienta de comunicación muy común en equipos de desarrollo y operaciones. Airflow permite enviar mensajes directamente a canales de Slack, lo que facilita una comunicación más inmediata y contextualizada.

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Configurar la conexión de Slack en Airflow (Admin -> Connections)
# conn_id: slack_connection, host: https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX

send_slack_notification = SlackWebhookOperator(
    task_id='slack_notification',
    slack_webhook_conn_id='slack_connection', # ID de la conexión configurada en Airflow
    message="""
    :red_circle: *Alerta en Pipeline de Datos*: La tarea `{{ ti.task_id }}` del DAG `{{ ti.dag_id }}` ha fallado.
    Por favor, revisa los logs en: {{ ti.log_url }}
    """,
    channel='#alerts_data_pipeline', # Canal de Slack al que se enviará el mensaje
    dag=my_dag,
    trigger_rule='one_failed'
    )
```

**Notificaciones vía Webhook**:

Para integraciones con sistemas personalizados o herramientas no directamente soportadas por operadores de Airflow, los webhooks ofrecen una gran flexibilidad. Permiten enviar una petición HTTP (POST) a una URL específica con un payload de datos.

```python
from airflow.operators.http import SimpleHttpOperator
import json

send_custom_alert = SimpleHttpOperator(
    task_id='send_custom_alert',
    http_conn_id='custom_alert_api', # Conexión HTTP configurada en Airflow
    endpoint='/api/v1/alerts',
    method='POST',
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "severity": "critical",
        "message": "Fallo en el procesamiento de datos del DAG {{ ti.dag_id }}",
        "dag_id": "{{ ti.dag_id }}",
        "task_id": "{{ ti.task_id }}",
        "log_url": "{{ ti.log_url }}"
    }),
    dag=my_dag,
    trigger_rule='one_failed'
)
```

**Configuración de alertas de métricas anómalas en herramientas externas (Grafana, Datadog)**:

Más allá de las fallas de ejecución, es vital monitorear el rendimiento y la calidad de los datos. Herramientas de monitoreo como Grafana y Datadog permiten configurar alertas basadas en métricas recolectadas de los pipelines.

* **Grafana**: Se pueden configurar paneles en Grafana para visualizar métricas como el tiempo de ejecución de las tareas, el número de registros procesados, la latencia de ingestión, etc. Luego, se definen umbrales y reglas de alerta. Por ejemplo, si el tiempo de ejecución de un DAG excede un umbral predefinido (indicando un cuello de botella o problema de rendimiento), Grafana puede enviar una notificación a Slack o correo electrónico.

    * **Métricas a monitorear**: `airflow.task_duration`, `airflow.dag_runs_succeeded`, `airflow.dag_runs_failed`, `spark.driver.memoryUsage`, `spark.executor.bytesRead`.
    * **Regla de Alerta**: "Si el `airflow.task_duration` promedio para el DAG 'data_ingestion' en los últimos 15 minutos es > 300 segundos, enviar alerta".

* **Datadog**: Datadog ofrece agentes que recolectan métricas de sistemas, bases de datos y aplicaciones (incluyendo Airflow y Spark). Se pueden crear "Monitors" para detectar anomalías.

    * **Monitoreo de latencia**: "Alertar si la latencia de los datos en la tabla 'clientes' (medida por la diferencia entre la marca de tiempo de ingestión y la marca de tiempo actual) es superior a 1 hora durante 5 minutos consecutivos."
    * **Monitoreo de volumen de datos**: "Alertar si el número de registros procesados por el job de Spark 'transform_sales' cae un 20% respecto a la media de la última semana."

##### Automatización de recuperación

La automatización de la recuperación es clave para construir pipelines resilientes que puedan manejar fallos de manera autónoma, reduciendo la necesidad de intervención manual y el impacto en la disponibilidad de los datos.

**Configurar retries automáticos y escalonados (exponential backoff)**:

Los reintentos son un mecanismo fundamental para manejar fallos transitorios (ej. problemas de red, saturación de la base de datos). El "exponential backoff" (retroceso exponencial) es una estrategia inteligente para los reintentos. En lugar de reintentar inmediatamente, se espera un período de tiempo que aumenta exponencialmente con cada reintento, lo que da más tiempo al sistema para recuperarse y evita sobrecargar un servicio ya inestable.

```python
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='example_retries_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 5, # Número de reintentos
        'retry_delay': timedelta(minutes=2), # Retraso inicial entre reintentos
        'retry_exponential_backoff': True, # Habilitar el retroceso exponencial
        'max_retry_delay': timedelta(hours=1) # Retraso máximo entre reintentos
    }
) as dag:
    failing_task = BashOperator(
        task_id='simulate_failing_task',
        # Este comando fallará intencionadamente para demostrar los reintentos
        bash_command='exit 1',
    )
```

En este ejemplo, la tarea intentará ejecutarse 5 veces. El primer reintento ocurrirá después de 2 minutos, el segundo después de 4 minutos, el tercero después de 8 minutos, y así sucesivamente (hasta un máximo de 1 hora).

**Aplicar mecanismos de fallback (ej. cargar datos alternativos)**:

En situaciones donde un componente crítico del pipeline falla y los reintentos no son suficientes, un mecanismo de fallback permite que el pipeline continúe operando, aunque sea con un conjunto de datos alternativo o con un rendimiento reducido. Esto es particularmente útil para dashboards o aplicaciones que requieren disponibilidad constante.

* Si el proceso ETL principal para cargar datos de ventas en tiempo real falla debido a un problema con el API de origen, se podría configurar un fallback para cargar los datos de ventas del día anterior desde un archivo plano o una base de datos de respaldo ya procesada. Esto asegura que los reportes de ventas no estén completamente vacíos, aunque no sean los más actualizados.

  En Airflow, esto se podría lograr con `BranchPythonOperator` o usando un `trigger_rule` específico en una tarea de fallback.

```python
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def check_main_etl_status(**kwargs):
    ti = kwargs['ti']
    # Obtener el estado de la tarea principal de ETL
    main_etl_success = ti.xcom_pull(task_ids='main_sales_etl', key='return_value')
    if main_etl_success:
        return 'continue_with_main_data'
    else:
        return 'load_fallback_data'

    with DAG(
        dag_id='sales_data_pipeline_with_fallback',
        start_date=datetime(2023, 1, 1),
        schedule_interval=timedelta(hours=1),
        catchup=False
    ) as dag:
        main_sales_etl = BashOperator(
            task_id='main_sales_etl',
            bash_command='python /app/scripts/run_realtime_sales_etl.py', # Simula una tarea que puede fallar
            retries=3,
            retry_delay=timedelta(minutes=1),
            do_xcom_push=True # Para que el estado de éxito se pueda leer
        )

        check_status = BranchPythonOperator(
            task_id='check_main_etl_status',
            python_callable=check_main_etl_status,
            provide_context=True,
        )

        continue_with_main_data = BashOperator(
            task_id='continue_with_main_data',
            bash_command='echo "Procesando datos de ventas en tiempo real..."',
            trigger_rule=TriggerRule.ONE_SUCCESS # Se ejecuta si check_status devuelve este ID
        )

        load_fallback_data = BashOperator(
            task_id='load_fallback_data',
            bash_command='python /app/scripts/load_yesterday_sales_data.py', # Script para cargar datos alternativos
            trigger_rule=TriggerRule.ONE_SUCCESS # Se ejecuta si check_status devuelve este ID
        )

        main_sales_etl >> check_status
        check_status >> [continue_with_main_data, load_fallback_data]
```

**Pausar DAGs automáticamente ante errores críticos**:

En situaciones donde un error es persistente y fundamental (ej. un problema de autenticación con una base de datos crítica que no se resuelve con reintentos), es mejor pausar el DAG automáticamente. Esto previene la ejecución continua de tareas que inevitablemente fallarán, generando ruido en los logs y consumiendo recursos innecesariamente.

* **Airflow**: Airflow no tiene un operador nativo para "pausar DAGs automáticamente". Sin embargo, se puede implementar esta lógica utilizando un `PythonOperator` que interactúe con la API de Airflow o directamente con la base de datos de Airflow (con precaución).

```python
    from airflow.operators.python import PythonOperator
    from airflow.api.client.local_client import Client # Necesitarías la configuración adecuada para usar el cliente local
    import logging

    log = logging.getLogger(__name__)

    def pause_dag_on_critical_error(**kwargs):
        dag_id = kwargs['dag_run'].dag_id
        ti = kwargs['ti']
        # Condición para pausar: Si la tarea falló y ya se agotaron los reintentos (o es un error específico)
        if ti.current_retries >= ti.max_tries: # O alguna otra lógica para determinar un error crítico
            log.info(f"Error crítico detectado en DAG {dag_id}. Pausando DAG...")
            # Instanciar el cliente de Airflow para interactuar con la API
            # Asegúrate de que Airflow esté configurado para permitir esta operación.
            # En un entorno de producción, es preferible usar la API REST de Airflow con autenticación.
            try:
                client = Client(None, None) # Placeholder. En producción, configurar conexión a Airflow API.
                client.set_dag_paused(dag_id=dag_id, is_paused=True)
                log.info(f"DAG '{dag_id}' pausado exitosamente.")
            except Exception as e:
                log.error(f"Error al intentar pausar el DAG '{dag_id}': {e}")
        else:
            log.info(f"Error en DAG {dag_id}, pero no crítico para pausar.")

    with DAG(
        dag_id='critical_error_handling_dag',
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args={
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        }
    ) as dag:
        critical_task = BashOperator(
            task_id='critical_database_access',
            bash_command='python /app/scripts/access_critical_db.py', # Simula una falla recurrente de DB
        )

        check_and_pause_dag = PythonOperator(
            task_id='check_and_pause_dag',
            python_callable=pause_dag_on_critical_error,
            provide_context=True,
            trigger_rule='all_failed' # Se ejecuta solo si la tarea anterior falla y no tiene más reintentos
        )

        critical_task >> check_and_pause_dag
```

Es importante destacar que la interacción con la API de Airflow desde dentro de un DAG requiere una configuración de seguridad adecuada y permisos. Para entornos productivos, es más robusto usar el cliente REST de Airflow con autenticación, o un sensor externo que monitoree los estados de los DAGs y actúe en consecuencia.

### 3.5.5 Troubleshooting común

Dominar la resolución de fallos típicos acelera la recuperación y mejora la resiliencia del sistema, minimizando el impacto en la operación y los tiempos de inactividad.

##### Tiempos largos de ejecución

Cuando los jobs de Spark tardan más de lo esperado, es crucial identificar la causa raíz.

**Diagnóstico de cuellos de botella en etapas específicas del DAG (Directed Acyclic Graph)**:

El DAG de Spark visualiza las transformaciones y acciones del job. Un análisis detallado permite identificar qué etapas están consumiendo más tiempo. Esto se puede lograr a través de la interfaz de usuario de Spark (Spark UI) o analizando los logs.

* Si observamos que la etapa de `shuffle` es la que más tiempo toma, podría indicar una gran cantidad de datos siendo transferidos entre los nodos, lo que sugiere una posible necesidad de optimización de las operaciones de agregación o joins.
* La pestaña "Stages" en la Spark UI proporciona una visión detallada del tiempo de ejecución de cada etapa, el tiempo de CPU, los datos leídos/escritos, etc.

**Optimización del paralelismo y tuning de recursos en Spark**: 

Ajustar el número de ejecutores, núcleos por ejecutor y la memoria asignada puede tener un impacto significativo.

* Un paralelismo insuficiente puede llevar a que los recursos no se utilicen por completo, mientras que un paralelismo excesivo puede generar una sobrecarga de tareas y una gestión ineficiente. Se puede ajustar el `spark.sql.shuffle.partitions` para operaciones como joins o agregaciones.

* **Tuning de recursos**:

    * `spark.executor.instances`: Número de ejecutores.
    * `spark.executor.cores`: Número de núcleos por ejecutor.
    * `spark.executor.memory`: Memoria asignada a cada ejecutor.
    * `spark.driver.memory`: Memoria asignada al driver.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("LongRunningJobOptimization") \
        .config("spark.executor.instances", "10") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
```

En este ejemplo, estamos configurando Spark para usar 10 ejecutores, cada uno con 4 núcleos y 8 GB de memoria, y estableciendo el número de particiones de shuffle en 200.

##### Fallos por particiones desbalanceadas

Las particiones desbalanceadas (data skew) pueden causar problemas de rendimiento severos, donde uno o pocos ejecutores terminan haciendo la mayor parte del trabajo, convirtiéndose en cuellos de botella.

**Evaluación del tamaño y distribución de particiones**:

Es fundamental entender cómo se distribuyen los datos entre las particiones. Esto se puede hacer inspeccionando los logs de Spark o utilizando herramientas como `df.rdd.getNumPartitions()` y `df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show()`.

* Si un `count()` en una columna específica muestra un valor muy alto para una única partición, es un claro indicador de desbalanceo.

```python
from pyspark.sql.functions import spark_partition_id

# Suponiendo que 'df' es tu DataFrame
df.withColumn("partitionId", spark_partition_id()) \
    .groupBy("partitionId") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()
```

Esto mostrará el número de registros en cada partición, permitiendo identificar rápidamente las particiones con un tamaño desproporcionado.

**Uso de `repartition()` o `coalesce()` para mejorar el rendimiento**:

* **`repartition(numPartitions, *cols)`**: Redistribuye los datos entre un número especificado de particiones. Es una operación de `shuffle` completa, lo que significa que mueve todos los datos entre los nodos. Es útil cuando se necesita aumentar o disminuir drásticamente el número de particiones o cuando se desea re-distribuir los datos basados en una o más columnas para evitar el desbalanceo.

```python
# Suponiendo que 'df' tiene una columna 'customer_id' que está causando skew
df_repartitioned = df.repartition(200, "customer_id")
# Ahora, las operaciones subsiguientes en customer_id deberían ser más balanceadas
```
* **`coalesce(numPartitions)`**: Reduce el número de particiones de un DataFrame de forma más eficiente que `repartition()` porque evita un shuffle completo de los datos. Solo puede reducir el número de particiones. Es útil cuando se han realizado muchas transformaciones que han creado un gran número de particiones pequeñas.

```python
# Si tienes demasiadas particiones pequeñas después de algunas transformaciones
df_coalesced = df.coalesce(50)
```

* **Consideraciones**: Elegir entre `repartition` y `coalesce` depende del escenario. Si el desbalanceo es severo y necesitas una redistribución completa o si necesitas un mayor número de particiones, `repartition` es la elección. Si solo necesitas reducir el número de particiones sin un shuffle costoso, `coalesce` es más eficiente.

##### Problemas de red o acceso a fuentes

Los problemas de conectividad o acceso a datos externos son comunes en entornos de Big Data y pueden llevar a fallos de jobs aparentemente inexplicables.

* **Verificación de credenciales, endpoints y latencias**:

    * **Credenciales**: Asegurarse de que las credenciales de acceso (claves API, nombres de usuario/contraseñas, tokens OAuth) para las fuentes de datos (bases de datos, S3, HDFS, Kafka, etc.) sean correctas y tengan los permisos adecuados.
    * **Endpoints**: Confirmar que los URLs, IPs o nombres de host de las fuentes de datos sean correctos y accesibles desde los nodos del cluster Spark.
    * **Latencias**: Las altas latencias de red pueden ralentizar significativamente la lectura de datos o la escritura de resultados. Se pueden usar herramientas como `ping`, `traceroute` o herramientas de monitoreo de red para diagnosticar esto.
    * **Ejemplo**: Un job que falla al intentar leer datos de un bucket S3 puede deberse a que las credenciales IAM no son válidas o el rol asumido no tiene permisos de lectura para ese bucket.

* **Implementación de retries y validaciones previas de conectividad**:

    * **Retries (Reintentos)**: Configurar el cliente o conector de la fuente de datos para reintentar automáticamente la conexión o la operación en caso de fallos transitorios (ej., timeouts de red). Muchos conectores de Spark (JDBC, S3) tienen opciones de reintento configurables.
    * **Validaciones previas de conectividad**: Antes de iniciar operaciones de lectura/escritura a gran escala, realizar una pequeña prueba de conectividad. Esto puede ser un simple ping a la base de datos o un intento de listar un pequeño número de archivos en un directorio de S3.

```python
# Ejemplo de lectura desde una base de datos con reintentos configurados
jdbc_url = "jdbc:postgresql://your_database_host:5432/your_database"
connection_properties = {
    "user": "your_user",
    "password": "your_password",
    "driver": "org.postgresql.Driver",
    "reconnect": "true",  # Algunas bases de datos soportan esto
    "retries": "3",       # Configuración a nivel de conector o driver
    "loginTimeout": "30"  # Timeout de conexión
}

try:
    df_jdbc = spark.read.jdbc(url=jdbc_url, table="your_table", properties=connection_properties)
    df_jdbc.show()
except Exception as e:
    print(f"Error al conectar o leer desde la base de datos: {e}")
    # Lógica para manejar el fallo, quizás enviar una alerta
```

* **Validación de conectividad a S3 (ejemplo conceptual)**:

```python
import boto3
from botocore.exceptions import ClientError

# Suponiendo que ya tienes tus credenciales AWS configuradas
s3_client = boto3.client('s3')
bucket_name = "your-s3-bucket"

try:
    # Intentar listar un prefijo o verificar la existencia del bucket
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"Conexión exitosa al bucket S3: {bucket_name}")
    # Proceder con la lectura de datos con Spark
    df_s3 = spark.read.parquet(f"s3a://{bucket_name}/path/to/data/")
    df_s3.show()
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        print(f"Bucket S3 '{bucket_name}' no encontrado o no accesible.")
    elif error_code == '403':
        print(f"Permisos insuficientes para acceder al bucket S3 '{bucket_name}'.")
    else:
        print(f"Error de S3 desconocido: {e}")
        # Lógica para manejar el fallo, evitar la ejecución del job de Spark
    except Exception as e:
    print(f"Error general al intentar acceder a S3: {e}")
```

## Tarea

1. Configura un DAG en Airflow con logging personalizado y verifica la salida de logs para una ejecución exitosa y una fallida.
2. Ejecuta un job en Apache Spark que procese datos particionados de forma desigual. Revisa el Spark UI e identifica cuellos de botella.
3. Simula un error de red al conectar con una fuente de datos externa en un DAG de Airflow. Implementa un retry automático y registra el comportamiento en logs.
4. Configura alertas por correo para fallos críticos en un pipeline ETL usando `EmailOperator` en Airflow.
5. Analiza métricas de throughput y retries de un flujo ETL histórico y propone ajustes de recursos o paralelismo para mejorar su eficiencia.

