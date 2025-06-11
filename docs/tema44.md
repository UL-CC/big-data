# 4. Automatización y Orquestación con Apache Airflow

## 4.4. Monitoreo, logging y manejo de dependencias

**Objetivo**:

Comprender y aplicar técnicas de monitoreo, logging y gestión de dependencias en Apache Airflow para asegurar la trazabilidad, confiabilidad y eficiencia en la ejecución de flujos de trabajo de datos distribuidos.

**Introducción**:

En entornos de Big Data, donde los pipelines pueden involucrar decenas o cientos de tareas interdependientes, es fundamental contar con mecanismos robustos para gestionar dependencias, detectar errores rápidamente y recibir notificaciones oportunas. Apache Airflow proporciona una infraestructura poderosa para monitorear y depurar flujos de trabajo, manejar relaciones entre tareas, establecer alertas ante fallas, y generar registros detallados para su análisis.

**Desarrollo**:

Este tema se enfoca en tres aspectos clave de la operación de DAGs en producción: cómo definir y controlar las dependencias entre tareas y DAGs, cómo configurar sistemas de notificaciones que alerten sobre estados críticos, y cómo aprovechar las capacidades de logging de Airflow para identificar y solucionar errores. Dominar estos elementos es esencial para mantener la confiabilidad y visibilidad de los pipelines en ecosistemas Big Data complejos, tanto on-premise como en la nube.


### 4.4.1 Gestión de dependencias entre tareas y DAGs

La gestión de dependencias en Airflow es crucial para asegurar que las tareas se ejecuten en el orden correcto, respetando los flujos lógicos del proceso. Las dependencias pueden definirse tanto dentro de un DAG como entre diferentes DAGs, y su correcta implementación mejora la modularidad, reutilización y control de los flujos.

##### Definición de dependencias entre tareas en un DAG

Airflow utiliza un modelo basado en grafos dirigidos acíclicos (DAGs), donde las tareas están conectadas entre sí por medio de relaciones de dependencia. Estas se definen de forma declarativa usando los operadores `>>` (downstream) y `<<` (upstream).

```python
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(dag_id="ejemplo_dependencias", start_date=datetime(2024, 1, 1), schedule_interval='@daily') as dag:
    inicio = DummyOperator(task_id="inicio")
    proceso_1 = DummyOperator(task_id="proceso_1")
    proceso_2 = DummyOperator(task_id="proceso_2")
    fin = DummyOperator(task_id="fin")

    inicio >> [proceso_1, proceso_2] >> fin
```

Este patrón garantiza que `proceso_1` y `proceso_2` solo inicien después de `inicio`, y que `fin` espere la finalización de ambos.

##### Dependencias entre DAGs (TriggerDagRunOperator y ExternalTaskSensor)

Airflow permite establecer dependencias entre DAGs distintos. Esto es útil para dividir procesos complejos en módulos independientes.

* `TriggerDagRunOperator`: inicia otro DAG desde una tarea.
* `ExternalTaskSensor`: espera la finalización de una tarea en otro DAG.

```python
from airflow.operators.dagrun_operator import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_otros_dag",
    trigger_dag_id="subdag_etl",
    dag=dag
)
```


### 4.4.2 Sistema de alertas y notificaciones

Airflow permite configurar alertas automáticas que se disparan cuando las tareas fallan, se retrasan o se comportan de forma inesperada. Estas alertas pueden enviarse por correo electrónico, Slack, Microsoft Teams u otros canales, facilitando la respuesta rápida a incidentes.

##### Configuración básica de notificaciones por correo electrónico

Se pueden configurar notificaciones usando los parámetros `email`, `email_on_failure`, `email_on_retry` en la definición de tareas.

```python
from airflow.operators.dummy import DummyOperator

DummyOperator(
    task_id="tarea_con_alerta",
    email=["alertas@empresa.com"],
    email_on_failure=True,
    retries=1,
    retry_delay=timedelta(minutes=5),
    dag=dag
)
```

A nivel global, se configuran en `airflow.cfg` o variables de entorno (`SMTP_HOST`, `SMTP_USER`, etc.).

##### Integración con herramientas de monitoreo y mensajería

Airflow puede integrarse con servicios como Slack o Microsoft Teams usando hooks o callbacks personalizados.

**Callback para Slack:**

```python
def enviar_mensaje_slack(context):
    mensaje = f"Tarea {context['task_instance_key_str']} falló."
    # Aquí se llamaría un webhook Slack

from airflow.operators.python import PythonOperator

tarea = PythonOperator(
    task_id="tarea_notificada",
    python_callable=mi_funcion,
    on_failure_callback=enviar_mensaje_slack,
    dag=dag
)
```

También existen integraciones listas como \[SlackWebhookOperator].


### 4.4.3 Logging y debugging

El sistema de logging de Airflow permite capturar, visualizar y analizar los eventos que ocurren durante la ejecución de tareas. Estos logs son fundamentales para depurar errores, evaluar el comportamiento de los DAGs y hacer troubleshooting en producción.

##### Visualización de logs por tarea en la UI

Cada tarea ejecutada genera un log accesible desde la interfaz web. Estos logs pueden ser configurados para almacenarse localmente o en la nube (S3, GCS, Azure Blob).

Configuración en `airflow.cfg`:

```ini
[logging]
base_log_folder = /opt/airflow/logs
remote_logging = True
remote_log_conn_id = MyS3Connection
remote_base_log_folder = s3://airflow-logs
```

Esto asegura que los logs estén centralizados y disponibles para auditoría.

##### Niveles de logging y debugging en desarrollo

Durante la fase de desarrollo, es común usar niveles de logging como `DEBUG`, `INFO`, `WARNING`, `ERROR`.

```python
import logging

def mi_funcion(**kwargs):
    logging.info("Inicio de la función")
    try:
        resultado = 1 / 0
    except ZeroDivisionError:
        logging.error("División por cero detectada", exc_info=True)
```

También es posible ejecutar DAGs en modo manual (`airflow dags test`) para obtener retroalimentación inmediata sin esperar al scheduler.


## Tarea

Desarrolla los siguientes ejercicios prácticos:

1. Diseña un DAG con 5 tareas, donde se evidencie la gestión correcta de dependencias usando `>>` y `<<`.
2. Implementa un DAG maestro que dispare otro DAG secundario mediante `TriggerDagRunOperator` y asegúrate de que este último se ejecute correctamente.
3. Configura una tarea que envíe una notificación por correo electrónico en caso de fallo. Prueba intencionalmente el fallo para verificar el envío.
4. Integra una alerta personalizada usando una función Python que simule el envío de un mensaje a Slack o Teams en caso de fallo.
5. Revisa los logs de ejecución de un DAG que falle, identifica el error, corrígelo y vuelve a ejecutar. Adjunta capturas o fragmentos del log que justifiquen tu análisis.

