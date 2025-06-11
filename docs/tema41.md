# 4. Automatización y Orquestación con Apache Airflow

## Tema 4.1. Arquitectura y componentes de Airflow

**Objetivo**:

Comprender y analizar los componentes fundamentales de Apache Airflow, su arquitectura interna y las configuraciones necesarias para su implementación, con el fin de diseñar flujos de trabajo robustos, escalables y mantenibles en entornos de Big Data.

**Introducción**:

Apache Airflow es una plataforma open-source para la orquestación de flujos de trabajo basada en programación. Su arquitectura modular y escalable permite coordinar tareas distribuidas, administrar dependencias entre procesos y monitorear su ejecución en tiempo real. Para aprovechar todo su potencial, es necesario conocer a fondo sus componentes principales y cómo se interrelacionan, así como las opciones de configuración y despliegue disponibles.

**Desarrollo**:

El núcleo de Apache Airflow se basa en una arquitectura distribuida compuesta por múltiples servicios que trabajan de manera coordinada: un Webserver para la interfaz de usuario, un Scheduler para planificar y lanzar tareas, un Executor para ejecutar las tareas, una base de datos de metadatos que registra el estado de las ejecuciones y, en algunos casos, Workers que procesan las tareas. Esta arquitectura flexible permite escalar desde una instalación local simple hasta implementaciones en clústeres de Kubernetes o en la nube. La comprensión profunda de estos componentes, junto con el manejo adecuado de configuraciones y variables de entorno, permite adaptar Airflow a una amplia variedad de contextos y necesidades.

### 4.1.1 Webserver, Scheduler, Executor, Metadata Database

Apache Airflow está compuesto por varios servicios fundamentales que interactúan para programar, ejecutar y monitorear flujos de trabajo (DAGs). Cada componente cumple un rol específico y puede escalarse de manera independiente.

##### Webserver

El Webserver es el componente que proporciona la interfaz gráfica de usuario (GUI) de Airflow. A través de esta interfaz, los usuarios pueden visualizar, ejecutar manualmente y monitorear DAGs, revisar logs, configurar conexiones y más. Corre típicamente en un proceso separado y puede desplegarse detrás de un balanceador de carga para escalar horizontalmente.

* Ofrece paneles para: DAGs, tareas, logs, variables, pools, conexiones y más.
* Admite autenticación (LDAP, OAuth, Kerberos, etc.)
* Corre como un servicio Flask, basado en Gunicorn para producción.

##### Scheduler

El Scheduler es el componente responsable de analizar los DAGs y programar las tareas según su configuración (`schedule_interval`, dependencias, etc.). Se encarga de colocar las tareas listas en la cola de ejecución correspondiente según el Executor en uso.

* Revisa constantemente la base de metadatos para detectar DAGs nuevos o actualizados.
* Evalúa dependencias y lanza tareas según su planificación y estado.
* Requiere alta disponibilidad en entornos de producción (puede ejecutarse en múltiples instancias).

##### Executor

El Executor es el componente que decide cómo y dónde se ejecutan las tareas. Define el modelo de ejecución, ya sea en el mismo proceso (LocalExecutor), en Workers (CeleryExecutor), o en pods (KubernetesExecutor).

* Es una de las piezas clave para definir la escalabilidad del sistema.
* Determina si las tareas se ejecutan de forma secuencial, paralela o distribuida.

##### Metadata Database

Airflow utiliza una base de datos relacional (PostgreSQL o MySQL) para almacenar metadatos de la ejecución: estado de DAGs y tareas, logs, registros de ejecución, variables, conexiones, etc.

* Es el **estado centralizado** del sistema.
* Debe estar siempre disponible y respaldada.
* Permite trazabilidad y auditoría completa de los flujos de trabajo.

### 4.1.2 Workers y tipos de executors (Local, Celery, Kubernetes, CeleryKubernetes)

El tipo de **Executor** elegido determina si es necesario usar Workers y define cómo se distribuyen y ejecutan las tareas de los DAGs. Airflow soporta varios modelos de ejecución, cada uno adecuado para diferentes escenarios.

##### LocalExecutor

* Ejecuta las tareas en procesos paralelos dentro del mismo nodo donde se ejecuta el Scheduler.
* Útil para entornos de desarrollo o producción ligera.
* No requiere Workers externos.

|Ventajas|Limitaciones|
|:--|:--|
|Simple de configurar|No se distribuye entre nodos|
|Sin dependencias adicionales (como colas o clústeres)|Escalabilidad limitada a los recursos del host|

##### CeleryExecutor

* Usa **Celery** como backend de procesamiento distribuido y **RabbitMQ** o **Redis** como broker de mensajes.
* Permite escalar horizontalmente mediante Workers externos que procesan tareas desde una cola.

|Ventajas|Limitaciones|
|:--|:--|
|Altamente escalable|Mayor complejidad operativa|
|Separación clara entre Scheduler y ejecución de tareas|Necesita configuración de broker y backend de resultados|

##### KubernetesExecutor

* Cada tarea se ejecuta como un **pod independiente** en un clúster de Kubernetes.
* Ideal para entornos cloud-native y escalabilidad extrema.

|Ventajas|Limitaciones|
|:--|:--|
|Escalabilidad automática|Requiere experiencia en Kubernetes|
|Aislamiento total de tareas|Inicialización de pods puede añadir latencia|
|Integración nativa con servicios de nube||

##### CeleryKubernetesExecutor

* Híbrido entre Celery y Kubernetes.
* Algunas tareas se ejecutan en Workers Celery, y otras en pods Kubernetes.

|Ventajas|Limitaciones|
|:--|:--|
|Permite flexibilidad en entornos híbridos|Complejidad elevada de configuración y monitoreo|
|Equilibrio entre tareas persistentes y dinámicas||

### 4.1.3 Configuración y variables de entorno

Airflow permite configurar su comportamiento mediante archivos, variables de entorno y parámetros internos. Esto otorga flexibilidad para adaptarse a múltiples entornos: local, CI/CD, nube, clústeres distribuidos, etc.

##### Archivo `airflow.cfg`

Es el archivo principal de configuración de Airflow. Contiene más de 70 parámetros organizados por secciones: `core`, `scheduler`, `webserver`, `logging`, `executor`, entre otros.

* Define la conexión a la base de datos (`sql_alchemy_conn`).
* Determina el tipo de executor.
* Configura el path de los DAGs.
* Controla la frecuencia de chequeo del scheduler, la política de retries, etc.

Este archivo puede ser sobrescrito o templado en entornos Dockerizados o de nube.

##### Variables de entorno

Airflow permite sobrescribir cualquier configuración de `airflow.cfg` mediante variables de entorno, utilizando el prefijo `AIRFLOW__`. Por ejemplo:

* `AIRFLOW__CORE__SQL_ALCHEMY_CONN`: define la URL de la base de datos.
* `AIRFLOW__CORE__EXECUTOR`: define el tipo de executor.

Esto es especialmente útil en entornos de contenedores, pipelines de CI/CD o despliegues cloud.

##### Variables y conexiones en la interfaz

Además de variables de entorno del sistema, Airflow permite definir **variables internas** (`Variables`) y **conexiones** (`Connections`) desde la UI o CLI:

* `Variables`: parámetros reutilizables dentro de los DAGs.
* `Connections`: credenciales y URIs de servicios externos (bases de datos, APIs, buckets, etc.)

Estas son accesibles desde los DAGs mediante la API interna de Airflow (`Variable.get()`, `BaseHook.get_connection()`).

## Tarea

1. ¿Qué componente de Airflow proporciona la interfaz gráfica de usuario?
2. ¿Cuál es el rol principal del Scheduler en la arquitectura de Airflow?
3. ¿Qué tipo de executor se recomienda para ambientes con Kubernetes?
4. ¿Cuál es la función del archivo `airflow.cfg`?
5. ¿Qué base de datos se utiliza para almacenar metadatos en Airflow?
6. Menciona dos ventajas del CeleryExecutor frente al LocalExecutor.
7. ¿Qué significa `AIRFLOW__CORE__EXECUTOR`?
8. ¿Qué componente almacena el historial de ejecuciones y tareas?
9. ¿Cuál es la diferencia entre una Variable y una Connection en Airflow?
10. ¿Para qué sirve la configuración `sql_alchemy_conn`?

