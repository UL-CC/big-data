# 1. Introducción

## Tema 1.4 Instalación y configuración de Spark

**Objetivo**:

Instalar y configurar entornos de Apache Spark tanto en escenarios locales (para desarrollo y pruebas) como en configuraciones de clúster on-premise y plataformas de nube, asegurando la capacidad de ejecutar aplicaciones Spark de manera eficiente.

**Introducción**:

Para aprovechar el poder de Apache Spark, es fundamental comprender cómo instalarlo y configurarlo correctamente. Este tema cubrirá los pasos necesarios para establecer un entorno Spark, desde los requisitos básicos hasta la configuración de clústeres a gran escala en diferentes modos de despliegue. Abordaremos tanto las instalaciones on-premise, que te dan un control total, como los servicios gestionados en la nube, que simplifican la operación. Finalmente, nos centraremos en la configuración de un entorno de desarrollo en Windows utilizando Docker y WSL2, lo que te permitirá realizar las prácticas del curso de manera efectiva y sin complicaciones.

**Desarrollo**:

La instalación y configuración de Spark puede variar significativamente dependiendo del entorno de despliegue. Ya sea que busques construir un clúster dedicado en tus propios servidores, aprovechar la elasticidad de los servicios en la nube, o simplemente configurar un entorno local para tus prácticas de desarrollo, cada escenario tiene sus particularidades. En este tema, desglosaremos los requisitos previos, los pasos detallados para las instalaciones on-premise, las consideraciones clave al trabajar con Spark en la nube, y una guía práctica para configurar tu estación de trabajo Windows con Docker y WSL2 para una experiencia de desarrollo fluida y eficiente.

### 1.4.1 Requisitos previos para la instalación

Antes de sumergirte en la instalación de Spark, es crucial asegurar que tu sistema cumpla con ciertos requisitos de software. Spark está construido sobre Java y se integra estrechamente con otros componentes, por lo que tener las versiones correctas de las dependencias es fundamental para evitar problemas de compatibilidad y asegurar un funcionamiento óptimo.

##### Java Development Kit (JDK)

Apache Spark requiere una instalación de **Java Development Kit (JDK)** para funcionar, ya que el propio Spark está escrito en Scala y Java. Es esencial tener una versión de JDK compatible con la versión de Spark que planeas instalar. Generalmente, Spark es compatible con JDK 8 o superior, pero siempre es buena práctica revisar la documentación oficial para la versión específica de Spark que estés utilizando.

* Verificar la versión de Java instalada ejecutando `java -version` en la terminal. Si no está instalada o la versión es incompatible, descargar e instalar el JDK apropiado (por ejemplo, OpenJDK 11 o Oracle JDK 8).
* Configurar la variable de entorno `JAVA_HOME` para que apunte al directorio de instalación de tu JDK. Esto es crucial para que Spark encuentre la JVM.
* Asegurarse de que el directorio `bin` del JDK esté en la variable `PATH` del sistema para poder ejecutar comandos Java desde cualquier ubicación.

##### Python (para PySpark)

Si planeas usar **PySpark** para escribir aplicaciones Spark en Python, necesitarás una instalación de Python en tu sistema. Spark utiliza el intérprete de Python para ejecutar el código PySpark. Es recomendable usar una versión de Python compatible con la versión de Spark que estás instalando, generalmente Python 3.9 o superior.

##### Apache Hadoop (opcional, para HDFS y YARN)

Aunque Spark puede ejecutarse de forma independiente, a menudo se utiliza en conjunción con **Apache Hadoop**, especialmente para el sistema de archivos distribuido (HDFS) y el gestor de recursos (YARN). Si planeas usar Spark con HDFS o YARN, necesitarás una instalación de Hadoop. La versión de Spark que descargues debería estar precompilada con la versión de Hadoop que planeas usar para evitar problemas de compatibilidad.

* Si tu clúster ya tiene Hadoop instalado, asegurarte de que `HADOOP_HOME` y `HADOOP_CONF_DIR` estén configurados correctamente para que Spark pueda interactuar con él.
* Si no tienes Hadoop, puedes descargar una distribución precompilada de Spark que incluya los *binaries* de Hadoop, lo que te permitirá usar funcionalidades básicas de Hadoop sin una instalación completa.
* En entornos de nube, esta dependencia se maneja típicamente por el servicio gestionado (ej., EMR ya incluye Hadoop).

### 1.4.2 Instalación de un clúster Spark On-Premise

Configurar un clúster Spark on-premise te brinda el máximo control y flexibilidad, aunque requiere una inversión significativa en hardware, configuración y mantenimiento. Es una opción común para organizaciones con centros de datos existentes o necesidades específicas de seguridad y rendimiento.

##### Descarga de Spark

El primer paso para una instalación on-premise es obtener la distribución de Spark. Debes elegir la versión precompilada que mejor se adapte a tu entorno de Hadoop (si lo usas) y tu versión de Scala. La descarga se realiza desde el sitio web oficial de Apache Spark.

1.  Navegar a la sección de descargas de Apache Spark y seleccionar la versión más reciente compatible con tu JDK y la versión de Hadoop deseada (ej., "Spark 3.5.1 for Hadoop 3.3 and later").
2.  Descargar el archivo `.tgz` (tar.gz) a cada nodo del clúster o a un servidor central para su distribución.
3.  Descomprimir el archivo en un directorio accesible, por ejemplo, `/opt/spark` en sistemas Linux:

```bash
tar -xzf spark-<version>-bin-hadoop<version>.tgz -C /opt/
```

##### Configuración del modo Standalone

El modo Standalone es el gestor de clústeres autónomo de Spark. Es el más fácil de configurar y es útil para pruebas rápidas o clústeres dedicados a Spark sin otras dependencias de gestores de recursos. Implica configurar un **Master** y varios **Workers**.

1.  En el nodo que será el Master, editar `spark/conf/spark-env.sh` (si no existe, copiar `spark-env.sh.template`) y añadir `export SPARK_MASTER_HOST=<IP_DEL_MASTER>`. También puedes configurar `SPARK_MASTER_PORT` y `SPARK_MASTER_WEBUI_PORT`.
2.  En cada nodo Worker, editar su `spark/conf/spark-env.sh` para definir `export SPARK_MASTER_URL=spark://<IP_DEL_MASTER>:<PUERTO_DEL_MASTER>`.
3.  Iniciar el Master y los Workers utilizando los scripts `sbin/start-master.sh` y `sbin/start-workers.sh` (o `sbin/start-all.sh` si usas `conf/slaves`). Verifica la UI del Master en `http://<IP_DEL_MASTER>:8080`.

**Comentario para la Nube**: En entornos de nube, el modo Standalone raramente se usa para producción. Los servicios gestionados (AWS EMR, Azure Databricks, GCP Dataproc) lo abstraen o emplean gestores de clústeres más robustos como YARN o Kubernetes. Sin embargo, puedes replicar esta configuración manualmente en VMs en la nube si necesitas un control granular y no quieres usar un servicio gestionado.

##### Configuración con YARN (Yet Another Resource Negotiator)

**YARN** es el gestor de recursos de Hadoop y es la forma más común de desplegar Spark en clústeres de Hadoop existentes. Permite a Spark compartir recursos dinámicamente con otras aplicaciones Hadoop. Para configurar Spark con YARN, es necesario que Spark tenga acceso a los archivos de configuración de Hadoop.

1.  Asegurarse de que la variable de entorno `HADOOP_CONF_DIR` esté configurada en `spark/conf/spark-env.sh` en todos los nodos y apunte al directorio que contiene `core-site.xml` y `yarn-site.xml` de tu instalación de Hadoop.
2.  Verificar que el clúster Hadoop con YARN esté en funcionamiento (ResourceManager, NodeManagers, etc.).
3.  Enviar una aplicación Spark a YARN utilizando `spark-submit --master yarn --deploy-mode cluster <your-app.jar>`. Spark utilizará YARN para asignar recursos y ejecutar la aplicación.

**Comentario para la Nube**: **YARN es el *Cluster Manager* predeterminado en muchos servicios gestionados de Spark en la nube, como AWS EMR y GCP Dataproc.** En estos casos, la integración con YARN es automática y no requiere configuración manual de `HADOOP_CONF_DIR`. Solo necesitas especificar `--master yarn` al enviar tus trabajos.

##### Optimización de la configuración (memoria, cores, paralelismo)

La optimización del rendimiento de Spark depende en gran medida de una configuración adecuada de sus recursos. Esto implica ajustar la memoria asignada al driver y a los ejecutores, el número de *cores* por ejecutor, y el paralelismo de las tareas. Una configuración incorrecta puede llevar a errores de memoria, subutilización de recursos o ejecuciones lentas.

1.  Ajustar la memoria del driver (`spark.driver.memory`) si el programa principal necesita cargar muchos datos en memoria o manejar una gran cantidad de metadatos. Por ejemplo: `--driver-memory 4g`.
2.  Configurar la memoria y los *cores* por ejecutor (`spark.executor.memory`, `spark.executor.cores`) para balancear el número de tareas concurrentes por nodo y la cantidad de datos que puede procesar cada tarea. Por ejemplo: `--executor-memory 8g --executor-cores 4`.
3.  Controlar el paralelismo de las operaciones de *shuffle* (`spark.sql.shuffle.partitions`) para evitar la creación de demasiadas particiones pequeñas o muy pocas particiones grandes, lo que puede impactar el rendimiento. Un valor de 200 o más es común para clústeres grandes.

**Comentario para la Nube**: La optimización de la configuración es igualmente crítica en la nube. Los servicios gestionados ofrecen la flexibilidad de ajustar estos parámetros a través de la consola o la CLI. Además, algunos servicios como Databricks y Dataproc ofrecen características avanzadas como el autoescalado y la optimización automática del motor que pueden simplificar este proceso, aunque entender los parámetros básicos sigue siendo fundamental.

### 1.4.3 Spark en entornos de nube (AWS, Azure, GCP)

La computación en la nube ha revolucionado la forma en que se despliegan y gestionan los clústeres de Spark. Los proveedores de la nube ofrecen servicios gestionados que abstraen la complejidad de la infraestructura subyacente, permitiéndote enfocarte en el desarrollo de tus aplicaciones de datos.

##### Servicios gestionados de Spark en la nube

Estos servicios ofrecen una experiencia "llave en mano" para Spark, donde el proveedor se encarga del aprovisionamiento de máquinas, la instalación del software Spark y Hadoop, la configuración de red y el monitoreo básico. Esto reduce significativamente la carga operativa y el tiempo de configuración.

1.  **AWS EMR (Elastic MapReduce)**: Un servicio de clústeres gestionados que facilita el despliegue y la ejecución de frameworks de Big Data como Spark, Hadoop, Hive, Presto, etc. Se integra nativamente con S3 para almacenamiento y Kinesis para streaming.
2.  **Azure HDInsight**: El servicio de análisis de Big Data de Microsoft Azure, que ofrece clústeres gestionados para Spark, Hadoop, Kafka y otros. Se integra con Azure Data Lake Storage (ADLS), Cosmos DB y Azure Synapse Analytics.
3.  **GCP Dataproc**: El servicio de Google Cloud para Spark y Hadoop. Se caracteriza por su rápido aprovisionamiento de clústeres y escalado automático, con fuerte integración con Google Cloud Storage (GCS) y BigQuery.
4.  **Databricks**: Una plataforma unificada para datos y IA, construida sobre Spark y disponible en AWS, Azure y GCP. Ofrece un entorno de desarrollo colaborativo (notebooks), optimizaciones de rendimiento a nivel de motor y gestión simplificada de clústeres.

##### Ventajas de los servicios gestionados

Las soluciones de Spark en la nube ofrecen beneficios sustanciales en comparación con las instalaciones on-premise, principalmente en términos de escalabilidad, flexibilidad de costos y reducción de la sobrecarga de gestión.

1.  Un clúster EMR o Dataproc puede escalar automáticamente el número de nodos hacia arriba o hacia abajo en función de la demanda de carga de trabajo, lo que optimiza el uso de recursos y el costo.
2.  Solo pagas por los recursos computacionales y de almacenamiento que consumes, sin la necesidad de una inversión inicial en hardware. Puedes apagar los clústeres cuando no los uses.
3.  El proveedor de la nube se encarga de las actualizaciones de software, parches de seguridad, mantenimiento de infraestructura y recuperación de fallos, liberando a tu equipo para centrarse en el desarrollo de aplicaciones.

##### Configuración de acceso a datos en la nube

Un aspecto clave al usar Spark en la nube es la configuración del acceso a los sistemas de almacenamiento de objetos nativos de la nube (como S3 en AWS, ADLS en Azure o GCS en GCP). Estos sistemas son altamente escalables, duraderos y rentables, y Spark se integra muy bien con ellos.

1.  Para acceder a datos en **AWS S3** desde EMR, solo necesitas especificar la ruta `s3a://<bucket-name>/<path-to-data>` en tu código Spark, y EMR gestionará automáticamente las credenciales de autenticación si el clúster tiene los roles IAM correctos.
2.  En **Azure HDInsight**, puedes leer y escribir datos en **Azure Data Lake Storage (ADLS) Gen2** especificando rutas como `abfss://<filesystem>@<accountname>.dfs.core.windows.net/<path>`. La autenticación se maneja a través de las identidades de Azure.
3.  Con **GCP Dataproc**, el acceso a **Google Cloud Storage (GCS)** es directo usando rutas `gs://<bucket-name>/<path-to-data>`, ya que los servicios de Google Cloud están configurados para interoperar con la seguridad del proyecto de GCP.

### 1.4.4 Instalación de Spark en Windows (para prácticas)

Para el desarrollo y las prácticas locales en Windows, la instalación nativa de Spark puede ser compleja debido a dependencias de Hadoop y Path variables. La solución más robusta y recomendada es [utilizar **Docker** y el **Windows Subsystem for Linux 2 (WSL2)**](tema_docker.md), que te permite ejecutar un entorno Linux con Spark de manera ligera y eficiente en tu máquina Windows.

##### Uso de imágenes Docker para Spark (ej. bitnami/spark)

Una vez que Docker Desktop y WSL2 están listos, puedes utilizar imágenes Docker preconstruidas que ya contienen Spark. Esto elimina la necesidad de instalar Java, Scala o Spark manualmente en tu entorno WSL, simplificando enormemente el setup para las prácticas.

Desde tu terminal de WSL2 (o PowerShell/CMD), descargar la imagen de Spark deseada, con el comando:

```bash
docker pull bitnami/spark:latest
```

2.  Ejecutar un contenedor Spark en modo Standalone. 

Primero el Master: 

```bash
docker run -d --name spark-master -p 8080:8080 -p 7077:7077 bitnami/spark:latest start_master.sh
```

Luego, un Worker:

```bash
docker run -d --name spark-worker1 --link spark-master:spark-master bitnami/spark:latest start_worker.sh spark://spark-master:7077
```

Acceder al Spark Shell (PySpark o Scala) dentro del contenedor Master: 

```bash
docker exec -it spark-master pyspark
```

Ahora puedes escribir y ejecutar código Spark directamente en tu terminal.

##### Configuración de entorno de desarrollo (Jupyter, IDEs)

Para una experiencia de desarrollo más completa, puedes configurar un entorno como Jupyter Notebook o integrar Spark con tu IDE favorito, conectándote al clúster Spark que se ejecuta en Docker/WSL2.

Dentro de tu distribución WSL2, instala Jupyter: 

```bash
pip install jupyter
```

Luego, para interactuar con PySpark, puedes añadir un kernel de PySpark al Jupyter:

```bash
# En tu .bashrc o .zshrc en WSL2
export SPARK_HOME=/opt/bitnami/spark # Si usas la imagen bitnami/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_SUBMIT_ARGS="--master spark://localhost:7077 pyspark-shell"
```

Luego, desde WSL2, lanza Jupyter: 

```bash
jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
```

Accede desde tu navegador Windows a `http://localhost:8888`

## Tarea

1.  Imagina que tu equipo tiene un centro de datos on-premise y está decidiendo si migrar su clúster Spark a la nube o mantenerlo local. Detalla al menos **cinco pros y cinco contras** de cada enfoque, considerando aspectos como costos, escalabilidad, seguridad y complejidad operativa.
2.  Explica con tus propias palabras qué es un **Cluster Manager** en Spark y por qué es una pieza tan fundamental en la arquitectura distribuida de Spark. Proporciona un ejemplo de cómo YARN y Spark Standalone difieren en su gestión de recursos.
3.  Investiga el concepto de **autoescalado (autoscaling)** en los servicios gestionados de Spark en la nube (ej., Dataproc o EMR). Describe cómo funciona y qué beneficios aporta en comparación con la gestión manual de recursos.
4.  ¿Cuáles son los pasos clave para conectar un Jupyter Notebook (ejecutándose en tu entorno WSL2) a un clúster Spark que está en un contenedor Docker? Proporciona un pseudocódigo o un ejemplo de configuración de variables de entorno necesario.

