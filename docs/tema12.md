# 1. Introducción

## Tema 1.2 Introducción al ecosistema Spark

**Objetivo**:

Comprender la arquitectura, componentes principales y modos de operación de Apache Spark, así como sus ventajas y casos de uso en el procesamiento de Big Data, para sentar las bases de su aplicación práctica.

**Introducción**:

En la era del Big Data, el procesamiento eficiente de grandes volúmenes de información es crucial. Apache Spark ha emergido como una de las herramientas más potentes y versátiles para esta tarea. Este tema proporcionará una visión integral del ecosistema Spark, desde su concepción hasta sus principales componentes y cómo interactúa con otras tecnologías del Big Data, preparando el terreno para un uso efectivo en el análisis y la ingeniería de datos.

**Desarrollo**:

Apache Spark es un motor unificado de análisis para el procesamiento de datos a gran escala. A diferencia de sus predecesores, como Hadoop MapReduce, Spark se distingue por su capacidad para procesar datos en memoria, lo que resulta en una velocidad significativamente mayor. Su diseño modular y extensible permite manejar una amplia variedad de cargas de trabajo, desde el procesamiento por lotes hasta el análisis en tiempo real, aprendizaje automático y procesamiento de grafos, todo ello dentro de un único ecosistema cohesivo.

### 1.2.1 ¿Qué es Apache Spark?

Apache Spark es un motor de procesamiento de datos distribuido de código abierto, diseñado para el análisis rápido de grandes volúmenes de datos. Se desarrolló en la Universidad de California, Berkeley, en el AMPLab, y fue donado a la Apache Software Foundation. Su principal fortaleza radica en su capacidad para realizar operaciones en memoria, lo que lo hace considerablemente más rápido que otros sistemas de procesamiento distribuido que dependen en gran medida del disco, como Hadoop MapReduce. Spark está optimizado para flujos de trabajo iterativos y consultas interactivas, lo que lo convierte en una opción ideal para Machine Learning y análisis de datos complejos.

##### Procesamiento en memoria

Se refiere a la capacidad de Apache Spark para retener los datos en la memoria RAM de los nodos del clúster mientras realiza operaciones, en lugar de escribirlos y leerlos repetidamente del disco. Esta característica es la principal razón de la alta velocidad de Spark, ya que el acceso a la memoria es órdenes de magnitud más rápido que el acceso al disco. Permite operaciones iterativas rápidas, algo esencial para algoritmos de Machine Learning y operaciones ETL complejas que requieren múltiples pasadas sobre los mismos datos.

1.  Un algoritmo de *Machine Learning* que requiere varias iteraciones para optimizar un modelo. Spark mantiene los datos en memoria a través de las iteraciones, evitando la sobrecarga de I/O de disco.
2.  Un proceso de *ETL* (Extracción, Transformación, Carga) que realiza múltiples transformaciones sobre un conjunto de datos. En lugar de guardar resultados intermedios en disco, Spark los gestiona en memoria.
3.  Consultas interactivas en un *data lake*. Los analistas pueden ejecutar consultas complejas con tiempos de respuesta muy bajos, ya que los datos relevantes pueden ser cacheados en memoria.

##### Resilient Distributed Datasets (RDDs)

Los RDDs son la colección fundamental de elementos tolerantes a fallos en Spark que se ejecutan en paralelo. Son la abstracción de datos principal en Spark Core. Un RDD es una colección inmutable y particionada de registros que se puede operar en paralelo. Los RDDs pueden ser creados a partir de fuentes externas (como HDFS o S3) o de colecciones existentes en Scala, Python o Java. Son "resilientes" porque pueden reconstruirse automáticamente en caso de fallo de un nodo, y "distribuidos" porque se extienden a través de múltiples nodos en un clúster.

1.  Cargar un archivo CSV grande de 1 TB desde HDFS en un RDD para su procesamiento.
2.  Realizar una operación `map` sobre un RDD para transformar cada elemento (ej: convertir una cadena a un número entero).
3.  Aplicar una operación `reduceByKey` a un RDD de pares clave-valor para sumar los valores por cada clave.

### 1.2.2 Componentes principales de Spark

Spark no es una herramienta monolítica; es un ecosistema compuesto por varios módulos integrados que extienden sus capacidades. Estos componentes se construyen sobre Spark Core y proporcionan APIs de alto nivel para diferentes tipos de procesamiento de datos, lo que permite a los desarrolladores elegir la herramienta adecuada para su tarea sin tener que manejar las complejidades del procesamiento distribuido desde cero.

##### Spark Core

Es el motor subyacente de todas las funcionalidades de Spark. Proporciona la funcionalidad básica de E/S, la planificación de tareas y la gestión de memoria. La abstracción fundamental de Spark Core es el RDD, que permite operaciones de procesamiento distribuido. Es la base sobre la cual se construyen todos los demás componentes de Spark.

1.  Leer un conjunto de datos sin estructura específica (por ejemplo, logs de servidores) y aplicar transformaciones básicas con RDDs.
2.  Realizar operaciones de bajo nivel y personalizadas que no están fácilmente disponibles en las APIs de alto nivel (como Spark SQL).
3.  Implementar un algoritmo de procesamiento de datos altamente específico donde se necesita control granular sobre las operaciones de particionamiento y persistencia.

##### Spark SQL

Spark SQL es un módulo para trabajar con datos estructurados y semiestructurados. Proporciona una interfaz de programación unificada para ejecutar consultas SQL y operaciones de manipulación de datos sobre estructuras como DataFrames y Datasets. Permite integrar código Spark con consultas SQL, facilitando la interacción con bases de datos relacionales, Hive, JSON, Parquet, etc. Su optimizador Catalyst es clave para su alto rendimiento.

1.  Cargar un archivo Parquet en un **DataFrame** y ejecutar una consulta SQL estándar como `SELECT * FROM tabla WHERE columna > 100`.
2.  Unir dos **DataFrames** basados en una clave común para combinar información de diferentes fuentes de datos.
3.  Leer un conjunto de datos JSON y transformarlo en un **DataFrame** para luego exportarlo a una base de datos relacional.

##### Spark Streaming

Spark Streaming es una extensión de la API principal de Spark que permite el procesamiento de flujos de datos en tiempo real. Recibe flujos de datos de diversas fuentes (Kafka, Flume, Kinesis, TCP Sockets, etc.) y los divide en pequeños lotes que luego son procesados por el motor Spark Core. Esto permite aplicar las mismas transformaciones de datos que se usan para el procesamiento por lotes a los datos en tiempo real.

1.  Analizar el *clickstream* de un sitio web en tiempo real para detectar patrones de navegación o anomalías.
2.  Monitorear datos de sensores de IoT para detectar fallos o eventos críticos instantáneamente.
3.  Procesar mensajes de Twitter en vivo para realizar análisis de sentimiento sobre temas específicos.

##### MLlib

MLlib es la biblioteca de aprendizaje automático de Spark. Proporciona una colección de algoritmos de Machine Learning de alto rendimiento y escalables, como clasificación, regresión, clustering, filtrado colaborativo, entre otros. Está diseñada para integrarse perfectamente con los DataFrames de Spark SQL, lo que permite a los usuarios construir pipelines de ML complejos.

1.  Entrenar un modelo de **clasificación** para predecir si un cliente abandonará un servicio (churn prediction) usando datos de transacciones.
2.  Aplicar un algoritmo de **clustering** para segmentar clientes basado en su comportamiento de compra.
3.  Construir un sistema de **recomendación** de productos utilizando datos de interacciones de usuarios con artículos.

##### GraphX

GraphX es la API de Spark para el procesamiento de grafos y el cálculo de grafos en paralelo. Combina las propiedades de los RDDs de Spark con las operaciones de grafos para proporcionar un marco flexible y eficiente para trabajar con estructuras de datos de grafos. Permite construir y manipular grafos, y ejecutar algoritmos de grafos como PageRank o Connected Components.

1.  Calcular el **PageRank** de nodos en una red social para identificar los usuarios más influyentes.
2.  Identificar las **conexiones más cortas** entre dos puntos en una red de transporte.
3.  Detectar **comunidades o grupos** de usuarios en una red de colaboración.

### 1.2.3 Arquitectura de Spark

La arquitectura de Spark es clave para su capacidad de procesamiento distribuido y tolerancia a fallos. Se basa en un modelo maestro-esclavo, donde un **Driver** coordina las operaciones entre los **Executors** distribuidos en el clúster, con la ayuda de un **Cluster Manager**. Entender estos roles es fundamental para desplegar y gestionar aplicaciones Spark de manera efectiva.

##### Spark Driver

El **Spark Driver** es el programa principal que coordina y gestiona la ejecución de una aplicación Spark. Contiene el `main` de la aplicación Spark y crea el `SparkContext` (o `SparkSession` en versiones más recientes). El Driver es responsable de convertir el código de la aplicación Spark en una serie de tareas, programarlas en los **Executors** y monitorear su ejecución. Es el punto de entrada para cualquier aplicación Spark.

1.  Un programa Python que inicializa una `SparkSession`, lee un archivo CSV y ejecuta algunas transformaciones de datos. El Driver se encarga de dividir el trabajo y enviarlo a los Executors.
2.  Cuando se utiliza `spark-submit` para lanzar una aplicación, el comando invoca el Driver en el nodo especificado (o en el *cluster manager*).
3.  En un *Jupyter Notebook* con un kernel Spark, el Driver se ejecuta en el proceso del *notebook* o en un nodo configurado, orquestando todas las operaciones.

##### Spark Executor

Un **Spark Executor** es un proceso que se ejecuta en los nodos *worker* del clúster de Spark. Son responsables de ejecutar las tareas individuales asignadas por el Driver y de almacenar los datos que se cachean o se persisten. Cada Executor tiene un cierto número de *cores* y una cantidad de memoria RAM asignada para ejecutar tareas en paralelo y almacenar datos.

1.  Un Executor recibe una tarea del Driver para filtrar un subconjunto de filas de un DataFrame.
2.  Múltiples Executors procesan diferentes particiones del mismo RDD en paralelo.
3.  Un Executor almacena en caché una porción de un DataFrame en su memoria local para acelerar futuras operaciones sobre esos datos.

##### Cluster Manager

El **Cluster Manager** es el componente responsable de asignar recursos del clúster (CPU, memoria) a la aplicación Spark. Actúa como intermediario entre el Driver y los Executors, gestionando la asignación de nodos y la disponibilidad de recursos. Spark puede trabajar con varios tipos de *Cluster Managers*.

1.  **YARN (Yet Another Resource Negotiator)**: Es el *Cluster Manager* más común en entornos Hadoop. Spark lo utiliza para solicitar recursos en un clúster Hadoop existente.
2.  **Apache Mesos**: Un gestor de recursos de propósito general que puede ejecutar Spark junto con otras aplicaciones distribuidas.
3.  **Spark Standalone**: El propio *Cluster Manager* de Spark, ideal para entornos de desarrollo y pruebas o clústeres dedicados a Spark sin otras dependencias.

### 1.2.4 Interacción con Spark

La interacción con Apache Spark puede realizarse de diversas maneras, dependiendo del propósito, ya sea para desarrollo interactivo, ejecución de trabajos programados o monitoreo. Comprender cómo interactuar con Spark permite a los desarrolladores y operadores gestionar sus aplicaciones de forma eficiente.

##### Spark Shell

El **Spark Shell** es una herramienta interactiva basada en la consola que permite a los usuarios experimentar con Spark directamente. Proporciona un entorno REPL (Read-Eval-Print Loop) donde se pueden escribir y ejecutar comandos Spark en Scala, Python o R. Es ideal para prototipado, pruebas rápidas y exploración de datos.

1.  Iniciar `pyspark` en la terminal para abrir el Spark Shell con soporte para Python.
2.  Escribir `sc.parallelize([1, 2, 3]).map(lambda x: x*2).collect()` en el Spark Shell para ver el resultado de una operación simple.
3.  Probar la lectura de un archivo de datos pequeño y las primeras transformaciones antes de integrarlas en un script más grande.

##### Spark Submit

`spark-submit` es el comando de línea de comandos principal utilizado para enviar aplicaciones Spark (escritas en Scala, Java, Python o R) a un clúster Spark. Permite especificar la ubicación del código de la aplicación, los recursos a asignar (memoria del driver, memoria de los executors, número de cores, etc.) y el *Cluster Manager* a utilizar. Es la forma estándar de ejecutar trabajos Spark en producción.

1.  `spark-submit --class com.example.MyApp --master yarn --deploy-mode cluster myapp.jar` para enviar una aplicación Java/Scala a un clúster YARN.
2.  `spark-submit --master local[*] my_python_script.py` para ejecutar un script Python en modo local (útil para desarrollo y pruebas en una sola máquina).
3.  `spark-submit --driver-memory 4g --executor-memory 8g --num-executors 10 my_etl_job.py` para asignar recursos específicos a una aplicación ETL.

##### Spark UI

La **Spark UI (User Interface)** es una interfaz web que proporciona monitoreo en tiempo real de las aplicaciones Spark en ejecución. Permite a los usuarios ver el estado de los trabajos, las etapas, las tareas, el consumo de memoria de los Executors, los logs y otra información detallada sobre la ejecución de la aplicación. Es una herramienta invaluable para depurar, optimizar y comprender el rendimiento de las aplicaciones Spark.

1.  Acceder a `http://localhost:4040` (o la dirección IP y puerto correspondientes) mientras una aplicación Spark se está ejecutando para ver los DAGs de las etapas.
2.  Inspeccionar la pestaña "Stages" para identificar qué partes de un trabajo están tardando más en ejecutarse o si hay *skew* en los datos (desequilibrio de carga).
3.  Revisar los logs de los Executors en la pestaña "Executors" para diagnosticar errores o problemas de memoria.

##### 1.2.5 Conceptos fundamentales de procesamiento distribuido en Spark

El procesamiento distribuido en Spark se basa en varios conceptos clave que optimizan el rendimiento y la tolerancia a fallos. Entender cómo Spark maneja la partición de datos, la persistencia y la evaluación perezosa es crucial para escribir aplicaciones eficientes y robustas.

##### Particionamiento de datos

El **particionamiento de datos** en Spark se refiere a cómo los datos se dividen y se distribuyen entre los nodos de un clúster. Cada partición de un RDD o DataFrame es un conjunto lógico de datos que puede ser procesado por una tarea individual en un Executor. El número y la estrategia de particionamiento afectan directamente el paralelismo, la eficiencia de las operaciones de *shuffle* (reorganización de datos entre nodos) y el rendimiento general de la aplicación.

1.  Al leer un archivo de texto grande, Spark lo divide automáticamente en particiones basadas en el tamaño de bloque del sistema de archivos subyacente (ej. HDFS).
2.  Después de una operación como `groupByKey` o `join`, Spark puede necesitar re-particionar los datos (esto se conoce como *shuffle*) para asegurar que los datos relacionados estén en el mismo nodo.
3.  Un desarrollador puede especificar el número de particiones manualmente (`repartition` o `coalesce`) para optimizar el rendimiento, por ejemplo, para evitar demasiadas particiones pequeñas o muy pocas particiones grandes.

##### Persistencia de datos (Caching)

La **persistencia de datos** o *caching* en Spark es la capacidad de almacenar en memoria o en disco los RDDs o DataFrames intermedios para acelerar futuras operaciones sobre ellos. Cuando se marca un RDD/DataFrame para persistencia, Spark intenta mantener sus particiones en la memoria RAM de los Executors. Esto es especialmente útil para flujos de trabajo iterativos o cuando se accede repetidamente al mismo conjunto de datos.

1.  Marcar un DataFrame como `df.cache()` después de una costosa operación de carga y limpieza, antes de ejecutar múltiples consultas sobre él.
2.  En un algoritmo de Machine Learning iterativo, el conjunto de datos de entrenamiento se persiste (`persist(StorageLevel.MEMORY_AND_DISK)`) para evitar recalcularlo en cada iteración.
3.  Un conjunto de datos de referencia (ej. una tabla de códigos postales) que se une frecuentemente con otros DataFrames se puede persistir para un acceso rápido.

##### Lazy Evaluation (Evaluación Perezosa)

La **Evaluación Perezosa** es un concepto fundamental en Spark que significa que las transformaciones (operaciones que producen un nuevo RDD/DataFrame a partir de uno existente, como `map`, `filter`, `join`) no se ejecutan inmediatamente cuando se invocan. En su lugar, Spark construye un *plan lógico* de las operaciones. La ejecución real de estas transformaciones solo ocurre cuando se invoca una **acción** (operación que devuelve un valor al Driver o escribe datos en un sistema externo, como `count`, `collect`, `saveAsTextFile`).

1.  Cuando se escribe `df.filter("edad > 30").select("nombre")`, Spark no procesa los datos en ese instante; solo registra estas transformaciones en su plan.
2.  La ejecución real del código del ejemplo anterior solo se dispara cuando se añade una acción como `.show()` o `.count()`.
3.  La evaluación perezosa permite a Spark optimizar el plan de ejecución completo (DAG) antes de ejecutar cualquier cálculo, eliminando operaciones innecesarias o reordenándolas para una mayor eficiencia.

### 1.2.6 Comparación de Spark con otras herramientas Big Data

Apache Spark, aunque muy potente, no es una solución aislada. Se integra y a menudo complementa a otras herramientas en el ecosistema Big Data. Comprender su posición y cómo se compara con otras soluciones es crucial para tomar decisiones arquitectónicas informadas.

##### Spark vs. Hadoop MapReduce

**Hadoop MapReduce** es el motor de procesamiento original del ecosistema Hadoop. Opera en un modelo de dos fases (map y reduce), escribiendo resultados intermedios en disco. **Spark**, por otro lado, puede realizar operaciones multipase en memoria y ofrece una API más flexible. Spark es generalmente más rápido para cargas de trabajo iterativas y para el procesamiento de datos en tiempo real, mientras que MapReduce puede ser adecuado para procesamientos por lotes masivos que no requieren mucha interacción o iteraciones.

1.  Para un proceso de *ETL* que involucra múltiples pasos de transformación y limpieza de datos (ej. `filter` -> `join` -> `groupBy`), Spark es significativamente más eficiente que MapReduce debido a su procesamiento en memoria.
2.  Un algoritmo de *PageRank* o *K-Means* que requiere muchas iteraciones sobre el mismo conjunto de datos se ejecuta mucho más rápido en Spark.
3.  Para un análisis de datos que solo implica una operación de conteo masiva y una sola pasada (ej. `word count` en archivos muy grandes), MapReduce podría ser suficiente, aunque Spark también lo manejaría eficientemente.

## Tarea

1.  Busca un ejemplo de código en Python o Scala donde se utilice `persist()` con diferentes `StorageLevel` (por ejemplo, `MEMORY_ONLY`, `DISK_ONLY`, `MEMORY_AND_DISK`) y explica cuándo sería apropiado usar cada uno.
2.  Compara la resiliencia de los **RDDs** en Spark con la tolerancia a fallos en **Hadoop HDFS**. ¿Cuáles son las similitudes y diferencias clave en cómo manejan la pérdida de datos o nodos?
3.  Identifica dos escenarios de negocio donde **Spark Streaming** sería la solución ideal y justifica por qué.

