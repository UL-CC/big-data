# 2. PySpark y SparkSQL

## Tema 2.4 Optimización y Rendimiento

**Objetivo**:

Al finalizar este tema, el estudiante será capaz de identificar y aplicar técnicas avanzadas de optimización y gestión del rendimiento en aplicaciones Spark, comprendiendo el funcionamiento interno del motor y las estrategias para el manejo eficiente de datos distribuidos y operaciones complejas, con el fin de mejorar significativamente la eficiencia y escalabilidad de los pipelines de procesamiento de Big Data.

**Introducción**:

En el vasto universo del procesamiento de Big Data con Apache Spark, la capacidad de escribir código funcional es solo la primera parte de la ecuación. La verdadera maestría reside en optimizar ese código para que se ejecute de manera eficiente, consumiendo menos recursos y completando las tareas en el menor tiempo posible. Este tema se adentra en el corazón del rendimiento de Spark, explorando las herramientas y los principios subyacentes que permiten transformar una aplicación de datos masivos en una solución robusta y escalable.

**Desarrollo**:

La optimización en Spark es un proceso multifacético que abarca desde la gestión inteligente de la memoria y el disco hasta la comprensión profunda de cómo Spark planifica y ejecuta las operaciones. Se explorarán conceptos fundamentales como la persistencia de DataFrames, la arquitectura de optimización de Spark (Catalyst y Tungsten), y estrategias avanzadas para el manejo de joins y la reducción de operaciones costosas como los shuffles. Finalmente, se abordarán consideraciones clave para escalar y mantener el desempeño en entornos de producción, proporcionando al estudiante las herramientas para diagnosticar cuellos de botella y aplicar soluciones efectivas en sus proyectos de Big Data.

### 2.4.1 Persistencia y caché (persist() y cache())

La persistencia en Spark es una característica fundamental para optimizar el rendimiento de las operaciones sobre DataFrames o RDDs que se reutilizan múltiples veces. Cuando una operación se ejecuta sobre un DataFrame, Spark recalcula el DataFrame desde el origen cada vez que se invoca una acción. Esto puede ser ineficiente si el mismo DataFrame es utilizado en varias operaciones subsiguientes. Al aplicar `persist()` o `cache()`, Spark almacena el DataFrame o RDD resultante de una transformación en memoria, en disco o una combinación de ambos, evitando recálculos innecesarios y acelerando las operaciones posteriores.

##### Niveles de almacenamiento en persist()

Spark ofrece diferentes niveles de almacenamiento para `persist()`, lo que permite controlar dónde y cómo se guardan los datos para optimizar el equilibrio entre memoria, disco y replicación. La elección del nivel adecuado depende de la cantidad de memoria disponible, la necesidad de tolerancia a fallos y la frecuencia de acceso a los datos.

1.  **`MEMORY_ONLY`**:

    Es el nivel por defecto para `cache()`. Los RDDs/DataFrames se almacenan como objetos deserializados de Python (o Java/Scala si se usa Scala/Java) en la JVM. Si no cabe en memoria, algunas particiones se recalculan cuando son necesarias.

    * **Conjuntos de datos pequeños y medianos**: Cuando se tiene un DataFrame que cabe completamente en la memoria de los ejecutores y se va a utilizar repetidamente en múltiples transformaciones. Por ejemplo, un catálogo de productos que se une frecuentemente con transacciones.
    * **Operaciones iterativas**: En algoritmos de machine learning (como K-Means o PageRank) donde un conjunto de datos se itera sobre muchas veces, manteniendo los datos en memoria reduce drásticamente el tiempo de ejecución de cada iteración.

2.  **`MEMORY_AND_DISK`**:

    Almacena las particiones en memoria. Si no hay suficiente memoria, las particiones que no caben se almacenan en disco. Las particiones en disco se leen y deserializan bajo demanda.

    * **DataFrames grandes con uso frecuente**: Cuando se trabaja con un DataFrame que es demasiado grande para caber completamente en memoria, pero aún se necesita un acceso rápido. Por ejemplo, un historial de eventos de usuario que se procesa a diario pero no cabe 100% en RAM.
    * **Reducir recálculos en fallos**: Si se necesita persistir un DataFrame para su uso posterior y se busca una combinación de rendimiento y resiliencia básica sin replicación.

3.  **`MEMORY_ONLY_SER` (Serialized)**:

    Similar a `MEMORY_ONLY`, pero los objetos se almacenan en su forma serializada (bytes). Esto reduce el uso de memoria (hasta un 10x) a expensas de un mayor costo de CPU para deserializar los datos.

    * **Optimización de memoria en clústeres limitados**: Cuando la memoria es un recurso escaso y se prefiere sacrificar un poco de tiempo de CPU por un uso de memoria mucho más eficiente. Útil para DataFrames muy grandes que aún se quieren mantener mayormente en memoria.
    * **Uso de Kryo Serialization**: Combinado con la serialización Kryo personalizada, puede ser extremadamente eficiente en memoria y aún ofrecer buen rendimiento de acceso.

4.  **`MEMORY_AND_DISK_SER`**:

    Igual que `MEMORY_ONLY_SER`, pero las particiones que no caben en memoria se almacenan en disco.

    * **Grandes conjuntos de datos con memoria limitada**: La opción más robusta para conjuntos de datos que exceden la memoria pero necesitan persistencia sin replicación. Ofrece un buen equilibrio entre uso de memoria, rendimiento y fiabilidad.

5.  **`DISK_ONLY`**:

    Almacena las particiones solo en disco. Es el más lento de los niveles de persistencia ya que implica operaciones de E/S de disco.

    * **Debugging y auditoría**: Cuando se quiere guardar el estado intermedio de un DataFrame para inspección o para reiniciar un proceso sin tener que recalcular todo desde el principio, pero no se necesita el rendimiento de la memoria.
    * **Tolerancia a fallos en el estado intermedio**: En casos donde la memoria es extremadamente limitada y se necesita garantizar que los resultados intermedios no se pierdan en caso de fallo de un ejecutor, aunque el acceso sea más lento.

##### Diferencia entre persist() y cache()

La función `cache()` es simplemente un alias para `persist()` con el nivel de almacenamiento por defecto `MEMORY_ONLY`. Esto significa que `df.cache()` es equivalente a `df.persist(StorageLevel.MEMORY_ONLY)`. Generalmente, `cache()` se usa para la persistencia más común y rápida (en memoria), mientras que `persist()` se utiliza cuando se necesita un control más granular sobre cómo se almacenan los datos. Es importante recordar que la persistencia es "lazy", es decir, los datos no se almacenan hasta que se ejecuta una acción sobre el DataFrame persistido por primera vez. Para despersistir un DataFrame, se utiliza `unpersist()`.

### 2.4.2 Optimización con Catalyst y Tungsten

Apache Spark se basa en dos motores de optimización clave: **Catalyst Optimizer** y **Project Tungsten**. Juntos, estos componentes son responsables de la eficiencia y el alto rendimiento que Spark logra en el procesamiento de datos a gran escala, transformando las operaciones de DataFrames y SQL en planes de ejecución optimizados y utilizando la memoria y la CPU de manera extremadamente eficiente.

##### Catalyst Optimizer: El Cerebro de la Planificación

Catalyst Optimizer es el motor de optimización de consultas de Spark SQL (y DataFrames). Funciona en varias fases para traducir las transformaciones de alto nivel que el usuario escribe en un plan de ejecución de bajo nivel y altamente optimizado. Su diseño modular y extensible permite incorporar nuevas técnicas de optimización y fuentes de datos.

Fase 1: **Análisis (Analysis)**: Spark SQL analiza la consulta (DataFrame API o SQL) para resolver referencias, verificar la sintaxis y el esquema. Convierte el árbol lógico no resuelto (unresolved logical plan) en un árbol lógico resuelto (resolved logical plan). Es decir, mapea los nombres de columnas y tablas a sus respectivas fuentes de datos.

* **Identificación de errores de esquema**: Si una columna referenciada no existe en el esquema de un DataFrame, Catalyst lo detectará en esta fase y lanzará una excepción.
* **Resolución de ambigüedades**: Si una columna existe en múltiples tablas en un join, Catalyst requiere que se califique con el nombre de la tabla para resolver la ambigüedad.

Fase 2: **Optimización Lógica (Logical Optimization)**: En esta fase, Catalyst aplica un conjunto de reglas de optimización sobre el plan lógico resuelto para reducir la cantidad de datos a procesar o el número de operaciones. Estas optimizaciones son independientes del tipo de motor de ejecución.

* **Predicado Pushdown (Predicate Pushdown)**: Si se aplica un filtro (`.where()`) a un DataFrame que se lee de una fuente de datos (como Parquet), Catalyst empujará este filtro a la fuente de datos. Esto significa que la fuente de datos leerá solo los registros que cumplan con la condición, reduciendo la cantidad de datos que se transfieren a Spark.
* **Column Pruning**: Si solo se seleccionan algunas columnas (`.select()`) de un DataFrame, Catalyst se asegura de que solo esas columnas se lean del origen de datos, en lugar de todo el conjunto de columnas.
* **Combinación de filtros**: Si se tienen múltiples condiciones `filter()` o `where()`, Catalyst puede combinarlas en una sola expresión para una evaluación más eficiente.

Fase 3: **Planificación Física (Physical Planning)**: El plan lógico optimizado se convierte en uno o más planes físicos. Aquí, Catalyst considera el entorno de ejecución (tamaño del clúster, datos en caché, etc.) y elige la mejor estrategia de ejecución para cada operación, generando código ejecutable para el motor Tungsten.

* **Elección de estrategia de Join**: Catalyst decide si usar un `Broadcast Join`, `Shuffle Hash Join`, `Sort Merge Join`, etc., basándose en el tamaño de las tablas y la configuración.
* **Manejo de agregaciones**: Decide si realizar agregaciones parciales (partial aggregations) en cada partición antes de combinarlas para reducir el shuffle.

Fase 4: **Generación de Código (Code Generation)**: La fase final donde se genera código Java bytecode dinámicamente en tiempo de ejecución para ejecutar el plan físico. Esto evita la sobrecarga de la interpretación y permite que las operaciones se ejecuten a velocidades cercanas a las de código nativo.

* **Evaluación de expresiones**: Genera código altamente optimizado para la evaluación de expresiones complejas en lugar de usar llamadas a funciones genéricas, lo que reduce la sobrecarga de la JVM.
* **Operaciones vectorizadas**: Permite la ejecución de operaciones por lotes (vectorizadas) en lugar de una fila a la vez, lo que es mucho más eficiente para operaciones como filtros y proyecciones.

##### Tungsten: El Motor de Ejecución de Bajo Nivel

Project Tungsten es una iniciativa de optimización de bajo nivel en Spark que se enfoca en mejorar el uso de la memoria y la eficiencia de la CPU. Su objetivo principal es cerrar la brecha de rendimiento entre el código Java/Scala y el código nativo, utilizando técnicas como la gestión de memoria off-heap (fuera del heap de la JVM), la serialización eficiente y la generación de código justo a tiempo (JIT).

**Gestión de Memoria Off-heap**: Tungsten permite a Spark almacenar datos directamente en la memoria fuera del heap de la JVM, en formato binario y compactado. Esto reduce la sobrecarga de la recolección de basura (Garbage Collection) de la JVM, que puede ser un cuello de botella significativo en cargas de trabajo de Big Data.

* **Agregaciones y Joins con mucha memoria**: Operaciones como `groupBy` o `join` que requieren mantener grandes tablas hash en memoria pueden beneficiarse enormemente al almacenar estas estructuras off-heap, evitando pausas prolongadas de GC.
* **Ordenamiento (Sorting)**: La clasificación de grandes volúmenes de datos puede ser más eficiente al manejar los datos directamente en memoria off-heap, reduciendo la presión sobre el heap de la JVM.

**Vectorización y Generación de Código**: Tungsten trabaja en conjunto con Catalyst para generar código optimizado que procesa los datos de forma vectorial (por lotes) en lugar de fila por fila. Esto minimiza el costo de las llamadas a funciones y permite una mejor utilización del caché de la CPU.

* **Procesamiento de columnas**: Al leer datos en formato columnar (como Parquet), Tungsten puede procesar múltiples valores de una columna a la vez, aplicando operaciones de forma más eficiente.
* **Operaciones de expresión**: Para expresiones complejas que involucran múltiples funciones (ej. `col1 + col2 * 5 - length(col3)`), Tungsten genera un único bloque de código que evalúa toda la expresión de una vez.

**Serialización Mejorada (Unsafe Row Format)**: Tungsten introduce un formato de fila binario llamado "Unsafe Row", que es muy compacto y permite un acceso a datos basado en punteros, similar a cómo se accede a los datos en C++. Esto elimina la necesidad de serialización/deserialización costosa entre pasos.

* **Reducción de I/O en shuffles**: Cuando los datos necesitan ser enviados a través de la red durante un shuffle, el formato Unsafe Row minimiza el volumen de datos a transferir, reduciendo el cuello de botella de la red.
** *Caché de datos eficiente**: Al almacenar datos en caché, el formato Unsafe Row permite que los datos se almacenen de manera más compacta y se accedan directamente sin deserialización completa, mejorando el rendimiento de las lecturas.

### 2.4.3 Broadcast joins y estrategias para evitar shuffles

El "shuffle" es una operación costosa en Spark que implica la reorganización de datos a través de la red entre los ejecutores. Ocurre en operaciones como `groupBy`, `join`, `orderBy`, y `repartition`. Minimizar los shuffles es una de las estrategias más importantes para optimizar el rendimiento en Spark. Una técnica clave para evitar shuffles en joins es el uso de `Broadcast Joins`.

##### Broadcast Join

Un `Broadcast Join` es una estrategia de join en Spark donde una de las tablas (la más pequeña) se "broadcast" (transmite) a todos los nodos del clúster que participan en la operación de join. Esto significa que cada ejecutor obtiene una copia completa de la tabla pequeña en su memoria local. Al tener la tabla pequeña localmente, cada ejecutor puede realizar el join con las particiones de la tabla grande sin necesidad de un shuffle, ya que no necesita intercambiar datos con otros ejecutores para encontrar las claves coincidentes.

Spark detecta automáticamente si una tabla es lo suficientemente pequeña (o si se le indica explícitamente con `broadcast()`) para ser transmitida. La tabla pequeña se colecta al driver, se serializa y luego se envía a cada ejecutor. Los ejecutores pueden entonces realizar un `Hash Join` con las particiones de la tabla grande.

* **Unión de una tabla de dimensiones pequeña con una tabla de hechos grande**: Por ejemplo, unir una tabla de `clientes` (miles o cientos de miles de registros) con una tabla de `transacciones` (miles de millones de registros). Si la tabla de clientes es menor que el umbral de broadcast (por defecto 10 MB en Spark 3.x, configurable con `spark.sql.autoBroadcastJoinThreshold`), Spark automáticamente realizará un Broadcast Join.
* **Filtros complejos con Lookups**: Cuando se tiene un conjunto de IDs de referencia (ej. una lista de códigos de productos a excluir) que es pequeño y se necesita filtrar o enriquecer un DataFrame muy grande. Se puede crear un pequeño DataFrame con estos IDs y luego hacer un Broadcast Join.

##### Estrategias para Evitar o Minimizar Shuffles

Más allá de los Broadcast Joins, existen otras estrategias para reducir la necesidad de shuffles o mitigar su impacto en el rendimiento.

**Predicado Pushdown y Column Pruning**: Estas optimizaciones (explicadas en la sección de Catalyst) reducen la cantidad de datos que se leen del origen y se procesan, lo que indirectamente reduce la cantidad de datos que potencialmente necesitarían ser shufflados. Al filtrar o seleccionar columnas tempranamente, se trabaja con un conjunto de datos más pequeño desde el principio.

* **Filtrado por fecha antes del join**: Si se va a unir una tabla de transacciones de varios años con una tabla de productos, y solo se necesitan transacciones del último mes, aplicar un `filter("fecha >= '2025-01-01'")` antes del join reducirá significativamente el volumen de datos que participan en el join y, por lo tanto, en cualquier shuffle subsiguiente.
* **Seleccionar solo columnas necesarias**: Si un DataFrame tiene 50 columnas pero solo se necesitan 5 para un análisis, realizar un `.select('col1', 'col2', ...)` al inicio reduce la cantidad de datos en memoria y en disco si hay shuffles.

**Co-ubicación de Datos (Co-location)**: Si los datos que se van a unir o agrupar están particionados de manera compatible en el almacenamiento subyacente (por ejemplo, en Hive o Parquet, utilizando la misma clave de partición que se usará para el join/group by), Spark puede aprovechar esto para realizar un `Sort-Merge Join` o `Hash Join` con menos o ningún shuffle. Esto requiere que las tablas se hayan escrito previamente con la misma estrategia de partición.

* **Joins entre tablas particionadas por la misma clave**: Si la tabla de `pedidos` y la tabla de `ítems_pedido` están ambas particionadas por `id_pedido`, un join entre ellas por `id_pedido` será mucho más eficiente ya que Spark puede simplemente unir las particiones coincidentes localmente en cada nodo.
* **Agregaciones en datos pre-particionados**: Si se agrupan datos por una columna que ya es la clave de partición de la tabla, Spark puede realizar agregaciones locales en cada partición antes de combinar resultados, reduciendo la cantidad de datos shufflados.

**Acumuladores y Broadcast Variables (para datos pequeños)**: Aunque no evitan directamente un shuffle en DataFrames en la misma medida que un Broadcast Join, los acumuladores y las broadcast variables (a nivel de RDD, pero también útiles para datos pequeños en Spark) son herramientas para compartir datos pequeños de manera eficiente entre tareas. Las broadcast variables permiten enviar un valor de solo lectura a todos los nodos, útil para tablas de búsqueda o configuraciones.

* **Listas de bloqueo o mapeos**: Transmitir una lista pequeña de IDs prohibidos o un mapa de códigos a descripciones a todos los ejecutores para filtrar o enriquecer datos sin realizar un join formal.
* **Parámetros de configuración dinámicos**: Si un algoritmo necesita un conjunto de parámetros que cambian dinámicamente pero es pequeño, se puede transmitir usando una broadcast variable.

**Uso de `repartition` y `coalesce` con cuidado**: Ambas funciones se utilizan para cambiar el número de particiones de un DataFrame. `repartition` siempre implica un shuffle completo de los datos, mientras que `coalesce` intenta reducir el número de particiones sin un shuffle completo si es posible (solo combina particiones existentes dentro del mismo nodo). Utilízalas solo cuando sea estrictamente necesario (ej. para uniones eficientes con datos de gran tamaño, o para reducir el número de archivos de salida).

* **Reducir archivos de salida (small files problem)**: Si un procesamiento genera miles de archivos pequeños (problema de "small files"), un `df.coalesce(N).write.parquet(...)` puede combinarlos en `N` archivos más grandes al final de la operación, aunque `coalesce` también puede implicar un shuffle si se reduce el número de particiones drásticamente.
* **Preparación para operaciones posteriores**: En algunos escenarios, re-particionar un DataFrame por la clave de join *antes* del join puede ser beneficioso si se planean múltiples joins o agregaciones sobre la misma clave, aunque es una decisión que debe tomarse con base en un análisis de rendimiento.

### 2.4.4 Consideraciones de escalabilidad y desempeño

La escalabilidad y el desempeño en Spark van más allá de la optimización de código individual; involucran la configuración del clúster, la gestión de recursos y la elección de las arquitecturas de datos adecuadas. Comprender estos aspectos es fundamental para diseñar aplicaciones Spark robustas que puedan manejar volúmenes de datos crecientes y mantener un rendimiento óptimo en entornos de producción.

##### Configuración del Clúster y Asignación de Recursos

Una configuración adecuada de Spark y la asignación de recursos a los ejecutores son críticas para el desempeño. Un clúster mal configurado puede llevar a cuellos de botella incluso con el código más optimizado.

**Tamaño de los Ejecutores (`spark.executor.cores`, `spark.executor.memory`)**: Estos parámetros controlan cuántos núcleos de CPU y cuánta memoria se asignan a cada ejecutor. Un número adecuado de núcleos permite el paralelismo, mientras que suficiente memoria evita derrames a disco y optimiza el caché. Un error común es tener ejecutores muy grandes (pocos ejecutores con muchos núcleos/memoria) o muy pequeños (muchos ejecutores con pocos recursos).

* **Clúster con nodos de 64GB RAM, 16 núcleos**: Una configuración común podría ser `spark.executor.cores=5` y `spark.executor.memory=20GB`. Esto permite tener 2 ejecutores por nodo y deja memoria para el sistema operativo y el driver, maximizando la utilización de recursos sin sobrecargar.
* **Tareas con mucha memoria (e.g., joins grandes sin broadcast)**: Aumentar `spark.executor.memory` puede ser necesario para evitar derrames a disco durante operaciones intensivas en memoria.

**Memoria del Driver (`spark.driver.memory`)**: La memoria asignada al nodo driver. El driver coordina las tareas, almacena metadatos y, en algunos casos, colecta resultados (como `collect()`). Si se realizan operaciones que recolectan grandes cantidades de datos al driver, o si se manejan muchas broadcast variables, se necesita más memoria para el driver.

* **Usar `collect()` en un DataFrame grande**: Si se intenta `df.collect()` sobre un DataFrame con millones de filas, el driver podría quedarse sin memoria. Ajustar `spark.driver.memory` o reestructurar el código para evitar `collect()` en grandes volúmenes.
* **Broadcast de múltiples tablas pequeñas**: Si se transmiten muchas tablas pequeñas, la memoria del driver podría verse afectada.

**Configuración de Shuffle (`spark.shuffle.service.enabled`, `spark.shuffle.file.buffer`)**: Estos parámetros afectan cómo Spark maneja los datos durante las operaciones de shuffle. Habilitar el servicio de shuffle externo (`spark.shuffle.service.enabled=true`) permite que los datos shufflados persistan incluso si un ejecutor falla, mejorando la fiabilidad. Ajustar el tamaño del buffer (`spark.shuffle.file.buffer`) puede optimizar las escrituras a disco durante el shuffle.

* **Estabilidad en shuffles grandes**: En entornos de producción con shuffles frecuentes y grandes, habilitar el shuffle service es crucial para la estabilidad y resiliencia.
* **Rendimiento de I/O en shuffles**: Para tareas con mucha escritura a disco durante shuffles, aumentar el buffer puede reducir la cantidad de pequeñas escrituras y mejorar el rendimiento.

##### Monitoreo y Diagnóstico

Monitorear las aplicaciones Spark es esencial para identificar cuellos de botella y comprender el comportamiento del rendimiento. Spark UI es la herramienta principal para esto.

**Spark UI (Stages, Tasks, DAG Visualization)**: La interfaz de usuario de Spark (accesible generalmente en `http://<driver-ip>:4040`) proporciona una visión detallada de las etapas (Stages), tareas (Tasks), y el Grafo Acíclico Dirigido (DAG) de la aplicación. Permite identificar qué etapas son lentas, si hay desequilibrio de datos (skew), o si los ejecutores están infrautilizados/sobrecargados.

* **Identificar Stage lento**: Si una etapa particular (e.g., un `join` o `groupBy`) toma mucho tiempo, se puede profundizar en esa etapa para ver si alguna tarea está tardando más de lo normal (indicando skew) o si hay problemas de I/O.
* **Análisis de Shuffles**: La Spark UI muestra el tamaño de los datos shufflados y el tiempo que toma. Un shuffle excesivo es una señal de que las estrategias de optimización (como Broadcast Join) podrían ser necesarias.
* **Revisar el plan de ejecución**: En la pestaña "SQL" de la Spark UI, se puede ver el plan de ejecución generado por Catalyst, lo que ayuda a entender cómo Spark está procesando la consulta y si se están aplicando las optimizaciones esperadas (e.g., Predicate Pushdown).

**Métricas de Recursos (CPU, Memoria, Disk I/O)**: Además de Spark UI, es importante monitorear las métricas a nivel de clúster (CPU, uso de memoria, E/S de disco, red) para cada nodo ejecutor. Esto ayuda a identificar si el problema es de Spark en sí o si hay una limitación de recursos a nivel de infraestructura.

* **CPU subutilizada**: Si la CPU de los ejecutores está consistentemente baja durante una etapa que se esperaba intensiva en CPU, podría indicar un problema de paralelismo o un cuello de botella en I/O.
* **Memoria agotada**: Si los ejecutores están reportando errores OOM (Out Of Memory) o si hay mucho spill a disco, es una señal de que los DataFrames son demasiado grandes para la memoria disponible o que la configuración de persistencia es ineficiente.

##### Consideraciones de Diseño de Datos

La forma en que se almacenan y estructuran los datos tiene un impacto directo en el rendimiento de Spark.

**Formato de Archivos (Parquet, ORC)**: Optar por formatos de archivo columnares como Parquet u ORC es crucial. Estos formatos son auto-descriptivos, comprimidos y permiten optimizaciones como "Predicate Pushdown" y "Column Pruning" de forma nativa, lo que reduce drásticamente la cantidad de datos leídos del disco.

* **Reemplazar CSV/JSON por Parquet**: Migrar conjuntos de datos de formatos basados en filas (CSV, JSON) a Parquet para obtener mejoras significativas en el rendimiento de lectura y la eficiencia del almacenamiento.
* **Beneficios del Predicate Pushdown**: Si se tiene una tabla Parquet de logs y se filtra por `timestamp > 'X'`, Spark solo leerá los bloques de datos relevantes, sin necesidad de escanear todo el archivo.

**Particionamiento de Datos**: Organizar los datos en el sistema de archivos (HDFS, S3) en directorios basados en valores de columna (ej. `data/year=2024/month=01/`). Esto permite a Spark (y otras herramientas de Big Data) "podar" particiones (partition pruning), es decir, escanear solo los directorios relevantes para una consulta, evitando la lectura de datos innecesarios.

* **Consulta de datos por fecha**: Si los datos de ventas están particionados por `año/mes/día`, una consulta para las ventas de un día específico solo escaneará el directorio correspondiente a ese día, no toda la tabla.
* **Optimizando Joins con partición por clave**: Si se va a unir por una clave y ambas tablas están particionadas por esa misma clave, se puede evitar un shuffle completo (co-ubicación, como se mencionó anteriormente).

**Tamaño de los Archivos (Small Files Problem)**: Tener un gran número de archivos pequeños (ej. miles de archivos de unos pocos KB) en un sistema de archivos distribuido (como HDFS) puede degradar seriamente el rendimiento. Esto se debe a la sobrecarga de gestionar metadatos para cada archivo y la ineficiencia de la lectura de muchos archivos pequeños.

* **Compactación de datos históricos**: Si se ingieren datos en pequeños lotes, periódicamente se debe ejecutar un proceso de "compactación" que combine estos pequeños archivos en unos pocos archivos más grandes (e.g., 128 MB a 1 GB) para optimizar las lecturas futuras.
* **Ajustar el número de particiones de salida**: Al escribir resultados, se puede usar `repartition()` o `coalesce()` para controlar el número de archivos de salida y evitar generar demasiados pequeños.

## Tarea

1.  **Ejercicio de Persistencia con diferentes niveles**: Crea un DataFrame con 10 millones de filas y varias columnas. Realiza una serie de transformaciones sobre él. Luego, persiste el DataFrame utilizando `MEMORY_ONLY`, `MEMORY_AND_DISK`, y `DISK_ONLY` (en diferentes ejecuciones). Mide el tiempo de ejecución de las operaciones posteriores a la primera acción para cada nivel de persistencia y explica las diferencias observadas en el rendimiento y el uso de memoria/disco (usando Spark UI).

2.  **Identificación de un Shuffle Costoso**: Diseña un escenario donde se genere un DataFrame grande y se realice una operación que cause un shuffle significativo (ej. un `groupBy` por una columna de alta cardinalidad o un `join` entre dos DataFrames grandes sin una estrategia de optimización). Utiliza Spark UI para identificar la etapa del shuffle, el volumen de datos shufflados y el tiempo que consume.

3.  **Implementación de Broadcast Join**: Toma el escenario del ejercicio anterior (o crea uno similar con un join entre una tabla grande y una pequeña). Implementa un `Broadcast Join` utilizando `F.broadcast()` y compara el tiempo de ejecución y la ausencia/reducción del shuffle en Spark UI con respecto a un `join` normal.

4.  **Predicado Pushdown y Column Pruning en acción**: Lee un archivo Parquet con múltiples columnas y filtra por una columna indexada (si el formato lo permite) y selecciona solo un subconjunto de columnas. Observa el plan de ejecución en Spark UI (pestaña SQL -> Details) y explica cómo Catalyst aplica el Predicado Pushdown y el Column Pruning.

5.  **Simulación de "Small Files Problem" y Solución**: Escribe un DataFrame grande en 1000 archivos pequeños. Luego, lee esos 1000 archivos y escribe el resultado en 10 archivos más grandes utilizando `coalesce()` o `repartition()`. Mide y compara los tiempos de lectura y escritura.

6.  **Configuración de Ejecutores**: Experimenta con la configuración de `spark.executor.cores` y `spark.executor.memory` en una aplicación Spark. Ejecuta una carga de trabajo intensiva (ej. un join complejo o una agregación grande) con diferentes configuraciones (ej. pocos ejecutores grandes vs. muchos ejecutores pequeños, manteniendo el mismo número total de núcleos y memoria para el clúster). Analiza el impacto en el rendimiento y la utilización de recursos en Spark UI.

7.  **UDFs vs. Funciones Nativas para Optimización**: Crea una función para manipular una columna (ej. concatenar strings, transformar un valor numérico). Primero, implementa la lógica como una UDF. Luego, re-implementa la misma lógica utilizando solo funciones nativas de Spark SQL (ej. `concat_ws`, `when`, `lit`). Compara el rendimiento de ambas implementaciones y explica por qué una es más eficiente.

8.  **Análisis de Derrames a Disco (Spill)**: Ejecuta una operación Spark (ej. un `groupBy` con una clave de muy alta cardinalidad y un agregado complejo) con memoria de ejecutor limitada, forzando que Spark "derrame" datos al disco (spill). Monitorea el evento en Spark UI y explica qué significa el "spill" y cómo afecta el rendimiento.

9.  **Optimización de Sort**: Genera un DataFrame con datos aleatorios y ordénalo utilizando `orderBy`. Luego, compara el plan de ejecución y el rendimiento si los datos ya estuvieran pre-ordenados o si se aplicara una partición adecuada antes del ordenamiento (esto puede ser un ejercicio conceptual o simulado).

10. **Lectura con Particionamiento (Partition Pruning)**: Crea un DataFrame grande y escríbelo en Parquet particionando por una columna (ej. `df.write.partitionBy("ciudad").parquet("path/to/data")`). Luego, lee los datos filtrando por esa columna particionada (ej. `spark.read.parquet("path/to/data").filter("ciudad = 'Bogota'")`). Observa en Spark UI cómo Spark solo escanea los directorios relevantes, demostrando el "partition pruning".

