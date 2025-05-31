# 1. Introducción

## Tema 1.3 RDD, DataFrame y Dataset

**Objetivo**:

Comprender las diferencias, ventajas y casos de uso de las principales abstracciones de datos en Apache Spark: RDD, DataFrame y Dataset, permitiendo a los estudiantes seleccionar la herramienta adecuada para diversas tareas de procesamiento de datos.

**Introducción**:

Apache Spark ofrece diferentes abstracciones para trabajar con datos, cada una con sus propias características y optimizaciones. Los **Resilient Distributed Datasets (RDDs)** fueron la abstracción original, proporcionando un control de bajo nivel. Posteriormente, surgieron los **DataFrames** para manejar datos estructurados y semiestructurados con optimizaciones de rendimiento significativas. Finalmente, los **Datasets** combinaron las ventajas de ambos, ofreciendo seguridad de tipos y las optimizaciones de los DataFrames. Dominar estas tres abstracciones es fundamental para explotar todo el potencial de Spark en el procesamiento de Big Data.

**Desarrollo**:

En este tema, exploraremos en detalle RDDs, DataFrames y Datasets, las tres principales formas de representar y manipular datos en Apache Spark. Cada una representa un paso evolutivo en la API de Spark, diseñada para mejorar la facilidad de uso, el rendimiento y la seguridad de tipos. Analizaremos sus características distintivas, cómo interactúan entre sí y en qué escenarios es más apropiado utilizar cada una, lo que te permitirá construir aplicaciones Spark más eficientes y robustas.

### 1.3.1 RDD (Resilient Distributed Datasets)

Los **RDDs** (Resilient Distributed Datasets) son la colección fundamental de elementos tolerantes a fallos en Spark que se ejecutan en paralelo. Fueron la abstracción de datos original de Spark y representan una colección inmutable y particionada de registros. Los RDDs pueden ser creados a partir de fuentes de datos externas (como HDFS, S3, HBASE) o a partir de colecciones existentes en lenguajes de programación como Scala, Python o Java. Su principal fortaleza radica en su naturaleza inmutable y en la capacidad de Spark para reconstruirlos automáticamente en caso de fallos de nodos, gracias a su mecanismo de linaje.

##### Características clave de los RDDs

Los RDDs son fundamentalmente colecciones de objetos inmutables distribuidas entre un clúster. Son "resilientes" porque pueden recuperarse de fallos reconstruyendo sus particiones a partir de las operaciones que los generaron (su linaje). Son "distribuidos" porque sus datos se reparten entre múltiples nodos, permitiendo el procesamiento en paralelo. Ofrecen una API de bajo nivel, lo que da un control granular sobre las operaciones de transformación y acción, pero carecen de información de esquema inherente, lo que puede limitar las optimizaciones de Spark.

1.  Procesamiento de **archivos de log sin formato** donde cada línea es un string y no se conoce una estructura fija.
2.  Implementación de **algoritmos de Machine Learning personalizados** que requieren control detallado sobre las estructuras de datos y el procesamiento por bloques.
3.  Trabajar con **datos binarios complejos o formatos propietarios** para los cuales no existen *parsers* o esquemas predefinidos en Spark.

##### Operaciones de transformación

Las **transformaciones** en RDDs son operaciones que crean un nuevo RDD a partir de uno existente. Son de naturaleza *lazy* (perezosa), lo que significa que no se ejecutan inmediatamente. En su lugar, Spark registra la transformación en un *linaje* o *DAG* (Directed Acyclic Graph) de operaciones. Esto permite a Spark optimizar el plan de ejecución antes de realizar cualquier cálculo real. Algunos ejemplos comunes incluyen `map`, `filter`, `flatMap`, `union`, `groupByKey`.

1.  Usar `rdd.map(lambda x: x.upper())` para convertir todas las cadenas de texto en un RDD a mayúsculas.
2.  Utilizar `rdd.filter(lambda x: "error" in x)` para seleccionar solo las líneas de un log que contienen la palabra "error".
3.  Aplicar `rdd1.union(rdd2)` para combinar dos RDDs en uno solo.

##### Operaciones de acción

Las **acciones** en RDDs son operaciones que disparan la ejecución de las transformaciones y devuelven un resultado al programa Driver o escriben datos en un sistema de almacenamiento externo. A diferencia de las transformaciones, las acciones son *eager* (ávidas), lo que significa que fuerzan la evaluación del DAG de transformaciones. Ejemplos incluyen `collect`, `count`, `reduce`, `saveAsTextFile`, `foreach`.

1.  Usar `rdd.collect()` para obtener todos los elementos del RDD como una lista en el programa Driver (tener cuidado con RDDs muy grandes).
2.  Aplicar `rdd.count()` para obtener el número de elementos en el RDD.
3.  Utilizar `rdd.saveAsTextFile("ruta/salida")` para escribir el contenido del RDD en un archivo de texto en el sistema de archivos distribuido.

### 1.3.2 DataFrame

Un **DataFrame** en Apache Spark es una colección distribuida de datos organizada en columnas con nombre. Se puede pensar en un DataFrame como una tabla en una base de datos relacional o una tabla en R/Python, pero con la capacidad de escalar a terabytes de datos en un clúster. A diferencia de los RDDs, los DataFrames tienen un esquema (estructura) definido, lo que permite a Spark realizar optimizaciones de rendimiento significativas a través de su optimizador Catalyst. Los DataFrames son la interfaz de programación preferida para la mayoría de los casos de uso de Spark, especialmente cuando se trabaja con datos estructurados y semiestructurados.

##### Ventajas sobre los RDDs

Los DataFrames ofrecen varias ventajas clave sobre los RDDs, principalmente debido a su conocimiento del esquema de los datos. Esto permite optimizaciones de rendimiento a nivel de motor, una sintaxis más expresiva y familiar para usuarios de SQL o Pandas, y una mejor interoperabilidad con diferentes fuentes de datos y herramientas de análisis.

1.  Spark puede optimizar automáticamente las operaciones de un DataFrame (por ejemplo, el orden de los filtros o *joins*) usando el optimizador **Catalyst**, algo que no es posible con los RDDs.
2.  La sintaxis de los DataFrames es mucho más intuitiva y menos propensa a errores que las operaciones de bajo nivel de los RDDs, especialmente para tareas comunes como filtrar, seleccionar columnas o agregar datos.
3.  Los DataFrames permiten ejecutar consultas SQL directamente sobre ellos, facilitando la integración con herramientas de BI y la familiaridad para usuarios de bases de datos.

##### Creación y manipulación de DataFrames

Los DataFrames se pueden crear a partir de una amplia variedad de fuentes de datos, incluyendo archivos CSV, JSON, Parquet, Hive tables, bases de datos JDBC, e incluso RDDs existentes. Una vez creados, Spark ofrece una API rica y expresiva para manipularlos, ya sea a través de un DSL (Domain Specific Language) con funciones de alto nivel o mediante consultas SQL.

1.  Cargar un archivo Parquet en un DataFrame: `spark.read.parquet("ruta/a/archivo.parquet")`.
2.  Seleccionar columnas y filtrar filas: `df.select("nombre", "edad").filter(df.edad > 30)`.
3.  Realizar una agregación: `df.groupBy("departamento").agg(avg("salario").alias("salario_promedio"))`.

### 1.3.3 Dataset

El **Dataset API** fue introducido en Spark 1.6 como un intento de proporcionar lo mejor de ambos mundos: la eficiencia y optimizaciones de rendimiento de los DataFrames, junto con la seguridad de tipos y la capacidad de usar funciones lambda que caracterizan a los RDDs. Los Datasets son fuertemente tipados, lo que significa que los errores relacionados con el tipo de datos pueden detectarse en tiempo de compilación (solo en Scala y Java), en lugar de en tiempo de ejecución, lo que lleva a un código más robusto. En esencia, un DataFrame es un `Dataset[Row]`, donde `Row` es un tipo genérico y no tiene seguridad de tipos en tiempo de compilación. Los Datasets requieren un *Encoder* para serializar y deserializar los objetos entre el formato de JVM y el formato binario interno de Spark.

##### Seguridad de tipos (Type-safety)

La **seguridad de tipos** es la principal ventaja de los Datasets sobre los DataFrames para los usuarios de Scala y Java. Permite a los desarrolladores trabajar con objetos fuertemente tipados, lo que significa que el compilador puede verificar los tipos de datos y detectar errores en tiempo de compilación. Esto reduce la posibilidad de errores en tiempo de ejecución que podrían surgir al intentar acceder a campos inexistentes o realizar operaciones con tipos incompatibles, algo común con DataFrames (donde tales errores solo se manifiestan al ejecutar el código).

1.  En Scala, si se tiene un `Dataset[Person]`, donde `Person` es una *case class* con campos `name` y `age`, el compilador detectará un error si se intenta acceder a `person.address` si `address` no es un campo de la clase `Person`.
2.  Al realizar transformaciones en un `Dataset[Product]`, las operaciones se aplican directamente sobre los objetos `Product`, aprovechando la autocompletación y las verificaciones del IDE.
3.  La refactorización de código es más segura y sencilla con Datasets, ya que los cambios en el esquema se detectan de inmediato por el compilador.

##### Encoders

Los **Encoders** son un mecanismo de serialización que Spark utiliza para convertir objetos de JVM (como las *case classes* de Scala o los POJOs de Java) en el formato binario interno de Spark (formato Tungsten) y viceversa. Los Encoders son más eficientes que la serialización de Java u otros mecanismos porque generan código para serializar y deserializar datos de forma compacta y rápida, permitiendo a Spark realizar operaciones directamente sobre el formato binario optimizado, lo que contribuye a las mejoras de rendimiento de los Datasets.

1.  Al crear un `Dataset[Long]`, Spark utiliza un `Encoder` optimizado para los tipos `Long`, que sabe cómo representar y operar sobre estos números de manera eficiente en formato binario.
2.  Si tienes una `case class Person(name: String, age: Int)`, el *Encoder* para `Person` sabrá cómo convertir una lista de objetos `Person` en un formato de columnas de Spark y viceversa.
3.  Los *Encoders* permiten que las operaciones de Datasets sean tan eficientes como las de DataFrames, ya que ambos utilizan el mismo formato de almacenamiento y motor de ejecución optimizado.

### 1.3.4 Comparación y Casos de Uso

La elección entre RDD, DataFrame y Dataset depende en gran medida del tipo de datos con el que se está trabajando, las necesidades de rendimiento, la seguridad de tipos deseada y el lenguaje de programación que se utiliza. Aunque las APIs de DataFrame y Dataset son las más recomendadas para la mayoría de los casos de uso modernos, entender las capacidades de los RDDs sigue siendo importante, especialmente para escenarios de bajo nivel o depuración.

##### Tabla comparativa detallada: RDD vs. DataFrame vs. Dataset

| Característica        | RDD                               | DataFrame                                     | Dataset                                         |
| :-------------------- | :-------------------------------- | :-------------------------------------------- | :---------------------------------------------- |
| **Abstracción** | Colección distribuida de objetos  | Colección distribuida de `Row` objetos con esquema | Colección distribuida de objetos fuertemente tipados con esquema |
| **Optimización** | Manual (sin optimizador)          | **Catalyst Optimizer** (optimización automática) | **Catalyst Optimizer** (optimización automática) |
| **Seguridad de Tipos**| No (colección de `Object`)       | No (en tiempo de compilación, `Row` es genérico) | **Sí** (en tiempo de compilación, para Scala/Java) |
| **API** | Bajo nivel, funcional             | Alto nivel, SQL-like (DSL, SQL)               | Alto nivel, funcional y SQL-like                |
| **Serialización** | Java Serialization / Kryo         | Tungsten (binario optimizado)                 | Tungsten (binario optimizado con Encoders)      |
| **Rendimiento** | Bueno, pero puede ser menor que DataFrame/Dataset | **Alto** (optimización automática)            | **Alto** (optimización automática + Encoders)   |
| **Lenguajes** | Scala, Java, Python, R            | Scala, Java, Python, R                        | **Scala, Java** (principalmente)               |
| **Mutabilidad** | Inmutable                         | Inmutable                                     | Inmutable                                       |

1.  Para un **análisis de logs complejos y no estructurados** donde necesitas un control muy granular sobre cada línea y las operaciones de bajo nivel, los **RDDs** son la opción adecuada.
2.  Para **consultas analíticas sobre datos de ventas estructurados** almacenados en Parquet, donde se busca eficiencia y facilidad de uso con sintaxis SQL, los **DataFrames** son la mejor elección.
3.  Si estás desarrollando una **aplicación de procesamiento de datos en Scala o Java** que requiere la máxima seguridad de tipos en tiempo de compilación y las optimizaciones de rendimiento de Spark, un **Dataset** es el camino a seguir.

## Tarea

1.  Explica la diferencia entre una **transformación** y una **acción** en Spark. Proporciona un ejemplo de código para cada una y describe cómo la evaluación perezosa afecta su ejecución.
2.  Considera un escenario donde tienes una tabla de clientes y otra de pedidos. Describe cómo usarías **DataFrames** para unir estas dos tablas y calcular el monto total de pedidos por cliente, utilizando tanto la API DSL como una consulta SQL.
3.  Imagina que estás depurando una aplicación Spark y notas que un RDD particular se está recalculando varias veces. ¿Cómo usarías el concepto de **persistencia (caching)** para optimizar el rendimiento en este escenario? Proporciona un ejemplo de código.

