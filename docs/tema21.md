# 2. PySpark y SparkSQL

## Tema 2.1 Fundamentos de DataFrames en Spark

**Objetivo**:

Al finalizar este tema, el estudiante será capaz de comprender la estructura y el funcionamiento de los DataFrames de Apache Spark, realizar operaciones fundamentales de manipulación de datos, gestionar esquemas y tipos de datos complejos, y leer y escribir datos en los formatos más comunes utilizados en entornos de Big Data.

**Introducción**:

En el vasto universo del Big Data, la capacidad de procesar y analizar volúmenes masivos de información es crucial. Apache Spark, con su motor de procesamiento distribuido, se ha consolidado como una herramienta indispensable para esta tarea. En el corazón de su eficiencia y facilidad de uso se encuentran los DataFrames, una abstracción de datos distribuida que organiza los datos en columnas con nombre, similar a una tabla en una base de datos relacional o una hoja de cálculo. Esta estructura permite a los desarrolladores trabajar con datos de forma intuitiva y optimizada, aprovechando el poder de Spark para el procesamiento paralelo y distribuido.

**Desarrollo**:

Este tema se centrará en los pilares de la manipulación de datos en Spark a través de los DataFrames. Exploraremos cómo los DataFrames facilitan las operaciones de transformación y consulta, abstraen la complejidad de la distribución de datos y proporcionan un API robusto para interactuar con ellos. Abordaremos desde las operaciones básicas como selección y filtrado, hasta la comprensión de los esquemas y tipos de datos complejos, y la interacción con una variedad de formatos de archivo estándar de la industria.

### 2.1.1 Operaciones con DataFrames

Las operaciones con DataFrames en Spark son el núcleo de la manipulación de datos, permitiendo seleccionar, filtrar, agregar, unir y realizar una multitud de transformaciones sobre conjuntos de datos distribuidos de manera eficiente. A diferencia de los RDDs, los DataFrames ofrecen un nivel de abstracción superior, permitiendo a Spark optimizar internamente las operaciones gracias a la información del esquema.

##### Creación de DataFrames

La creación de DataFrames es el primer paso para trabajar con datos en PySpark. Se pueden crear a partir de diversas fuentes, como listas de Python, RDDs existentes, o leyendo directamente desde archivos.

1.  **Desde una lista de tuplas o diccionarios:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DataFrameOperations").getOrCreate()

# Opción 1: Usando una lista de tuplas y definiendo el esquema
data_tuples = [("Alice", 1, "NY"), ("Bob", 2, "LA"), ("Charlie", 3, "CHI")]
schema_tuples = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("city", StringType(), True)
])
df_from_tuples = spark.createDataFrame(data_tuples, schema=schema_tuples)
df_from_tuples.show()
# Resultado:
# +-------+---+----+
# |   name| id|city|
# +-------+---+----+
# |  Alice|  1|  NY|
# |    Bob|  2|  LA|
# |Charlie|  3| CHI|
# +-------+---+----+

# Opción 2: Usando una lista de diccionarios (Spark infiere el esquema)
data_dicts = [{"name": "Alice", "id": 1, "city": "NY"},
              {"name": "Bob", "id": 2, "city": "LA"},
              {"name": "Charlie", "id": 3, "city": "CHI"}]
df_from_dicts = spark.createDataFrame(data_dicts)
df_from_dicts.show()
# Resultado:
# +-------+---+----+
# |   city| id|name|
# +-------+---+----+
# |     NY|  1|Alice|
# |     LA|  2|  Bob|
# |    CHI|  3|Charlie|
# +-------+---+----+
```

2.  **Desde un RDD existente:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameOperations").getOrCreate()

rdd = spark.sparkContext.parallelize([("Alice", 1), ("Bob", 2), ("Charlie", 3)])
df_from_rdd = spark.createDataFrame(rdd, ["name", "id"])
df_from_rdd.show()
# Resultado:
# +-------+---+
# |   name| id|
# +-------+---+
# |  Alice|  1|
# |    Bob|  2|
# |Charlie|  3|
# +-------+---+
```

3.  **Desde un archivo (se verá en 2.1.3):**

```python
# df = spark.read.csv("path/to/your/file.csv", header=True, inferSchema=True)
# df.show()
```

##### Transformaciones de DataFrames

Las transformaciones en DataFrames son operaciones *lazy* (perezosas), lo que significa que no se ejecutan hasta que se invoca una acción. Esto permite a Spark optimizar el plan de ejecución.

1.  **Selección de columnas (`select` y `withColumn`):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("DataFrameTransformations").getOrCreate()

data = [("Alice", 1, "NY"), ("Bob", 2, "LA"), ("Charlie", 3, "CHI")]
df = spark.createDataFrame(data, ["name", "id", "city"])

# Seleccionar columnas específicas
df.select("name", "city").show()
# Resultado:
# +-------+----+
# |   name|city|
# +-------+----+
# |  Alice|  NY|
# |    Bob|  LA|
# |Charlie| CHI|
# +-------+----+

# Renombrar una columna al seleccionar
df.select(col("name").alias("full_name"), "city").show()
# Resultado:
# +---------+----+
# |full_name|city|
# +---------+----+
# |    Alice|  NY|
# |      Bob|  LA|
# |  Charlie| CHI|
# +---------+----+

# Añadir una nueva columna
df.withColumn("country", lit("USA")).show()
# Resultado:
# +-------+---+----+-------+
# |   name| id|city|country|
# +-------+---+----+-------+
# |  Alice|  1|  NY|    USA|
# |    Bob|  2|  LA|    USA|
# |Charlie|  3| CHI|    USA|
# +-------+---+----+-------+

# Modificar una columna existente (ejemplo: incrementar id)
df.withColumn("id_plus_10", col("id") + 10).show()
# Resultado:
# +-------+---+----+----------+
# |   name| id|city|id_plus_10|
# +-------+---+----+----------+
# |  Alice|  1|  NY|        11|
# |    Bob|  2|  LA|        12|
# |Charlie|  3| CHI|        13|
# +-------+---+----+----------+
```

2.  **Filtrado de filas (`filter` o `where`):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataFrameFiltering").getOrCreate()

data = [("Alice", 25, "NY"), ("Bob", 30, "LA"), ("Charlie", 22, "CHI"), ("David", 35, "NY")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# Filtrar por una condición simple
df.filter(col("age") > 25).show()
# Resultado:
# +-----+---+----+
# | name|age|city|
# +-----+---+----+
# |  Bob| 30|  LA|
# |David| 35|  NY|
# +-----+---+----+

# Filtrar por múltiples condiciones
df.filter((col("age") > 20) & (col("city") == "NY")).show()
# Resultado:
# +-----+---+----+
# | name|age|city|
# +-----+---+----+
# |Alice| 25|  NY|
# |David| 35|  NY|
# +-----+---+----+

# Usando el método where (alias de filter)
df.where(col("name").like("A%")).show()
# Resultado:
# +-----+---+----+
# | name|age|city|
# +-----+---+----+
# |Alice| 25|  NY|
# +-----+---+----+
```

3.  **Agregaciones (`groupBy` y funciones de agregación):**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, min, max

spark = SparkSession.builder.appName("DataFrameAggregations").getOrCreate()

data = [("Dept1", "Alice", 1000), ("Dept1", "Bob", 1200), ("Dept2", "Charlie", 900), ("Dept2", "David", 1500)]
df = spark.createDataFrame(data, ["department", "name", "salary"])

# Contar empleados por departamento
df.groupBy("department").count().show()
# Resultado:
# +----------+-----+
# |department|count|
# +----------+-----+
# |     Dept1|    2|
# |     Dept2|    2|
# +----------+-----+

# Calcular salario promedio y máximo por departamento
df.groupBy("department").agg(avg("salary").alias("avg_salary"),
                             max("salary").alias("max_salary")).show()
# Resultado:
# +----------+----------+----------+
# |department|avg_salary|max_salary|
# +----------+----------+----------+
# |     Dept1|    1100.0|      1200|
# |     Dept2|    1200.0|      1500|
# +----------+----------+----------+

# Sumar salarios por departamento
df.groupBy("department").agg(sum("salary").alias("total_salary")).show()
# Resultado:
# +----------+------------+
# |department|total_salary|
# +----------+------------+
# |     Dept1|        2200|
# |     Dept2|        2400|
# +----------+------------+
```

##### Acciones de DataFrames

Las acciones son operaciones que disparan la ejecución del plan de transformaciones y devuelven un resultado a la aplicación del controlador o escriben datos en un sistema de almacenamiento.

1.  **Mostrar datos (`show`):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameActions").getOrCreate()

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])

# Mostrar las primeras filas del DataFrame
df.show()
# Resultado:
# +-----+---+
# | name|age|
# +-----+---+
# |Alice| 25|
# |  Bob| 30|
# +-----+---+

# Mostrar un número específico de filas y truncar el contenido de las columnas si es largo
df.show(numRows=1, truncate=False)
# Resultado:
# +-----+---+
# |name |age|
# +-----+---+
# |Alice|25 |
# +-----+---+
```

2.  **Contar filas (`count`):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameActions").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]
df = spark.createDataFrame(data, ["name", "age"])

# Contar el número total de filas en el DataFrame
num_rows = df.count()
print(f"Número de filas: {num_rows}")
# Resultado: Número de filas: 3
```

3.  **Recopilar datos (`collect`, `take`, `first`):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameActions").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 22)]
df = spark.createDataFrame(data, ["name", "age"])

# Recopilar todos los datos del DataFrame en una lista de Rows en el driver
all_data = df.collect()
print(f"Todos los datos: {all_data}")
# Resultado: Todos los datos: [Row(name='Alice', age=25), Row(name='Bob', age=30), Row(name='Charlie', age=22)]

# Tomar las primeras N filas
first_two = df.take(2)
print(f"Primeras 2 filas: {first_two}")
# Resultado: Primeras 2 filas: [Row(name='Alice', age=25), Row(name='Bob', age=30)]

# Obtener la primera fila
first_row = df.first()
print(f"Primera fila: {first_row}")
# Resultado: Primera fila: Row(name='Alice', age=25)
```

### 2.1.2 Esquemas y tipos de datos complejos

El esquema de un DataFrame es una estructura fundamental que define los nombres de las columnas y sus tipos de datos correspondientes. Esta metadata es crucial para la optimización de Spark, ya que le permite saber cómo se organizan los datos y aplicar optimizaciones de tipo de datos y de columna. La inferencia de esquema y la definición explícita son dos formas de manejarlo, y Spark también soporta tipos de datos complejos como `ArrayType`, `MapType` y `StructType` para manejar estructuras anidadas.

##### Inferencia y Definición Explícita de Esquemas

La forma en que Spark determina el esquema de un DataFrame es vital para la integridad y eficiencia del procesamiento de datos.

1.  **Inferencia de esquema (`inferSchema=True`):**

Spark puede intentar adivinar el esquema de un archivo de datos (CSV, JSON, Parquet, etc.) leyendo una muestra. Esto es conveniente para la exploración inicial, pero puede ser propenso a errores, especialmente con datos inconsistentes o tipos ambiguos.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaInference").getOrCreate()

# Creando un archivo CSV de ejemplo
data_csv = """name,age,city
Alice,25,NY
Bob,30,LA
Charlie,null,CHI
David,35,NY"""
with open("data.csv", "w") as f:
    f.write(data_csv)

# Inferencia de esquema al leer un CSV
df_inferred = spark.read.csv("data.csv", header=True, inferSchema=True)
df_inferred.printSchema()
# Resultado (ejemplo):
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- city: string (nullable = true)

# Observar que "age" se infirió como IntegerType, lo cual es correcto si no hay valores no numéricos.
# Si hubiera un valor no numérico, podría inferirse como StringType o fallar la inferencia.
```

2.  **Definición explícita de esquema (`StructType` y `StructField`):**

Es la forma más robusta y recomendada para entornos de producción. Permite controlar con precisión los tipos de datos y la nulabilidad, evitando problemas de inferencia y mejorando el rendimiento.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

spark = SparkSession.builder.appName("ExplicitSchema").getOrCreate()

# Definir un esquema explícito
custom_schema = StructType([
    StructField("employee_name", StringType(), True),
    StructField("employee_id", IntegerType(), False), # Not nullable
    StructField("salary", DoubleType(), True),
    StructField("is_active", BooleanType(), True)
])

data = [("Alice", 1, 50000.0, True), ("Bob", 2, 60000.50, False), ("Charlie", 3, 75000.0, True)]
df_explicit = spark.createDataFrame(data, schema=custom_schema)
df_explicit.printSchema()
# Resultado:
# root
#  |-- employee_name: string (nullable = true)
#  |-- employee_id: integer (nullable = false)
#  |-- salary: double (nullable = true)
#  |-- is_active: boolean (nullable = true)

# Intentar insertar un valor nulo en una columna no nula causaría un error o comportamiento inesperado
# data_error = [("David", None, 80000.0, True)] # Esto generaría un error si intentas crear el DF
# df_error = spark.createDataFrame(data_error, schema=custom_schema)
```

3.  **Acceder y manipular el esquema (`df.schema`):**

El esquema de un DataFrame es accesible a través del atributo `.schema`, que devuelve un objeto `StructType`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SchemaAccess").getOrCreate()

data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "id"])

# Acceder al esquema
print(df.schema)
# Resultado: StructType([StructField('name', StringType(), True), StructField('id', LongType(), True)])

# Iterar sobre los campos del esquema
for field in df.schema:
    print(f"Nombre de columna: {field.name}, Tipo: {field.dataType}, Nulable: {field.nullable}")
# Resultado:
# Nombre de columna: name, Tipo: StringType, Nulable: True
# Nombre de columna: id, Tipo: LongType, Nulable: True
```

##### Tipos de Datos Complejos

Spark permite manejar estructuras de datos más allá de los tipos atómicos, lo que es fundamental para trabajar con datos semi-estructurados y anidados como JSON.

1.  **`StructType` (Estructuras Anidadas/Registros):**

Representa una estructura similar a un objeto o un registro, donde cada campo tiene un nombre y un tipo de datos. Permite modelar objetos complejos dentro de una columna.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("ComplexTypes").getOrCreate()

# Definir un esquema con un StructType anidado
address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True)
])

person_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", address_schema, True) # Columna de tipo StructType
])

data = [
    ("Alice", 25, ("123 Main St", "NY", "10001")),
    ("Bob", 30, ("456 Oak Ave", "LA", "90001"))
]

df = spark.createDataFrame(data, schema=person_schema)
df.printSchema()
# Resultado:
# root
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- address: struct (nullable = true)
#  |    |-- street: string (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- zip: string (nullable = true)

df.show(truncate=False)
# Resultado:
# +-----+---+-------------------------+
# |name |age|address                  |
# +-----+---+-------------------------+
# |Alice|25 |{123 Main St, NY, 10001} |
# |Bob  |30 |{456 Oak Ave, LA, 90001} |
# +-----+---+-------------------------+

# Acceder a campos anidados
df.select("name", "address.city").show()
# Resultado:
# +-----+----+
# |name |city|
# +-----+----+
# |Alice|NY  |
# |Bob  |LA  |
# +-----+----+
```

2.  **`ArrayType` (Arrays/Listas):**

Representa una colección de elementos del mismo tipo. Útil para modelar listas o arreglos de datos dentro de una columna.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName("ComplexTypesArray").getOrCreate()

# Definir un esquema con un ArrayType
course_schema = StructType([
    StructField("student_name", StringType(), True),
    StructField("grades", ArrayType(IntegerType()), True) # Columna de tipo ArrayType
])

data = [
    ("Alice", [90, 85, 92]),
    ("Bob", [78, 80]),
    ("Charlie", [])
]

df = spark.createDataFrame(data, schema=course_schema)
df.printSchema()
# Resultado:
# root
#  |-- student_name: string (nullable = true)
#  |-- grades: array (nullable = true)
#  |    |-- element: integer (containsNull = true)

df.show(truncate=False)
# Resultado:
# +------------+----------+
# |student_name|grades    |
# +------------+----------+
# |Alice       |[90, 85, 92]|
# |Bob         |[78, 80]  |
# |Charlie     |[]        |
# +------------+----------+

# Acceder a elementos de array (requiere funciones de Spark)
from pyspark.sql.functions import size, array_contains
df.select("student_name", size("grades").alias("num_grades")).show()
# Resultado:
# +------------+----------+
# |student_name|num_grades|
# +------------+----------+
# |       Alice|         3|
# |         Bob|         2|
# |     Charlie|         0|
# +------------+----------+

df.filter(array_contains("grades", 90)).show()
# Resultado:
# +------------+----------+
# |student_name|    grades|
# +------------+----------+
# |       Alice|[90, 85, 92]|
# +------------+----------+
```

3.  **`MapType` (Mapas/Diccionarios):**

Representa una colección de pares clave-valor. Útil para datos que se asemejan a diccionarios o JSON con claves dinámicas.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType

spark = SparkSession.builder.appName("ComplexTypesMap").getOrCreate()

# Definir un esquema con un MapType
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True) # Columna de tipo MapType
])

data = [
    ("P101", {"color": "red", "size": "M", "material": "cotton"}),
    ("P102", {"color": "blue", "size": "L"}),
    ("P103", {})
]

df = spark.createDataFrame(data, schema=product_schema)
df.printSchema()
# Resultado:
# root
#  |-- product_id: string (nullable = true)
#  |-- attributes: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (containsNull = true)

df.show(truncate=False)
# Resultado:
# +----------+-----------------------------------+
# |product_id|attributes                         |
# +----------+-----------------------------------+
# |P101      |{color -> red, size -> M, material -> cotton}|
# |P102      |{color -> blue, size -> L}         |
# |P103      |{}                                 |
# +----------+-----------------------------------+

# Acceder a elementos de mapa (se usa con `getItem` o notación de corchetes)
from pyspark.sql.functions import col
df.select("product_id", col("attributes")["color"].alias("product_color")).show()
# Resultado:
# +----------+-------------+
# |product_id|product_color|
# +----------+-------------+
# |      P101|          red|
# |      P102|         blue|
# |      P103|         null|
# +----------+-------------+
```

### 2.1.3 Lectura y escritura en formatos populares (Parquet, Avro, ORC, CSV, JSON)

Spark es versátil en la lectura y escritura de datos, soportando una amplia gama de formatos de archivo. La elección del formato adecuado es crucial para el rendimiento y la eficiencia del almacenamiento en entornos de Big Data. Los formatos columnares como Parquet y ORC son altamente recomendados para el análisis debido a su eficiencia en la lectura y compresión.

##### Lectura de Datos

La lectura de datos es la base para cualquier análisis. Spark proporciona un API `spark.read` muy flexible para cargar datos desde diversas fuentes.

1.  **Lectura de archivos CSV:**

Ideal para datos tabulares simples. Es importante configurar `header=True` si el archivo tiene encabezados y `inferSchema=True` para que Spark intente adivinar los tipos de datos.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteCSV").getOrCreate()

# Crear un archivo CSV de ejemplo
data_csv = """id,name,age,city
1,Alice,25,New York
2,Bob,30,Los Angeles
3,Charlie,22,Chicago"""
with open("users.csv", "w") as f:
    f.write(data_csv)

# Leer un archivo CSV
df_csv = spark.read.csv("users.csv", header=True, inferSchema=True)
df_csv.printSchema()
df_csv.show()
# Resultado:
# root
#  |-- id: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- age: integer (nullable = true)
#  |-- city: string (nullable = true)
# +---+-------+---+----------+
# | id|   name|age|      city|
# +---+-------+---+----------+
# |  1|  Alice| 25|  New York|
# |  2|    Bob| 30|Los Angeles|
# |  3|Charlie| 22|   Chicago|
# +---+-------+---+----------+
```

2.  **Lectura de archivos JSON:**

Útil para datos semi-estructurados. Spark puede inferir el esquema automáticamente.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteJSON").getOrCreate()

# Crear un archivo JSON de ejemplo
data_json = """
{"id": 1, "name": "Alice", "hobbies": ["reading", "hiking"]}
{"id": 2, "name": "Bob", "hobbies": ["gaming"]}
{"id": 3, "name": "Charlie", "hobbies": []}
"""
with open("users.json", "w") as f:
    f.write(data_json)

# Leer un archivo JSON
df_json = spark.read.json("users.json")
df_json.printSchema()
df_json.show(truncate=False)
# Resultado:
# root
#  |-- hobbies: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
# +--------------------+---+-------+
# |hobbies             |id |name   |
# +--------------------+---+-------+
# |[reading, hiking]   |1  |Alice  |
# |[gaming]            |2  |Bob    |
# |[]                  |3  |Charlie|
# +--------------------+---+-------+
```

3.  **Lectura de archivos Parquet:**

Formato columnar altamente eficiente para Big Data. Spark lo usa como formato por defecto y es altamente optimizado para el rendimiento.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteParquet").getOrCreate()

# Primero, creamos un DataFrame y lo guardamos como Parquet
data = [("Alice", 25, "NY"), ("Bob", 30, "LA")]
df = spark.createDataFrame(data, ["name", "age", "city"])
df.write.mode("overwrite").parquet("users.parquet")

# Leer un archivo Parquet
df_parquet = spark.read.parquet("users.parquet")
df_parquet.printSchema()
df_parquet.show()
# Resultado:
# root
#  |-- name: string (nullable = true)
#  |-- age: long (nullable = true)
#  |-- city: string (nullable = true)
# +-----+---+----+
# | name|age|city|
# +-----+---+----+
# |Alice| 25|  NY|
# |  Bob| 30|  LA|
# +-----+---+----+
```

4.  **Lectura de archivos ORC:**

Otro formato columnar optimizado para Big Data, desarrollado por Apache Hive. Ofrece compresión y rendimiento similares a Parquet.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteORC").getOrCreate()

# Primero, creamos un DataFrame y lo guardamos como ORC
data = [("ProductA", 100, "Electronics"), ("ProductB", 50, "Books")]
df = spark.createDataFrame(data, ["product_name", "price", "category"])
df.write.mode("overwrite").orc("products.orc")

# Leer un archivo ORC
df_orc = spark.read.orc("products.orc")
df_orc.printSchema()
df_orc.show()
# Resultado:
# root
#  |-- product_name: string (nullable = true)
#  |-- price: long (nullable = true)
#  |-- category: string (nullable = true)
# +------------+-----+-----------+
# |product_name|price|   category|
# +------------+-----+-----------+
# |    ProductA|  100|Electronics|
# |    ProductB|   50|      Books|
# +------------+-----+-----------+
```

5.  **Lectura de archivos Avro:**

Formato de serialización de datos basado en esquema. Requiere el paquete `spark-avro`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadWriteAvro") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()

# Primero, creamos un DataFrame y lo guardamos como Avro
data = [("Event1", "typeA", 1678886400), ("Event2", "typeB", 1678886460)]
df = spark.createDataFrame(data, ["event_id", "event_type", "timestamp"])
df.write.mode("overwrite").format("avro").save("events.avro")

# Leer un archivo Avro
df_avro = spark.read.format("avro").load("events.avro")
df_avro.printSchema()
df_avro.show()
# Resultado:
# root
#  |-- event_id: string (nullable = true)
#  |-- event_type: string (nullable = true)
#  |-- timestamp: long (nullable = true)
# +--------+----------+----------+
# |event_id|event_type| timestamp|
# +--------+----------+----------+
# |  Event1|     typeA|1678886400|
# |  Event2|     typeB|1678886460|
# +--------+----------+----------+
```

##### Escritura de Datos

La escritura de DataFrames a diferentes formatos es tan importante como su lectura, ya que permite persistir los resultados de las transformaciones y compartirlos con otras aplicaciones o sistemas.

1.  **Modos de escritura (`mode`):**

Cuando se escribe un DataFrame, es fundamental especificar el modo de escritura para evitar pérdidas de datos o errores.

* `overwrite`: Sobrescribe los datos existentes en la ubicación de destino.
* `append`: Añade los datos al final de los datos existentes.
* `ignore`: Si los datos ya existen, la operación de escritura no hace nada.
* `error` (o `errorIfExists`): Lanza un error si los datos ya existen (modo por defecto).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WriteModes").getOrCreate()

data = [("A", 1), ("B", 2)]
df = spark.createDataFrame(data, ["col1", "col2"])

# Escribir en modo 'overwrite'
df.write.mode("overwrite").parquet("output_data.parquet")

# Escribir en modo 'append'
data_new = [("C", 3)]
df_new = spark.createDataFrame(data_new, ["col1", "col2"])
df_new.write.mode("append").parquet("output_data.parquet")

# Verificar el contenido
spark.read.parquet("output_data.parquet").show()
# Resultado:
# +----+----+
# |col1|col2|
# +----+----+
# |   A|   1|
# |   B|   2|
# |   C|   3|
# +----+----+

# Escribir en modo 'ignore' (si el archivo ya existe, no hará nada)
df.write.mode("ignore").csv("output_csv", header=True)
```

2.  **Particionamiento de salida (`partitionBy`):**

Permite organizar los datos en el sistema de archivos subyacente (HDFS, S3, ADLS) en directorios basados en el valor de una o más columnas. Esto mejora el rendimiento de lectura para consultas que filtran por las columnas de partición.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionBy").getOrCreate()

data = [("Sales", 2023, 100), ("Sales", 2024, 120), ("Marketing", 2023, 80), ("Marketing", 2024, 90)]
df = spark.createDataFrame(data, ["department", "year", "revenue"])

# Escribir con particionamiento por 'department' y 'year'
df.write.mode("overwrite").partitionBy("department", "year").parquet("department_yearly_revenue.parquet")

# Esto creará una estructura de directorios como:
# department_yearly_revenue.parquet/department=Sales/year=2023/part-....parquet
# department_yearly_revenue.parquet/department=Sales/year=2024/part-....parquet
# ...
```

3.  **Manejo de directorios de salida:**

Spark crea un directorio para cada operación de escritura. Dentro de este directorio, se encuentran los archivos de datos (partes) y un archivo `_SUCCESS` si la operación fue exitosa.

```python
# Después de ejecutar una escritura, puedes explorar la estructura de directorios.
# Por ejemplo, para el caso de Parquet sin particionamiento:
# ls -R users.parquet/
# Resultado (ejemplo):
# users.parquet/:
# _SUCCESS/
# part-00000-....snappy.parquet/
```

## Tarea

**Ejercicios con PySpark**:

1.  Crea un DataFrame a partir de la siguiente lista de tuplas: `[("Juan", "Perez", 30, "Ingeniero"), ("Maria", "Lopez", 25, "Doctora"), ("Carlos", "Gomez", 35, "Abogado")]`. Define el esquema explícitamente con las columnas `nombre`, `apellido`, `edad` (entero) y `profesion`. Luego, muestra el esquema y las primeras filas del DataFrame.

2.  Dado el DataFrame del ejercicio 1, selecciona únicamente las columnas `nombre` y `profesion`. Además, renombra la columna `nombre` a `primer_nombre` en el DataFrame resultante.

3.  Al DataFrame original del ejercicio 1, añade una nueva columna llamada `salario_base` con un valor fijo de `50000`. Luego, crea otra columna `salario_ajustado` que sea `salario_base` más `edad * 100`.

4.  Filtra el DataFrame resultante del ejercicio 3 para mostrar solo las personas cuya `edad` sea mayor a `28` Y su `profesion` sea `Ingeniero` o `Abogado`.

5.  Utilizando el DataFrame original del ejercicio 1, calcula el promedio de `edad` y la cantidad total de personas.

6.  Crea un DataFrame de empleados que incluya una columna `contacto` de tipo `StructType` con `email` y `telefono` como subcampos. Los datos de ejemplo podrían ser: `[("Alice", {"email": "alice@example.com", "telefono": "123-456-7890"})]`. Muestra el esquema y accede al `email` de Alice.

7.  Crea un DataFrame de estudiantes con una columna `cursos_inscritos` de tipo `ArrayType(StringType())`. Ejemplo de datos: `[("Bob", ["Matemáticas", "Física"]), ("Eve", ["Química"])]`. Muestra el esquema y filtra los estudiantes que estén inscritos en `Matemáticas`.

8.  Crea un archivo CSV llamado `productos.csv` con los siguientes datos (incluye encabezado):

```csv
producto_id,nombre,precio,cantidad
1,Laptop,1200.50,10
2,Mouse,25.00,50
3,Teclado,75.99,30
```

Lee este archivo en un DataFrame, infiriendo el esquema y mostrando el esquema y el contenido.

9.  Crea un DataFrame con columnas `region`, `mes` y `ventas`. Los datos de ejemplo: `[("Norte", "Enero", 1000), ("Sur", "Enero", 800), ("Norte", "Febrero", 1100), ("Sur", "Febrero", 900)]`. Guarda este DataFrame como archivos Parquet, particionando por `region` y `mes`. Luego, lee solo las ventas de la región `Norte` en `Enero` para verificar la partición.

10. Crea un archivo JSON llamado `config.json` con los siguientes datos (cada objeto en una línea):

```json
{"id": 1, "settings": {"theme": "dark", "notifications": true}}
{"id": 2, "settings": {"theme": "light", "notifications": false}}
```

Lee este archivo en un DataFrame y muestra el `theme` para cada ID.

**Ejercicios con SparkSQL**:

1.  Crea el mismo DataFrame del Ejercicio 1 de PySpark (empleados). Registra este DataFrame como una vista temporal llamada `empleados_temp`. Luego, ejecuta una consulta SQL para seleccionar todos los empleados.

2.  Usando la vista `empleados_temp`, escribe una consulta SparkSQL para seleccionar `nombre`, `apellido` y `profesion` de los empleados con `edad` menor a `30`.

3.  Sobre la vista `empleados_temp`, realiza una consulta SQL que seleccione `nombre` como `primer_nombre` y `profesion` como `ocupacion`.

4.  Utilizando `empleados_temp`, añade una columna calculada llamada `edad_futura` que sea la `edad` actual más `5`.

5.  Crea una vista temporal a partir de un DataFrame de `ventas` con columnas `producto`, `region` y `cantidad`. Datos de ejemplo: `[("Laptop", "Norte", 5), ("Mouse", "Norte", 10), ("Laptop", "Sur", 3)]`. Calcula la `SUM` de `cantidad` por `producto` usando SparkSQL.

6.  Partiendo del DataFrame de empleados con `contacto` (email y telefono) del Ejercicio 6 de PySpark, crea una vista temporal. Luego, usa SparkSQL para seleccionar el `nombre` del empleado y su `contacto.email`.

7.  Utilizando el DataFrame de estudiantes con `cursos_inscritos` del Ejercicio 7 de PySpark, crea una vista temporal. Escribe una consulta SparkSQL para seleccionar los estudiantes que tienen `Matemáticas` en su lista de `cursos_inscritos` (puedes necesitar una función SQL de Spark para arrays).

8.  Crea el archivo `productos.csv` del Ejercicio 8 de PySpark. Luego, usando `spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("productos.csv")`, crea un DataFrame y regístralo como vista temporal `productos_temp`. Finalmente, selecciona todos los productos con un `precio` mayor a `50`.

9.  Guarda un DataFrame (por ejemplo, el de ventas del Ejercicio 9 de PySpark) como archivos Parquet en una ubicación específica (ej: `"data/ventas_particionadas"`). Luego, crea una tabla externa de SparkSQL apuntando a esa ubicación (`CREATE TABLE ... USING PARQUET LOCATION ...`). Finalmente, consulta las ventas de una `region` específica directamente desde la tabla SQL.

10. Usando el archivo `config.json` del Ejercicio 10 de PySpark, lee el JSON y crea una vista temporal `config_temp`. Escribe una consulta SparkSQL para extraer el valor del `theme` de la columna `settings` para cada `id` (esto requerirá desanidación o funciones JSON de SparkSQL).

