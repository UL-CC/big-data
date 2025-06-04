# 2. PySpark y SparkSQL

## Tema 2.2 Manipulación y Transformación de Datos

**Objetivo**:

Al finalizar esta unidad, el estudiante será capaz de aplicar técnicas avanzadas de manipulación y transformación sobre DataFrames de Apache Spark, utilizando un amplio rango de funciones integradas, creando funciones personalizadas (UDFs) para lógica específica, y comprendiendo cómo el particionamiento y el paralelismo influyen en el procesamiento eficiente de grandes volúmenes de datos distribuidos.

**Introducción**:

La capacidad de transformar datos brutos en información valiosa es el corazón del análisis de Big Data. En Apache Spark, los DataFrames no solo ofrecen una interfaz intuitiva para trabajar con datos estructurados y semi-estructurados, sino que también proporcionan un conjunto robusto de operaciones y funciones para la manipulación y limpieza de datos a escala. Desde simples selecciones y filtrados hasta complejas agregaciones y uniones, Spark permite a los usuarios moldear sus datos para satisfacer las necesidades de análisis, modelado o visualización, todo ello aprovechando su arquitectura distribuida subyacente.

**Desarrollo**:

Este tema profundiza en las capacidades de manipulación de DataFrames de Spark. Retomaremos y ampliaremos las operaciones fundamentales, exploraremos el vasto catálogo de funciones integradas de Spark SQL y aprenderemos a extender esta funcionalidad creando nuestras propias funciones definidas por el usuario (UDFs). Además, abordaremos conceptos cruciales como el particionamiento y el paralelismo, fundamentales para optimizar el rendimiento y escalar el procesamiento de datos distribuidos de manera efectiva. Comprender estos conceptos es clave para escribir código Spark eficiente y robusto en escenarios de Big Data.

### 2.2.1 Operaciones con DataFrames (selección, filtrado, agregaciones)

Si bien ya se introdujeron las operaciones básicas en el tema 2.1, esta sección se enfoca en profundizar y mostrar ejemplos más avanzados y combinaciones de estas operaciones, destacando su poder para la limpieza y preparación de datos. La flexibilidad del API de DataFrames permite encadenar múltiples transformaciones, construyendo flujos de trabajo de datos complejos de manera legible y eficiente.

##### Selección Avanzada de Columnas

Más allá de simplemente elegir columnas por nombre, Spark ofrece potentes capacidades para manipular columnas existentes o crear nuevas basadas en expresiones complejas.

1.  **Seleccionar y Renombrar Múltiples Columnas dinámicamente:**

Es común necesitar seleccionar un subconjunto de columnas y, al mismo tiempo, renombrarlas. Esto se puede hacer de forma programática utilizando listas de columnas y aplicando aliases.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AdvancedSelection").getOrCreate()

data = [("Alice", 25, "NY", "USA"), ("Bob", 30, "LA", "USA"), ("Charlie", 22, "CHI", "USA")]
df = spark.createDataFrame(data, ["name", "age", "city", "country"])

# Seleccionar y renombrar múltiples columnas
selected_cols = [col("name").alias("full_name"), col("age"), col("city").alias("location")]
df.select(*selected_cols).show()
# Resultado:
# +---------+---+--------+
# |full_name|age|location|
# +---------+---+--------+
# |    Alice| 25|      NY|
# |      Bob| 30|      LA|
# |  Charlie| 22|     CHI|
# +---------+---+--------+
```

2.  **Uso de expresiones SQL en `select`:**

Spark permite incrustar expresiones SQL directamente dentro de la función `select` para mayor flexibilidad, especialmente cuando se trabaja con funciones complejas o lógicas condicionales.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("SqlExpressions").getOrCreate()

data = [("Alice", 25, 50000), ("Bob", 30, 60000), ("Charlie", 22, 45000)]
df = spark.createDataFrame(data, ["name", "age", "salary"])

# Calcular un bono basado en el salario usando una expresión SQL
df.select("name", "salary", expr("salary * 0.10 AS bonus")).show()
# Resultado:
# +-------+------+-------+
# |   name|salary|  bonus|
# +-------+------+-------+
# |  Alice| 50000| 5000.0|
# |    Bob| 60000| 6000.0|
# |Charlie| 45000| 4500.0|
# +-------+------+-------+
```

3.  **Eliminar columnas (`drop`):**

Es una operación común para limpiar DataFrames, eliminando columnas que no son relevantes para el análisis posterior.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DropColumn").getOrCreate()

data = [("Alice", 25, "NY", "USA"), ("Bob", 30, "LA", "USA")]
df = spark.createDataFrame(data, ["name", "age", "city", "country"])

# Eliminar una sola columna
df.drop("country").show()
# Resultado:
# +-----+---+----+
# | name|age|city|
# +-----+---+----+
# |Alice| 25|  NY|
# |  Bob| 30|  LA|
# +-----+---+----+

# Eliminar múltiples columnas
df.drop("age", "city").show()
# Resultado:
# +-----+-------+
# | name|country|
# +-----+-------+
# |Alice|    USA|
# |  Bob|    USA|
# +-----+-------+
```

##### Filtrado Avanzado de Filas

Las condiciones de filtrado pueden ser muy complejas, combinando múltiples operadores lógicos y funciones.

1.  **Combinar múltiples condiciones con `&` (AND), `|` (OR), `~` (NOT):**

Permite construir filtros sofisticados para aislar subconjuntos de datos específicos.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AdvancedFiltering").getOrCreate()

data = [("Laptop", 1200, "Electronics"), ("Mouse", 25, "Electronics"),
        ("Book", 15, "Books"), ("Monitor", 300, "Electronics"), ("Pen", 2, "Office")]
df = spark.createDataFrame(data, ["product", "price", "category"])

# Filtrar productos de "Electronics" con precio > 100 O productos de "Books"
df.filter( (col("category") == "Electronics") & (col("price") > 100) | (col("category") == "Books") ).show()
# Resultado:
# +--------+-----+-----------+
# | product|price|   category|
# +--------+-----+-----------+
# |  Laptop| 1200|Electronics|
# |    Book|   15|      Books|
# | Monitor|  300|Electronics|
# +--------+-----+-----------+
```

2.  **Uso de `isin` para filtrar por múltiples valores en una columna:**

Una forma concisa de filtrar filas donde una columna toma uno de varios valores posibles.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FilteringIsIn").getOrCreate()

data = [("Alice", "NY"), ("Bob", "LA"), ("Charlie", "CHI"), ("David", "NY")]
df = spark.createDataFrame(data, ["name", "city"])

# Filtrar por ciudades específicas
df.filter(col("city").isin("NY", "LA")).show()
# Resultado:
# +-----+----+
# | name|city|
# +-----+----+
# |Alice|  NY|
# |  Bob|  LA|
# |David|  NY|
# +-----+----+
```

3.  **Manejo de valores nulos en filtros (`isNull`, `isNotNull`, `na.drop`):**

Es crucial manejar los valores nulos al filtrar para evitar resultados inesperados o errores.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("HandlingNullsFiltering").getOrCreate()

data = [("Alice", 25), ("Bob", None), ("Charlie", 22), ("David", None)]
df = spark.createDataFrame(data, ["name", "age"])

# Filtrar filas donde 'age' no es nulo
df.filter(col("age").isNotNull()).show()
# Resultado:
# +-------+---+
# |   name|age|
# +-------+---+
# |  Alice| 25|
# |Charlie| 22|
# +-------+---+

# Filtrar filas donde 'age' es nulo
df.filter(col("age").isNull()).show()
# Resultado:
# +-----+----+
# | name| age|
# +-----+----+
# |  Bob|null|
# |David|null|
# +-----+----+

# Eliminar filas con cualquier valor nulo
df.na.drop().show()
# Resultado:
# +-------+---+
# |   name|age|
# +-------+---+
# |  Alice| 25|
# |Charlie| 22|
# +-------+---+
```

##### Agregaciones Avanzadas y Agrupamiento

Las agregaciones son potentes para resumir datos. Spark permite agregaciones por múltiples columnas, con funciones de ventana y pivoteo.

1.  **Agregaciones sobre múltiples columnas con `agg`:**

Permite aplicar múltiples funciones de agregación a diferentes columnas en una sola operación de `groupBy`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count, col

spark = SparkSession.builder.appName("MultiAggregations").getOrCreate()

data = [("Dept1", "Alice", 1000, 5), ("Dept1", "Bob", 1200, 8),
        ("Dept2", "Charlie", 900, 3), ("Dept2", "David", 1500, 10)]
df = spark.createDataFrame(data, ["department", "name", "salary", "projects_completed"])

# Agregaciones múltiples por departamento
df.groupBy("department").agg(
    avg(col("salary")).alias("avg_salary"),
    sum(col("projects_completed")).alias("total_projects"),
    count(col("name")).alias("num_employees")
).show()
# Resultado:
# +----------+----------+--------------+-------------+
# |department|avg_salary|total_projects|num_employees|
# +----------+----------+--------------+-------------+
# |     Dept1|    1100.0|            13|            2|
# |     Dept2|    1200.0|            13|            2|
# +----------+----------+--------------+-------------+
```

2.  **Pivoteo de datos (`pivot`):**

Transforma filas en columnas, muy útil para análisis de series temporales o reportes.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("PivotOperation").getOrCreate()

data = [("Sales", "Jan", 100), ("Sales", "Feb", 120),
        ("Marketing", "Jan", 80), ("Marketing", "Feb", 90),
        ("Sales", "Mar", 150)]
df = spark.createDataFrame(data, ["department", "month", "revenue"])

# Pivoteo de ingresos por departamento y mes
df.groupBy("department").pivot("month", ["Jan", "Feb", "Mar"]).agg(sum("revenue")).show()
# Resultado:
# +----------+---+---+---+
# |department|Jan|Feb|Mar|
# +----------+---+---+---+
# |     Sales|100|120|150|
# | Marketing| 80| 90|null|
# +----------+---+---+---+
```

3.  **Uniones de DataFrames (`join`):**

Combina dos DataFrames en función de una o más claves comunes. Spark soporta varios tipos de uniones (inner, outer, left, right, anti, semi).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameJoins").getOrCreate()

# DataFrame de empleados
employees_data = [("Alice", 1, "Sales"), ("Bob", 2, "HR"), ("Charlie", 3, "IT")]
employees_df = spark.createDataFrame(employees_data, ["name", "emp_id", "dept_id"])

# DataFrame de departamentos
departments_data = [(1, "Sales", "NY"), (2, "HR", "LA"), (4, "Finance", "CHI")]
departments_df = spark.createDataFrame(departments_data, ["dept_id", "dept_name", "location"])

# Inner Join: solo filas que coinciden en ambas tablas
employees_df.join(departments_df, on="dept_id", how="inner").show()
# Resultado:
# +-------+-----+---------+---------+--------+
# |dept_id| name|   emp_id|dept_name|location|
# +-------+-----+---------+---------+--------+
# |      1|Alice|        1|    Sales|      NY|
# |      2|  Bob|        2|       HR|      LA|
# +-------+-----+---------+---------+--------+

# Left Outer Join: todas las filas de la izquierda, y las que coinciden de la derecha
employees_df.join(departments_df, on="dept_id", how="left_outer").show()
# Resultado:
# +-------+-------+------+---------+---------+--------+
# |dept_id|   name|emp_id|dept_name|location|
# +-------+-------+------+---------+---------+--------+
# |      1|  Alice|     1|    Sales|       NY|
# |      2|    Bob|     2|       HR|       LA|
# |      3|Charlie|     3|     null|     null|
# +-------+-------+------+---------+---------+--------+
```

### 2.2.2 Funciones integradas y definidas por el usuario (UDFs)

Spark proporciona una rica biblioteca de funciones integradas (`pyspark.sql.functions`) que cubren una amplia gama de transformaciones de datos. Sin embargo, cuando la lógica de negocio es muy específica y no está cubierta por las funciones existentes, las Funciones Definidas por el Usuario (UDFs) permiten extender la funcionalidad de Spark utilizando código Python.

##### Funciones Integradas de Spark SQL

Estas funciones son altamente optimizadas y deben ser la primera opción para cualquier transformación. Cubren operaciones numéricas, de cadena, de fecha y hora, y de manipulación de arrays y mapas.

1.  **Funciones de cadena (`substring`, `concat_ws`, `length`, `lower`, `upper`):**

Útiles para manipular texto en columnas.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, length, lower, upper

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [("john doe",), ("JANE SMITH",)]
df = spark.createDataFrame(data, ["full_name"])

# Extraer subcadena
df.withColumn("first_3_chars", substring(col("full_name"), 1, 3)).show()
# Resultado:
# +----------+-------------+
# | full_name|first_3_chars|
# +----------+-------------+
# |  john doe|          joh|
# |JANE SMITH|          JAN|
# +----------+-------------+

# Concatenar con separador (necesita más columnas para ser útil, ejemplo conceptual)
df.withColumn("formatted_name", concat_ws(", ", lower(col("full_name")))).show() # En este caso, solo convierte a minúsculas
# Resultado:
# +----------+--------------+
# | full_name|formatted_name|
# +----------+--------------+
# |  john doe|      john doe|
# |JANE SMITH|    jane smith|
# +----------+--------------+

# Obtener longitud y convertir a mayúsculas/minúsculas
df.select(col("full_name"), length(col("full_name")).alias("name_length"),
          lower(col("full_name")).alias("lower_name"),
          upper(col("full_name")).alias("upper_name")).show()
# Resultado:
# +----------+-----------+----------+----------+
# | full_name|name_length|lower_name|upper_name|
# +----------+-----------+----------+----------+
# |  john doe|          8|  john doe|  JOHN DOE|
# |JANE SMITH|         10|jane smith|JANE SMITH|
# +----------+-----------+----------+----------+
```

2.  **Funciones de fecha y hora (`current_date`, `datediff`, `year`, `month`, `to_date`, `to_timestamp`):**

Esenciales para el procesamiento de datos temporales.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, year, month, to_date, to_timestamp

spark = SparkSession.builder.appName("DateFunctions").getOrCreate()

data = [("2023-01-15",), ("2024-03-01",)]
df = spark.createDataFrame(data, ["event_date"])

# Convertir a tipo Date y obtener el año y mes
df.withColumn("event_date_parsed", to_date(col("event_date"))) \
  .withColumn("current_date", current_date()) \
  .withColumn("days_since_event", datediff(col("current_date"), col("event_date_parsed"))) \
  .withColumn("event_year", year(col("event_date_parsed"))) \
  .withColumn("event_month", month(col("event_date_parsed"))) \
  .show()
# Resultado (valores de días_since_event variarán con la fecha actual):
# +----------+-----------------+------------+----------------+----------+-----------+
# |event_date|event_date_parsed|current_date|days_since_event|event_year|event_month|
# +----------+-----------------+------------+----------------+----------+-----------+
# |2023-01-15|       2023-01-15|  2025-05-31|             867|      2023|          1|
# |2024-03-01|       2024-03-01|  2025-05-31|             457|      2024|          3|
# +----------+-----------------+------------+----------------+----------+-----------+

# Convertir a timestamp
df_ts = spark.createDataFrame([("2023-01-15 10:30:00",)], ["datetime_str"])
df_ts.withColumn("parsed_timestamp", to_timestamp(col("datetime_str"))).show()
# Resultado:
# +-------------------+--------------------+
# |       datetime_str|    parsed_timestamp|
# +-------------------+--------------------+
# |2023-01-15 10:30:00|2023-01-15 10:30:00|
# +-------------------+--------------------+
```

3.  **Funciones condicionales (`when`, `otherwise`):**

Permiten aplicar lógica condicional para crear nuevas columnas o modificar existentes.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("ConditionalFunctions").getOrCreate()

data = [("Alice", 25, "NY"), ("Bob", 35, "LA"), ("Charlie", 17, "CHI")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# Clasificar edad en categorías
df.withColumn("age_group",
              when(col("age") < 18, "Minor")
              .when(col("age") >= 18, "Adult")
              .otherwise("Unknown")
             ).show()
# Resultado:
# +-------+---+----+---------+
# |   name|age|city|age_group|
# +-------+---+----+---------+
# |  Alice| 25|  NY|    Adult|
# |    Bob| 35|  LA|    Adult|
# |Charlie| 17| CHI|    Minor|
# +-------+---+----+---------+
```

##### Funciones Definidas por el Usuario (UDFs)

Las UDFs permiten a los desarrolladores de Python extender la funcionalidad de Spark implementando lógica personalizada. Aunque potentes, pueden tener un impacto en el rendimiento debido a la serialización y deserialización de datos entre la JVM (donde Spark se ejecuta) y el proceso Python.

1.  **Creación de UDFs simples:**

Se definen como funciones Python normales y luego se registran en Spark usando `udf` de `pyspark.sql.functions`. Es crucial especificar el tipo de retorno.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("SimpleUDF").getOrCreate()

data = [("alice",), ("BOB",), ("charlie",)]
df = spark.createDataFrame(data, ["name"])

# Definir una función Python para capitalizar la primera letra
def capitalize_name(name):
    return name.capitalize() if name else None

# Registrar la UDF con el tipo de retorno
capitalize_udf = udf(capitalize_name, StringType())

# Aplicar la UDF al DataFrame
df.withColumn("capitalized_name", capitalize_udf(col("name"))).show()
# Resultado:
# +-------+----------------+
# |   name|capitalized_name|
# +-------+----------------+
# |  alice|           Alice|
# |    BOB|             Bob|
# |charlie|         Charlie|
# +-------+----------------+
```

2.  **UDFs con múltiples argumentos:**

Las UDFs pueden aceptar múltiples columnas como entrada.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("MultiArgUDF").getOrCreate()

data = [(1000, 0.10), (1200, 0.05), (800, 0.15)]
df = spark.createDataFrame(data, ["base_salary", "bonus_rate"])

# Función Python para calcular el salario total
def calculate_total_salary(base_salary, bonus_rate):
    if base_salary is None or bonus_rate is None:
        return None
    return base_salary * (1 + bonus_rate)

# Registrar la UDF
total_salary_udf = udf(calculate_total_salary, DoubleType())

# Aplicar la UDF
df.withColumn("total_salary", total_salary_udf(col("base_salary"), col("bonus_rate"))).show()
# Resultado:
# +-----------+----------+------------+
# |base_salary|bonus_rate|total_salary|
# +-----------+----------+------------+
# |       1000|      0.10|      1100.0|
# |       1200|      0.05|      1260.0|
# |        800|      0.15|       920.0|
# +-----------+----------+------------+
```

3.  **Consideraciones de rendimiento de UDFs (Vectorized UDFs con Pandas):**

Para mitigar el costo de serialización/deserialización, Spark 2.3+ introdujo las UDFs vectorizadas (anteriormente "Pandas UDFs"). Estas UDFs operan en `pandas.Series` o `pandas.DataFrame` en lugar de una fila a la vez, lo que reduce la sobrecarga y mejora significativamente el rendimiento para ciertas operaciones.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StringType

spark = SparkSession.builder.appName("PandasUDF").getOrCreate()

data = [(1, 10.0), (2, 20.0), (3, 30.0)]
df = spark.createDataFrame(data, ["id", "value"])

# Pandas UDF para escalar valores (Series a Series)
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def multiply_by_two(value: float) -> float:
    return value * 2

df.withColumn("value_doubled", multiply_by_two(col("value"))).show()
# Resultado:
# +---+-----+-------------+
# | id|value|value_doubled|
# +---+-----+-------------+
# |  1| 10.0|         20.0|
# |  2| 20.0|         40.0|
# |  3| 30.0|         60.0|
# +---+-----+-------------+

# Pandas UDF para agregación (Series a escalar)
@pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
def concat_strings(col_series):
    return "_".join(col_series.astype(str))

df_agg = spark.createDataFrame([("A", "x"), ("A", "y"), ("B", "z")], ["group", "value"])
df_agg.groupBy("group").agg(concat_strings(col("value")).alias("concatenated_values")).show()
# Resultado:
# +-----+-------------------+
# |group|concatenated_values|
# +-----+-------------------+
# |    A|                x_y|
# |    B|                  z|
# +-----+-------------------+
```

### 2.2.3 Particionamiento y paralelismo

El particionamiento es un concepto fundamental en Spark que define cómo se distribuyen los datos en el clúster. Un particionamiento adecuado es clave para optimizar el rendimiento de las operaciones, especialmente las que involucran shuffles (intercambio de datos entre nodos). El paralelismo se refiere a la cantidad de tareas que Spark puede ejecutar simultáneamente.

##### Conceptos de Particionamiento

Los datos en Spark se dividen en "particiones" lógicas, cada una de las cuales es procesada por una tarea. La forma en que los datos se particionan afecta el rendimiento y la eficiencia de la computación.

1.  **¿Qué es una partición en Spark?:**

Una partición es una división lógica de los datos de un RDD o DataFrame. Cada partición se puede almacenar en un nodo diferente del clúster y se procesa de forma independiente y paralela. Más particiones no siempre es mejor; el número óptimo depende del tamaño de los datos, el número de núcleos del clúster y la naturaleza de las operaciones.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionsConcept").getOrCreate()

data = [(i,) for i in range(100)]
df = spark.createDataFrame(data, ["value"])

# Obtener el número de particiones actual (por defecto Spark usa el número de núcleos disponibles o spark.sql.shuffle.partitions)
print(f"Número inicial de particiones: {df.rdd.getNumPartitions()}")

# Podemos re-particionar un DataFrame (esto implica un shuffle)
df_repartitioned = df.repartition(10)
print(f"Número de particiones después de repartition: {df_repartitioned.rdd.getNumPartitions()}")

# Una partición es como un bloque de trabajo para un núcleo de CPU.
# Si tenemos 100 particiones y 20 núcleos, cada núcleo procesará 5 particiones en paralelo (idealmente).
```

2.  **Visualización del número de particiones (`df.rdd.getNumPartitions()`):**

Es importante saber cuántas particiones tiene un DataFrame para entender cómo se distribuirá el trabajo.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckPartitions").getOrCreate()

# Leer un archivo de ejemplo para ver sus particiones
# Si no tienes un archivo grande, crea uno pequeño y léelo
data = [(i, f"Name_{i}") for i in range(1000)]
df_large = spark.createDataFrame(data, ["id", "name"])
df_large.write.mode("overwrite").parquet("large_data.parquet")

# Leer el DataFrame
df_read = spark.read.parquet("large_data.parquet")

# Ver el número de particiones
print(f"Número de particiones del DataFrame leído: {df_read.rdd.getNumPartitions()}")
# Generalmente, Spark intenta que el número de particiones sea cercano al tamaño de bloque del HDFS (128MB por defecto)
# o al número de núcleos en el cluster.
```

3.  **Impacto del particionamiento en el rendimiento (Shuffle):**

Las operaciones que requieren agrupar o unir datos (como `groupBy`, `join`, `orderBy`) a menudo implican un "shuffle", donde los datos se mueven entre los nodos del clúster. Un número incorrecto de particiones puede llevar a un shuffle ineficiente, causando cuellos de botella. Demasiadas particiones pequeñas pueden generar mucha sobrecarga, mientras que muy pocas pueden limitar el paralelismo.

* **Skewed data:** Si los datos están muy desequilibrados en las particiones (algunas particiones tienen muchos más datos que otras), esto puede causar cuellos de botella donde una o pocas tareas tardan mucho más en completarse.
* **Small files problem:** Si hay muchas particiones muy pequeñas, la sobrecarga de gestionar cada una puede ser mayor que el tiempo de procesamiento real.

##### Control del Particionamiento y Paralelismo

Spark ofrece mecanismos para controlar cómo se particionan los datos y el nivel de paralelismo.

1.  **`repartition()` y `coalesce()`:**

`repartition()` crea un nuevo RDD/DataFrame con un número especificado de particiones, distribuyendo los datos uniformemente. Esto siempre implica un shuffle completo.
`coalesce()` reduce el número de particiones sin shuffle si es posible (solo reduce el número de particiones en el mismo nodo), o con un shuffle mínimo si se requiere. Es más eficiente para reducir particiones.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RepartitionCoalesce").getOrCreate()

data = [(i,) for i in range(1000)]
df = spark.createDataFrame(data, ["value"])

print(f"Particiones iniciales: {df.rdd.getNumPartitions()}") # Varía según la configuración local

# Reparticionar a 4 particiones (siempre con shuffle)
df_repartitioned = df.repartition(4)
print(f"Particiones después de repartition(4): {df_repartitioned.rdd.getNumPartitions()}")

# Coalesce a 2 particiones (puede evitar shuffle si los datos ya están en menos de 2)
df_coalesced = df_repartitioned.coalesce(2)
print(f"Particiones después de coalesce(2): {df_coalesced.rdd.getNumPartitions()}")

# Coalesce a 1 partición (siempre implicará un shuffle si hay más de 1 partición)
df_single_partition = df.coalesce(1)
print(f"Particiones después de coalesce(1): {df_single_partition.rdd.getNumPartitions()}")
```

2.  **Configuración de `spark.sql.shuffle.partitions`:**

Este parámetro controla el número de particiones que Spark utiliza por defecto después de una operación de shuffle. Un valor bien ajustado puede mejorar drásticamente el rendimiento de las operaciones de agregación y unión.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Configurar el número de particiones de shuffle antes de crear la SparkSession
spark = SparkSession.builder \
    .appName("ShufflePartitions") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

data = [("A", 1), ("B", 2), ("A", 3), ("C", 4), ("B", 5)]
df = spark.createDataFrame(data, ["key", "value"])

# La operación groupBy implicará un shuffle, y el resultado tendrá 8 particiones.
df_agg = df.groupBy("key").agg(count("*")).repartition(1) # Repartition para mostrar en una sola salida
df_agg.show()
# Para verificar el número de particiones de la salida de la agregación (antes del repartition)
# print(df_agg.rdd.getNumPartitions())
```

3.  **Estrategias para optimizar particiones en Joins:**

* **Broadcast Join:** Cuando un DataFrame es pequeño (por defecto, menos de `spark.sql.autoBroadcastJoinThreshold`), Spark puede "broadcast" (transmitir) el DataFrame pequeño a todos los nodos del clúster, evitando el shuffle de la tabla grande. Es muy eficiente.
* **Hash Join:** Cuando la clave de unión está particionada de forma similar en ambos DataFrames, Spark puede realizar un Hash Join, que es eficiente ya que los datos con la misma clave ya están en las mismas particiones.
* **Sort-Merge Join:** El join por defecto si las tablas no se pueden transmitir y no están co-ubicadas. Implica ordenar y fusionar las particiones, lo que puede ser costoso.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast # Para forzar un broadcast join

spark = SparkSession.builder \
    .appName("JoinStrategies") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB") # Configurar el umbral de broadcast (por defecto 10MB)
    .getOrCreate()

# DataFrame pequeño (menos de 10MB de datos)
small_df = spark.createDataFrame([(1, "DeptA"), (2, "DeptB")], ["dept_id", "dept_name"])
# DataFrame grande
large_df = spark.createDataFrame([(101, "Alice", 1), (102, "Bob", 2), (103, "Charlie", 1)], ["emp_id", "emp_name", "dept_id"])

# Spark automáticamente intentará un Broadcast Join si small_df está por debajo del umbral
large_df.join(small_df, "dept_id", "inner").explain() # Ver el plan de ejecución para confirmar BroadcastHashJoin
# Resultado de explain() debería mostrar "*BroadcastHashJoin"

# Forzar un Broadcast Join (útil si Spark no lo infiere automáticamente o para depuración)
large_df.join(broadcast(small_df), "dept_id", "inner").explain()
```

### 2.2.4 Manejo de datos distribuidos

Trabajar con datos distribuidos implica más que solo particionar. Se trata de entender cómo Spark gestiona la memoria, el almacenamiento en caché y la persistencia para optimizar el acceso a los datos, y cómo maneja los "shuffles" que son costosos.

##### Persistencia y Almacenamiento en Caché

Para evitar recalcular DataFrames que se utilizan múltiples veces, Spark permite persistirlos en memoria o en disco.

1.  **`cache()` y `persist()`:**

`cache()` es un alias de `persist()` con el nivel de almacenamiento por defecto (`MEMORY_AND_DISK`). Almacena el DataFrame en la memoria del clúster para un acceso rápido en operaciones futuras.
`persist()` permite especificar diferentes niveles de almacenamiento (solo memoria, solo disco, memoria y disco, con o sin serialización, con o sin replicación).

```python
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("Persistence").getOrCreate()

data = [(i, f"Item_{i}") for i in range(100000)]
df = spark.createDataFrame(data, ["id", "description"])

# Persistir en memoria y disco (comportamiento por defecto de cache())
df.cache() # Equivalente a df.persist(StorageLevel.MEMORY_AND_DISK)

# La primera acción dispara la carga y el almacenamiento en caché
df.count()
print(f"DataFrame cached. Number of partitions: {df.rdd.getNumPartitions()}")

# Las acciones subsiguientes serán más rápidas
df.filter(df.id > 50000).show(5)

# Persistir solo en memoria (más rápido si los datos caben en memoria)
df_mem_only = df.persist(StorageLevel.MEMORY_ONLY)
df_mem_only.count()
print(f"DataFrame persisted in MEMORY_ONLY. Number of partitions: {df_mem_only.rdd.getNumPartitions()}")

# Despersistir (liberar los datos de caché)
df.unpersist()
df_mem_only.unpersist()
```

2.  **Niveles de almacenamiento (`StorageLevel`):**

Permiten un control granular sobre cómo se almacenan los datos persistidos, equilibrando velocidad y tolerancia a fallos.

* `MEMORY_ONLY`: Guarda el RDD deserializado como objetos Python en la JVM. Si no cabe, recalcula.
* `MEMORY_AND_DISK`: Guarda en memoria; si no cabe, se desborda a disco.
* `MEMORY_ONLY_SER`: Igual que MEMORY_ONLY, pero los datos están serializados (ahorra espacio, pero más lento de acceder).
* `MEMORY_AND_DISK_SER`: Igual que MEMORY_AND_DISK, pero serializado.
* `DISK_ONLY`: Solo guarda en disco.
* Versiones con `_2` al final (ej. `MEMORY_ONLY_2`): Replica la partición en dos nodos, para tolerancia a fallos.

```python
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("StorageLevels").getOrCreate()

data = [(i,) for i in range(1000000)]
df = spark.createDataFrame(data, ["value"])

# Persistir en disco para mayor confiabilidad en caso de poca memoria
df.persist(StorageLevel.DISK_ONLY)
df.count() # Dispara la persistencia
print("DataFrame persisted to DISK_ONLY.")

# Persistir con replicación (para tolerancia a fallos)
df.persist(StorageLevel.MEMORY_AND_DISK_2)
df.count() # Dispara la persistencia
print("DataFrame persisted to MEMORY_AND_DISK_2 (replicated).")

df.unpersist()
```

3.  **¿Cuándo usar `cache()`/`persist()`?:**

* Cuando un DataFrame se usa en múltiples acciones (ej. `count()`, `show()`, luego `filter()`, `join()`).
* Cuando un DataFrame es el resultado de transformaciones costosas (ej. `join` complejos, agregaciones pesadas).
* Antes de aplicar algoritmos iterativos (ej. Machine Learning), donde los datos se leen repetidamente.
* En puntos intermedios de un flujo de trabajo de ETL complejo que se reutilizan.

##### Comprensión y Optimización de Shuffles

Los shuffles son la operación más costosa en Spark porque implican la transferencia de datos a través de la red entre diferentes nodos. Minimizar o optimizar los shuffles es clave para el rendimiento.

1.  **Identificación de operaciones que causan Shuffle:**

Cualquier operación que requiera que Spark reorganice los datos a través del clúster, como:

* `groupBy()`
* `join()` (excepto Broadcast Joins)
* `orderBy()` y `sort()`
* `repartition()`
* Ventanas analíticas (ciertas operaciones)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("IdentifyShuffles").getOrCreate()

data = [("A", 1), ("B", 2), ("A", 3), ("C", 4)]
df = spark.createDataFrame(data, ["key", "value"])

# explain() muestra el plan de ejecución y ayuda a identificar shuffles
df.groupBy("key").agg(count("value")).explain()
# En el plan de ejecución, buscar etapas como "Exchange", "Sort", "HashPartitioning"
# Estas indican una operación de shuffle.
# Ejemplo de salida parcial de explain:
# == Physical Plan ==
# *(2) HashAggregate(keys=[key#123], functions=[count(value#124)])
# +- Exchange hashpartitioning(key#123, 200), [id=#12] <--- Esto es un shuffle
#    +- *(1) HashAggregate(keys=[key#123], functions=[partial_count(value#124)])
#       +- *(1) Project [key#123, value#124]
#          +- *(1) Scan ExistingRDD
```

2.  **Mitigación de Shuffles (ej. Broadcast Join, Co-locación de datos):**

* **Broadcast Join:** Como se mencionó, usar `broadcast()` para DataFrames pequeños evita el shuffle de la tabla grande.
* **Co-locación de datos:** Si los datos que se van a unir o agrupar ya están particionados de forma similar en el sistema de archivos (ej. Parquet particionado por la clave de unión), Spark puede evitar shuffles completos. Esto se logra mediante la escritura de datos particionados (`df.write.partitionBy(...)`).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShuffleMitigation").getOrCreate()

# Pequeño DataFrame de lookup
lookup_data = [(1, "RegionA"), (2, "RegionB")]
lookup_df = spark.createDataFrame(lookup_data, ["region_id", "region_name"])

# DataFrame grande
sales_data = [(101, 1, 100), (102, 2, 150), (103, 1, 200)]
sales_df = spark.createDataFrame(sales_data, ["sale_id", "region_id", "amount"])

# Forzar Broadcast Join para evitar shuffle de sales_df
from pyspark.sql.functions import broadcast
sales_df.join(broadcast(lookup_df), "region_id").explain()
# Deberías ver BroadcastHashJoin en el plan

# Ejemplo de co-locación con escritura particionada (requiere planificación previa)
# df_large.write.partitionBy("join_key").parquet("path/to/partitioned_data")
# df_other_large.write.partitionBy("join_key").parquet("path/to/other_partitioned_data")
# Luego, al unirlos, si Spark detecta que están co-ubicados, puede usar un SortMergeJoin más eficiente
```

3.  **Manejo de datos desequilibrados (Skewed Data):**

Cuando un valor de clave tiene significativamente más filas que otros, la partición correspondiente se convierte en un cuello de botella.

* **Salting:** Añadir un sufijo aleatorio a la clave desequilibrada en el DataFrame grande y replicar las filas de la clave desequilibrada en el DataFrame pequeño con los mismos sufijos.
* **Spark 3.x Adaptive Query Execution (AQE):** AQE puede detectar y manejar skew de forma automática durante la ejecución de los joins, dividiendo las particiones grandes en subparticiones más pequeñas. Habilitar `spark.sql.adaptive.enabled` a `true` (es por defecto en Spark 3.2+).

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, rand, concat

spark = SparkSession.builder \
    .appName("SkewedJoin") \
    .config("spark.sql.adaptive.enabled", "true") # Habilitar AQE
    .getOrCreate()

# Simular datos desequilibrados: 'HotKey' tiene muchos más registros
data_large = [(i, "NormalKey") for i in range(1000)] + \
             [(i, "HotKey") for i in range(9000)]
large_df = spark.createDataFrame(data_large, ["id", "join_key"])

data_small = [("NormalKey", "ValueA"), ("HotKey", "ValueB")]
small_df = spark.createDataFrame(data_small, ["join_key", "value"])

# Unión estándar (puede ser lenta debido a HotKey si AQE no está activo o no es suficiente)
result_df = large_df.join(small_df, "join_key", "inner")
result_df.explain() # Observar si AQE detecta el skew (si está habilitado)

# Ejemplo de Salting (manual):
# Añadir un "salt" aleatorio a las claves
num_salt_buckets = 10
salted_large_df = large_df.withColumn("salted_key",
                                      concat(col("join_key"), lit("_"), (rand() * num_salt_buckets).cast("int")))
salted_small_df = small_df.withColumn("salted_key",
                                      concat(col("join_key"), lit("_"), (lit(0) + rand() * num_salt_buckets).cast("int")))
# Si 'HotKey' tiene mucho skew, se replicaría 'HotKey' en small_df para cada sufijo de salt.
# Luego, se uniría por (join_key, salted_key)

# Unión con salting:
# result_salted = salted_large_df.join(salted_small_df, on="salted_key", how="inner")
# result_salted.show()
```

## Tarea

1.  Crea un DataFrame de `pedidos` con las columnas `order_id`, `customer_id`, `order_date` (formato 'YYYY-MM-DD'), y `amount` (valor numérico).
    Datos de ejemplo:
    `[("ORD001", "C001", "2023-01-10", 150.75), ("ORD002", "C002", "2023-01-15", 200.00), ("ORD003", "C001", "2023-02-01", 50.25), ("ORD004", "C003", "2023-02-05", 300.00)]`

    Realiza las siguientes transformaciones:
    * Añade una columna `order_year` que contenga solo el año de `order_date`.
    * Añade una columna `order_month` que contenga el número del mes de `order_date`.
    * Añade una columna `is_high_value` que sea `True` si `amount` es mayor o igual a `100`, de lo contrario `False`.
    * Muestra el DataFrame resultante.

2.  Usando el DataFrame de `pedidos` del ejercicio 1, calcula el `total_amount_spent` y el `num_orders` por `customer_id`. Muestra los resultados.

3.  Crea un DataFrame de `productos` con columnas `product_name` y `category`.
    Datos de ejemplo:
    `[("laptop Dell", "electronics"), ("teclado logitech", "electronics"), ("libro de cocina", "books"), ("Auriculares sony", "electronics")]`

    Realiza las siguientes transformaciones:
    * Normaliza la columna `product_name` a minúsculas.
    * Añade una columna `brand` que extraiga la primera palabra de `product_name`.
    * Añade una columna `product_type` basada en la `category`: si es "electronics", "Tech Gadget"; si es "books", "Reading Material"; de lo contrario "Other".
    * Muestra el DataFrame resultante.

4.  Define una UDF de PySpark llamada `classify_amount` que tome un `amount` numérico y devuelva una cadena: "Pequeño" si `amount < 50`, "Mediano" si `50 <= amount < 200`, y "Grande" si `amount >= 200`.
    Aplica esta UDF al DataFrame de `pedidos` del ejercicio 1 para crear una nueva columna `amount_category`. Muestra el DataFrame.

5.  Crea un segundo DataFrame de `clientes` con las columnas `customer_id` y `customer_name`.
    Datos de ejemplo:
    `[("C001", "Ana Garcia"), ("C002", "Pedro Ruiz"), ("C003", "Laura Sanz"), ("C004", "Diego Marin")]`

    Realiza un `INNER JOIN` entre el DataFrame de `pedidos` (ejercicio 1) y el DataFrame de `clientes` para mostrar los `order_id`, `customer_name` y `amount` de cada pedido.

6.  Realiza un `LEFT OUTER JOIN` de `clientes` (izquierda) con `pedidos` (derecha).
    * Muestra todos los clientes y sus pedidos (si los tienen).
    * Filtra el resultado para mostrar solo los clientes que *no* han realizado ningún pedido (es decir, donde `order_id` es nulo).

7.  Crea un DataFrame de `ventas_regionales` con columnas `region`, `product_category` y `sales_value`.
    Datos de ejemplo:
    `[("Norte", "Electronics", 1000), ("Norte", "Books", 500), ("Sur", "Electronics", 1200), ("Sur", "Books", 600), ("Centro", "Electronics", 800)]`

    Pivotea este DataFrame para mostrar `sales_value` por `region` (filas) y `product_category` (columnas).

8.  Crea un DataFrame con 1,000,000 de filas y dos columnas: `id` (entero secuencial) y `random_value` (número aleatorio).
    * Guarda este DataFrame en formato Parquet en un directorio temporal (`/tmp/large_data.parquet`).
    * Lee el DataFrame de nuevo y averigua su número de particiones.
    * Reparticiona el DataFrame a 10 particiones y persistélo en memoria (`MEMORY_AND_DISK`).
    * Realiza una operación de conteo y luego despersiste el DataFrame.

9.  Configura `spark.sql.shuffle.partitions` a un valor bajo (ej. 2) y luego a un valor más alto (ej. 20).
    Crea un DataFrame con una columna `category` que tenga 5 valores únicos y una columna `value`.

    Realiza una operación `groupBy` por `category` y suma `value`.
    * Usa `explain()` para observar los planes de ejecución y cómo el número de particiones de shuffle cambia.
    * (Opcional, para un entorno de clúster) Intenta medir el tiempo de ejecución en ambos casos para ver el impacto.

10. Crea un DataFrame `dim_customers` muy pequeño (ej. 10 filas, `customer_id`, `customer_name`).
    Crea un DataFrame `fact_transactions` muy grande (ej. 1,000,000 filas, `transaction_id`, `customer_id`, `amount`).

    Realiza un `INNER JOIN` entre `fact_transactions` y `dim_customers` en `customer_id`.
    * Usa `.explain()` para verificar si Spark ha utilizado automáticamente un `BroadcastHashJoin`.
    * Intenta forzar un `BroadcastHashJoin` si no se aplica automáticamente utilizando `broadcast(dim_customers)` en la unión, y verifica de nuevo el plan de ejecución.

