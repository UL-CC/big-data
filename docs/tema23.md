# 2. PySpark y SparkSQL

## Tema 2.3 Consultas y SQL en Spark

**Objetivo**:

Al finalizar este tema, el estudiante será capaz de comprender los fundamentos de SparkSQL como una interfaz declarativa para el procesamiento de datos distribuidos, ejecutar consultas SQL directamente sobre DataFrames y fuentes de datos, crear y gestionar vistas temporales, y aplicar sentencias SQL avanzadas para realizar análisis complejos y transformaciones eficientes de grandes volúmenes de datos.

**Introducción**:

Spark SQL es un módulo de Apache Spark para trabajar con datos estructurados. Proporciona una interfaz unificada para interactuar con datos utilizando consultas SQL estándar, lo que lo convierte en una herramienta invaluable para analistas de datos, ingenieros y científicos que ya están familiarizados con el lenguaje SQL. Permite a los usuarios consultar datos almacenados en DataFrames, así como en diversas fuentes de datos como Parquet, ORC, JSON, CSV, bases de datos JDBC y Hive, aprovechando al mismo tiempo el motor de ejecución optimizado de Spark para lograr un rendimiento excepcional en escala de Big Data.

**Desarrollo**:

Este tema explorará cómo Spark SQL integra la potencia de SQL con la escalabilidad de Spark. Iniciaremos con los fundamentos de Spark SQL, comprendiendo cómo los DataFrames pueden ser vistos y consultados como tablas relacionales. Luego, avanzaremos a la ejecución de consultas SQL básicas y la creación y gestión de vistas temporales, que son esenciales para estructurar flujos de trabajo SQL. Finalmente, nos sumergiremos en consultas SQL avanzadas, incluyendo uniones complejas, subconsultas, funciones de ventana y CTEs (Common Table Expressions), demostrando cómo Spark SQL puede manejar escenarios de análisis y transformación de datos altamente sofisticados.

### 2.3.1 Fundamentos de SparkSQL

Spark SQL permite la ejecución de consultas SQL sobre datos estructurados o semi-estructurados. Esencialmente, actúa como un motor SQL distribuido, permitiendo a los usuarios interactuar con DataFrames como si fueran tablas de bases de datos tradicionales, combinando la familiaridad de SQL con el poder de procesamiento de Spark.

##### La relación entre DataFrames y Tablas/Vistas en SparkSQL

La clave de SparkSQL reside en su capacidad para mapear DataFrames a estructuras relacionales como tablas o vistas. Esto permite que los datos en un DataFrame sean consultados usando sintaxis SQL estándar, lo que facilita la integración para usuarios con experiencia en bases de datos relacionales.

1.  **DataFrames como la base de SparkSQL:**

Todos los datos en SparkSQL se manejan como DataFrames. Cuando se ejecuta una consulta SQL, Spark la analiza, la optimiza y la ejecuta sobre los DataFrames subyacentes. El resultado de una consulta SQL es siempre un DataFrame.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLFundamentals").getOrCreate()

# Crear un DataFrame
data = [("Alice", 25, "NY"), ("Bob", 30, "LA"), ("Charlie", 22, "CHI")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# El DataFrame es la representación en memoria
df.show()
# +-------+---+----+
# |   name|age|city|
# +-------+---+----+
# |  Alice| 25|  NY|
# |    Bob| 30|  LA|
# |Charlie| 22| CHI|
# +-------+---+----+

# Sin una vista temporal, no podemos consultarlo directamente con spark.sql
try:
    spark.sql("SELECT * FROM my_table").show()
except Exception as e:
    print(f"Error esperado: {e}")
    # Resultado: Error esperado: Table or view not found: my_table;
```

2.  **`spark.sql()` para ejecutar consultas SQL:**

Esta es la función principal para ejecutar consultas SQL directamente en el contexto de Spark. Las consultas se escriben como cadenas de texto.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLQuery").getOrCreate()

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])

# Crear una vista temporal para poder consultar el DataFrame con SQL
df.createOrReplaceTempView("people")

# Ejecutar una consulta SQL
result_df = spark.sql("SELECT name, age FROM people WHERE age > 25")
result_df.show()
# Resultado:
# +----+---+
# |name|age|
# +----+---+
# | Bob| 30|
# +----+---+
```

3.  **Optimizador Catalyst y Generación de Código Tungsten:**

Spark SQL utiliza dos componentes clave para la optimización y ejecución de consultas:

* **Catalyst Optimizer:** Un optimizador de consulta basado en reglas y costos que genera planes de ejecución eficientes. Puede realizar optimizaciones como predicado pushdown, column pruning y reordenamiento de joins.
* **Tungsten:** Un motor de ejecución que genera código optimizado en tiempo de ejecución para DataFrames, mejorando la eficiencia de la CPU y el uso de memoria a través de técnicas como la eliminación de punteros y la gestión explícita de memoria.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CatalystTungsten").getOrCreate()

data = [("Laptop", 1200, "Electronics", 10), ("Mouse", 25, "Electronics", 50), ("Book", 15, "Books", 100)]
df = spark.createDataFrame(data, ["product_name", "price", "category", "stock"])
df.createOrReplaceTempView("products")

# Observar el plan de ejecución lógico y físico (Catalyst y Tungsten en acción)
# Explain muestra cómo Spark traduce la consulta SQL a una serie de operaciones optimizadas
spark.sql("SELECT product_name, price FROM products WHERE category = 'Electronics' AND price > 100").explain(extended=True)
# La salida mostrará el plan lógico (Parsed, Analyzed, Optimized) y el plan físico.
# Notar "PushedFilters" y "PushedProjections" que son optimizaciones de Catalyst.
# Los operadores físicos son implementaciones optimizadas por Tungsten.
```

### 2.3.2 Consultas básicas con SparkSQL

Una vez que un DataFrame se ha registrado como una vista temporal, se pueden realizar las operaciones de SQL más comunes sobre él. Estas operaciones son equivalentes a las transformaciones de DataFrame API, pero expresadas en un lenguaje declarativo.

##### SELECT y FROM

La base de cualquier consulta SQL, permitiendo especificar qué columnas se quieren recuperar y de qué fuente de datos.

1.  **Selección de todas las columnas (`SELECT *`):**

Recupera todas las columnas de una vista o tabla.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQL").getOrCreate()

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])
df.createOrReplaceTempView("people")

spark.sql("SELECT * FROM people").show()
# Resultado:
# +-----+---+
# | name|age|
# +-----+---+
# |Alice| 25|
# |  Bob| 30|
# +-----+---+
```

2.  **Selección de columnas específicas (`SELECT column1, column2`):**

Permite especificar las columnas que se desean recuperar, optimizando el rendimiento al no leer datos innecesarios.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQL").getOrCreate()

data = [("Alice", 25, "NY"), ("Bob", 30, "LA")]
df = spark.createDataFrame(data, ["name", "age", "city"])
df.createOrReplaceTempView("users")

spark.sql("SELECT name, city FROM users").show()
# Resultado:
# +-----+----+
# | name|city|
# +-----+----+
# |Alice|  NY|
# |  Bob|  LA|
# +-----+----+
```

3.  **Renombrar columnas con `AS`:**

Asigna un alias a una columna para hacer el resultado más legible o para evitar conflictos de nombres.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQL").getOrCreate()

data = [("ProductA", 100), ("ProductB", 200)]
df = spark.createDataFrame(data, ["item_name", "item_price"])
df.createOrReplaceTempView("items")

spark.sql("SELECT item_name AS product, item_price AS price FROM items").show()
# Resultado:
# +--------+-----+
# | product|price|
# +--------+-----+
# |ProductA|  100|
# |ProductB|  200|
# +--------+-----+
```

##### WHERE (Filtrado)

La cláusula `WHERE` se utiliza para filtrar filas basándose en una o más condiciones, al igual que en SQL tradicional.

1.  **Condiciones de igualdad y desigualdad (`=`, `!=`, `<`, `>`, `<=`, `>=`):**

Filtra filas donde una columna cumple una condición de comparación.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQLWhere").getOrCreate()

data = [("Juan", 30), ("Maria", 25), ("Pedro", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.createOrReplaceTempView("employees")

spark.sql("SELECT name, age FROM employees WHERE age > 28").show()
# Resultado:
# +-----+---+
# | name|age|
# +-----+---+
# | Juan| 30|
# |Pedro| 35|
# +-----+---+
```

2.  **Operadores lógicos (`AND`, `OR`, `NOT`):**

Combina múltiples condiciones de filtrado.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQLWhereLogic").getOrCreate()

data = [("Shirt", "Blue", "Small"), ("Pants", "Black", "Medium"), ("T-Shirt", "Red", "Large")]
df = spark.createDataFrame(data, ["item", "color", "size"])
df.createOrReplaceTempView("apparel")

spark.sql("SELECT item, color FROM apparel WHERE color = 'Blue' OR size = 'Large'").show()
# Resultado:
# +-------+-----+
# |   item|color|
# +-------+-----+
# |  Shirt| Blue|
# |T-Shirt|  Red|
# +-------+-----+
```

3.  **`LIKE`, `IN`, `BETWEEN`, `IS NULL` / `IS NOT NULL`:**

Funciones de filtrado comunes para patrones, listas de valores, rangos y valores nulos.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQLWhereSpecial").getOrCreate()

data = [("Apple", 1.0), ("Banana", 0.5), ("Cherry", 2.0), ("Date", None)]
df = spark.createDataFrame(data, ["fruit", "price"])
df.createOrReplaceTempView("fruits")

# LIKE
spark.sql("SELECT fruit FROM fruits WHERE fruit LIKE 'B%'").show()
# Resultado:
# +------+
# | fruit|
# +------+
# |Banana|
# +------+

# IN
spark.sql("SELECT fruit FROM fruits WHERE fruit IN ('Apple', 'Cherry')").show()
# Resultado:
# +------+
# | fruit|
# +------+
# | Apple|
# |Cherry|
# +------+

# BETWEEN
spark.sql("SELECT fruit, price FROM fruits WHERE price BETWEEN 0.8 AND 1.5").show()
# Resultado:
# +-----+-----+
# |fruit|price|
# +-----+-----+
# |Apple|  1.0|
# +-----+-----+

# IS NULL / IS NOT NULL
spark.sql("SELECT fruit, price FROM fruits WHERE price IS NULL").show()
# Resultado:
# +----+-----+
# |fruit|price|
# +----+-----+
# |Date| null|
# +----+-----+
```

##### GROUP BY y HAVING (Agregación)

`GROUP BY` se utiliza para agrupar filas que tienen los mismos valores en una o más columnas, permitiendo aplicar funciones de agregación. `HAVING` se usa para filtrar los resultados de las agregaciones.

1.  **Funciones de agregación (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`):**

Permiten resumir datos dentro de cada grupo.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQLAggregation").getOrCreate()

data = [("DeptA", 100), ("DeptB", 150), ("DeptA", 200), ("DeptC", 50), ("DeptB", 100)]
df = spark.createDataFrame(data, ["department", "salary"])
df.createOrReplaceTempView("salaries")

spark.sql("SELECT department, SUM(salary) AS total_salary, COUNT(*) AS num_employees FROM salaries GROUP BY department").show()
# Resultado:
# +----------+------------+-------------+
# |department|total_salary|num_employees|
# +----------+------------+-------------+
# |     DeptA|         300|            2|
# |     DeptB|         250|            2|
# |     DeptC|          50|            1|
# +----------+------------+-------------+
```

2.  **`HAVING` para filtrar resultados de agregación:**

Aplica condiciones de filtrado *después* de que las agregaciones se han calculado, a diferencia de `WHERE` que filtra antes.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicSQLHaving").getOrCreate()

data = [("DeptA", 100), ("DeptB", 150), ("DeptA", 200), ("DeptC", 50), ("DeptB", 100)]
df = spark.createDataFrame(data, ["department", "salary"])
df.createOrReplaceTempView("salaries")

# Sumar salarios por departamento, pero solo para aquellos departamentos con un total de salarios > 200
spark.sql("SELECT department, SUM(salary) AS total_salary FROM salaries GROUP BY department HAVING SUM(salary) > 200").show()
# Resultado:
# +----------+------------+
# |department|total_salary|
# +----------+------------+
# |     DeptA|         300|
# +----------+------------+
```

### 2.3.3 Creación y uso de vistas temporales

Las vistas temporales en SparkSQL son referencias a DataFrames que se registran en el catálogo de Spark como si fueran tablas de bases de datos. Son "temporales" porque solo duran mientras la `SparkSession` esté activa, o hasta que se eliminen explícitamente. Son esenciales para permitir que las consultas SQL interactúen con los DataFrames.

##### Vistas Temporales

Las vistas temporales son la forma principal de exponer un DataFrame para ser consultado mediante SQL.

1.  **`createTempView()` vs. `createOrReplaceTempView()`:**

* `createTempView(view_name)`: Crea una vista temporal con el nombre especificado. Si ya existe una vista con ese nombre, lanzará un error.
* `createOrReplaceTempView(view_name)`: Crea o reemplaza una vista temporal. Si una vista con el mismo nombre ya existe, la reemplazará sin lanzar un error. Esta es la más comúnmente utilizada.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TempViews").getOrCreate()

data1 = [("Laptop", 1200)]
df1 = spark.createDataFrame(data1, ["item", "price"])

data2 = [("Monitor", 300)]
df2 = spark.createDataFrame(data2, ["item", "price"])

# Usar createTempView (fallará si se ejecuta dos veces sin borrar)
df1.createTempView("products_v1")
spark.sql("SELECT * FROM products_v1").show()

# Usar createOrReplaceTempView (permite sobrescribir)
df2.createOrReplaceTempView("products_v1") # Reemplaza la vista existente
spark.sql("SELECT * FROM products_v1").show()
# Resultado:
# +-------+-----+
# |   item|price|
# +-------+-----+
# |Monitor|  300|
# +-------+-----+
```

2.  **Alcance de las vistas temporales (sesión):**

Las vistas temporales son locales a la `SparkSession` en la que se crearon. No son visibles para otras `SparkSession`s ni persisten después de que la `SparkSession` finaliza.

```python
from pyspark.sql import SparkSession

spark1 = SparkSession.builder.appName("Session1").getOrCreate()
spark2 = SparkSession.builder.appName("Session2").getOrCreate()

df_session1 = spark1.createDataFrame([("Data1",)], ["col"])
df_session1.createOrReplaceTempView("my_data_session1")

# Consulta en la misma sesión (spark1)
spark1.sql("SELECT * FROM my_data_session1").show()

# Intentar consultar desde otra sesión (spark2) - esto fallará
try:
    spark2.sql("SELECT * FROM my_data_session1").show()
except Exception as e:
    print(f"Error esperado en spark2: {e}")
    # Resultado: Error esperado en spark2: Table or view not found: my_data_session1;
```

3.  **Eliminación de vistas temporales (`dropTempView()`):**

Aunque son temporales, es buena práctica eliminarlas explícitamente si ya no se necesitan para liberar recursos o evitar conflictos de nombres.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DropView").getOrCreate()

df = spark.createDataFrame([("test",)], ["col"])
df.createOrReplaceTempView("my_test_view")

spark.sql("SELECT * FROM my_test_view").show()

spark.catalog.dropTempView("my_test_view")

# Intentar consultar después de eliminar - esto fallará
try:
    spark.sql("SELECT * FROM my_test_view").show()
except Exception as e:
    print(f"Error esperado: {e}")
    # Resultado: Error esperado: Table or view not found: my_test_view;
```

##### Vistas Globales Temporales

A diferencia de las vistas temporales, las vistas globales temporales son visibles a través de todas las `SparkSession`s dentro de la misma aplicación Spark. Esto las hace útiles para compartir datos entre diferentes módulos o usuarios de una misma aplicación.

1.  **`createGlobalTempView()` vs. `createOrReplaceGlobalTempView()`:**

* `createGlobalTempView(view_name)`: Crea una vista global temporal. Lanza un error si ya existe.
* `createOrReplaceGlobalTempView(view_name)`: Crea o reemplaza una vista global temporal.

Las vistas globales temporales se acceden con el prefijo `global_temp`.`view_name`.

```python
from pyspark.sql import SparkSession

# Session 1
spark1 = SparkSession.builder.appName("GlobalViewSession1").getOrCreate()

df_global = spark1.createDataFrame([("GlobalData",)], ["data_col"])
df_global.createOrReplaceGlobalTempView("shared_data")

spark1.sql("SELECT * FROM global_temp.shared_data").show()

# Session 2
spark2 = SparkSession.builder.appName("GlobalViewSession2").getOrCreate()

# La vista global es accesible desde otra SparkSession
spark2.sql("SELECT * FROM global_temp.shared_data").show()
# Resultado (desde spark2):
# +----------+
# |  data_col|
# +----------+
# |GlobalData|
# +----------+
```

2.  **Alcance de las vistas globales temporales (aplicación Spark):**

Persisten mientras la aplicación Spark esté activa (es decir, el proceso `SparkContext` esté en ejecución). Se eliminan cuando la aplicación finaliza.

El ejemplo anterior demuestra el alcance a través de sesiones, una vez que el script o la aplicación Spark finalizan, la vista global temporal se destruye.

3.  **Eliminación de vistas globales temporales (`dropGlobalTempView()`):**

Se pueden eliminar explícitamente si ya no se necesitan.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DropGlobalView").getOrCreate()

df = spark.createDataFrame([("global test",)], ["col"])
df.createOrReplaceGlobalTempView("my_global_view")

spark.sql("SELECT * FROM global_temp.my_global_view").show()

spark.catalog.dropGlobalTempView("my_global_view")

# Intentar consultar después de eliminar - esto fallará
try:
    spark.sql("SELECT * FROM global_temp.my_global_view").show()
except Exception as e:
    print(f"Error esperado: {e}")
    # Resultado: Error esperado: Table or view not found: my_global_view;
```

### 2.3.4 Consultas avanzadas con SparkSQL

SparkSQL no se limita a consultas básicas; soporta características SQL avanzadas que son fundamentales para análisis complejos y transformaciones de datos, como uniones complejas, subconsultas, funciones de ventana y Common Table Expressions (CTEs).

##### Uniones (JOINs) avanzadas

Más allá del `INNER JOIN`, SparkSQL soporta una gama completa de tipos de uniones, incluyendo `LEFT OUTER`, `RIGHT OUTER`, `FULL OUTER`, `LEFT SEMI` y `LEFT ANTI JOIN`.

1.  **Tipos de JOINs (`INNER`, `LEFT OUTER`, `RIGHT OUTER`, `FULL OUTER`):**

Comprender las diferencias entre los tipos de uniones es crucial para obtener los resultados deseados.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AdvancedJoins").getOrCreate()

# Tabla de empleados
employees_data = [(1, "Alice", 101), (2, "Bob", 102), (3, "Charlie", 103)]
employees_df = spark.createDataFrame(employees_data, ["emp_id", "emp_name", "dept_id"])
employees_df.createOrReplaceTempView("employees")

# Tabla de departamentos
departments_data = [(101, "Sales"), (102, "HR"), (104, "IT")]
departments_df = spark.createDataFrame(departments_data, ["dept_id", "dept_name"])
departments_df.createOrReplaceTempView("departments")

# LEFT OUTER JOIN (todas las filas de la izquierda, coincidencias de la derecha o NULLs)
spark.sql("""
    SELECT e.emp_name, d.dept_name
    FROM employees e
    LEFT OUTER JOIN departments d ON e.dept_id = d.dept_id
""").show()
# Resultado:
# +---------+---------+
# | emp_name|dept_name|
# +---------+---------+
# |    Alice|    Sales|
# |      Bob|       HR|
# |  Charlie|     null|
# +---------+---------+

# FULL OUTER JOIN (todas las filas de ambas tablas, con NULLs donde no hay coincidencia)
spark.sql("""
    SELECT e.emp_name, d.dept_name
    FROM employees e
    FULL OUTER JOIN departments d ON e.dept_id = d.dept_id
""").show()
# Resultado:
# +---------+---------+
# | emp_name|dept_name|
# +---------+---------+
# |  Charlie|     null|
# |    Alice|    Sales|
# |      Bob|       HR|
# |     null|       IT|
# +---------+---------+
```

2.  **`LEFT SEMI JOIN` y `LEFT ANTI JOIN` (Subconsultas optimizadas):**

* `LEFT SEMI JOIN`: Retorna las filas del lado izquierdo (primera tabla) que tienen una coincidencia en el lado derecho (segunda tabla). No incluye columnas de la tabla derecha. Es similar a un `INNER JOIN` pero solo retorna columnas de la tabla izquierda y es más eficiente.
* `LEFT ANTI JOIN`: Retorna las filas del lado izquierdo que *no* tienen una coincidencia en el lado derecho. Útil para encontrar registros huérfanos.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SemiAntiJoins").getOrCreate()

# Tabla de clientes
customers_data = [(1, "Ana"), (2, "Luis"), (3, "Marta"), (4, "Pedro")]
customers_df = spark.createDataFrame(customers_data, ["customer_id", "customer_name"])
customers_df.createOrReplaceTempView("customers")

# Tabla de pedidos
orders_data = [(101, 1, 50), (102, 2, 75), (103, 1, 120)] # Marta y Pedro no tienen pedidos
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])
orders_df.createOrReplaceTempView("orders")

# LEFT SEMI JOIN (clientes que tienen al menos un pedido)
spark.sql("""
    SELECT c.customer_name
    FROM customers c
    LEFT SEMI JOIN orders o ON c.customer_id = o.customer_id
""").show()
# Resultado:
# +-------------+
# |customer_name|
# +-------------+
# |          Ana|
# |         Luis|
# +-------------+

# LEFT ANTI JOIN (clientes que NO tienen ningún pedido)
spark.sql("""
    SELECT c.customer_name
    FROM customers c
    LEFT ANTI JOIN orders o ON c.customer_id = o.customer_id
""").show()
# Resultado:
# +-------------+
# |customer_name|
# +-------------+
# |        Marta|
# |        Pedro|
# +-------------+
```

##### Subconsultas y CTEs (Common Table Expressions)

Las subconsultas y CTEs permiten dividir consultas complejas en partes más manejables, mejorando la legibilidad y, en algunos casos, la optimización.

1.  **Subconsultas en `WHERE` (IN, EXISTS):**

Permiten filtrar resultados basándose en los valores de otra consulta.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Subqueries").getOrCreate()

# Tabla de productos
products_data = [(1, "Laptop", 1200), (2, "Mouse", 25), (3, "Keyboard", 75), (4, "Monitor", 300)]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "price"])
products_df.createOrReplaceTempView("products")

# Tabla de stock
stock_data = [(1, 10), (3, 5), (5, 20)] # Producto 2 y 4 no tienen stock
stock_df = spark.createDataFrame(stock_data, ["product_id", "quantity"])
stock_df.createOrReplaceTempView("stock")

# Seleccionar productos que tienen stock (usando IN)
spark.sql("""
    SELECT product_name, price
    FROM products
    WHERE product_id IN (SELECT product_id FROM stock)
""").show()
# Resultado:
# +------------+-----+
# |product_name|price|
# +------------+-----+
# |      Laptop| 1200|
# |    Keyboard|   75|
# +------------+-----+
```

2.  **CTEs (Common Table Expressions) con `WITH`:**

Las CTEs definen un conjunto de resultados temporal y nombrado que se puede referenciar dentro de una única sentencia `SELECT`, `INSERT`, `UPDATE` o `DELETE`. Mejoran la legibilidad y la modularidad de las consultas complejas.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CTEs").getOrCreate()

# Tabla de ventas
sales_data = [("RegionA", "Q1", 1000), ("RegionA", "Q2", 1200),
              ("RegionB", "Q1", 800), ("RegionB", "Q2", 900),
              ("RegionA", "Q3", 1500)]
sales_df = spark.createDataFrame(sales_data, ["region", "quarter", "revenue"])
sales_df.createOrReplaceTempView("sales")

# Calcular el promedio de ingresos por región usando una CTE
spark.sql("""
    WITH RegionalRevenue AS (
        SELECT
            region,
            SUM(revenue) AS total_region_revenue
        FROM sales
        GROUP BY region
    )
    SELECT
        r.region,
        r.total_region_revenue,
        (SELECT AVG(total_region_revenue) FROM RegionalRevenue) AS overall_avg_revenue
    FROM RegionalRevenue r
""").show()
# Resultado:
# +-------+------------------+-------------------+
# | region|total_region_revenue|overall_avg_revenue|
# +-------+------------------+-------------------+
# |RegionA|              3700|             2200.0|
# |RegionB|              1700|             2200.0|
# +-------+------------------+-------------------+
```

3.  **Funciones de Ventana (Window Functions):**

Permiten realizar cálculos sobre un conjunto de filas relacionadas con la fila actual (`PARTITION BY`, `ORDER BY`, `ROWS BETWEEN` / `RANGE BETWEEN`). Son muy potentes para cálculos analíticos como promedios móviles, rankings, sumas acumuladas, etc.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

# Tabla de transacciones
transactions_data = [("StoreA", "2023-01-01", 100), ("StoreA", "2023-01-02", 150), ("StoreA", "2023-01-03", 80),
                     ("StoreB", "2023-01-01", 200), ("StoreB", "2023-01-02", 120)]
transactions_df = spark.createDataFrame(transactions_data, ["store_id", "sale_date", "amount"])
transactions_df.createOrReplaceTempView("transactions")

# Calcular la suma acumulada de ventas por tienda, ordenada por fecha
spark.sql("""
    SELECT
        store_id,
        sale_date,
        amount,
        SUM(amount) OVER (PARTITION BY store_id ORDER BY sale_date) AS running_total
    FROM transactions
    ORDER BY store_id, sale_date
""").show()
# Resultado:
# +--------+----------+------+-------------+
# |store_id| sale_date|amount|running_total|
# +--------+----------+------+-------------+
# |  StoreA|2023-01-01|   100|          100|
# |  StoreA|2023-01-02|   150|          250|
# |  StoreA|2023-01-03|    80|          330|
# |  StoreB|2023-01-01|   200|          200|
# |  StoreB|2023-01-02|   120|          320|
# +--------+----------+------+-------------+

# Ranking de transacciones por tienda
spark.sql("""
    SELECT
        store_id,
        sale_date,
        amount,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC) AS rank_by_amount
    FROM transactions
    ORDER BY store_id, rank_by_amount
""").show()
# Resultado:
# +--------+----------+------+------------+
# |store_id| sale_date|amount|rank_by_amount|
# +--------+----------+------+------------+
# |  StoreA|2023-01-02|   150|           1|
# |  StoreA|2023-01-01|   100|           2|
# |  StoreA|2023-01-03|    80|           3|
# |  StoreB|2023-01-01|   200|           1|
# |  StoreB|2023-01-02|   120|           2|
# +--------+----------+------+------------+
```

## Tarea

### Ejercicios Prácticos relacionados con el tema 2.3

1.  Crea un DataFrame de `estudiantes` con las columnas `id`, `nombre`, `carrera` y `promedio_calificaciones`.
    Datos de ejemplo: `[(1, "Juan", "Ingeniería", 4.2), (2, "Maria", "Medicina", 3.8), (3, "Pedro", "Ingeniería", 4.5), (4, "Ana", "Derecho", 3.5)]`.

    Registra este DataFrame como una vista temporal llamada `estudiantes_temp`.
    Luego, escribe una consulta SparkSQL que seleccione el `nombre` y la `carrera` de todos los estudiantes con un `promedio_calificaciones` superior a `4.0`.

2.  Usando la vista `estudiantes_temp`, escribe una consulta SparkSQL que seleccione el `nombre`, `carrera` y `promedio_calificaciones` de los estudiantes cuya `carrera` sea 'Ingeniería' Y cuyo `nombre` empiece con 'P'.

3.  Crea un DataFrame de `ventas` con columnas `region`, `producto` y `cantidad`.
    Datos de ejemplo: `[("Norte", "A", 10), ("Norte", "B", 15), ("Sur", "A", 20), ("Norte", "A", 5), ("Sur", "B", 12)]`.

    Registra este DataFrame como `ventas_temp`.
    Escribe una consulta SparkSQL que calcule la `SUM` de `cantidad` por `region` y `producto`, pero solo para aquellos grupos donde la `cantidad` total sea mayor o igual a `25`.

4.  Crea un DataFrame de `departamentos` con `id_departamento` y `nombre_departamento`.
    Datos de ejemplo: `[(1, "Ventas"), (2, "Marketing"), (3, "Recursos Humanos")]`.

    Registra este DataFrame como `departamentos_temp`.
    Realiza un `INNER JOIN` entre `estudiantes_temp` (asume que `carrera` se puede unir con `nombre_departamento`) y `departamentos_temp` para mostrar el `nombre` del estudiante y el `nombre_departamento` al que pertenecen. *Nota: Para simplificar, puedes asumir que `carrera` de `estudiantes_temp` corresponde a `nombre_departamento` en `departamentos_temp`.*

5.  Crea un DataFrame de `productos_disponibles` con `producto_id` y `nombre_producto`.
    Datos de ejemplo: `[(101, "Laptop"), (102, "Mouse"), (103, "Teclado"), (104, "Monitor")]`.

    Registra como `productos_disponibles_temp`.

    Crea otro DataFrame de `productos_vendidos` con `producto_id` y `fecha_venta`.
    Datos de ejemplo: `[(101, "2023-01-01"), (103, "2023-01-05")]`.

    Registra como `productos_vendidos_temp`.
    Escribe una consulta SparkSQL que encuentre los `nombre_producto` de los productos que *nunca* han sido vendidos.

6.  Utilizando las vistas `productos_disponibles_temp` y `productos_vendidos_temp` del ejercicio anterior, escribe una consulta SparkSQL usando una subconsulta con `IN` para seleccionar los `nombre_producto` de los productos que *sí* han sido vendidos.

7.  Crea un DataFrame de `empleados_salarios` con `id_empleado`, `departamento` y `salario`.
    Datos de ejemplo: `[(1, "IT", 60000), (2, "IT", 70000), (3, "HR", 50000), (4, "HR", 55000), (5, "IT", 65000)]`.

    Registra como `empleados_salarios_temp`.
    Usa una CTE para calcular el `salario_promedio_departamental` para cada `departamento`. Luego, en la consulta principal, selecciona `id_empleado`, `departamento`, `salario` y el `salario_promedio_departamental` de su departamento.

8.  Crea un DataFrame de `puntajes_examen` con `clase`, `estudiante` y `puntaje`.
    Datos de ejemplo: `[("A", "Alice", 90), ("A", "Bob", 85), ("A", "Charlie", 92), ("B", "David", 78), ("B", "Eve", 95)]`.

    Registra como `puntajes_examen_temp`.
    Usa una función de ventana para calcular el ranking de `puntaje` para cada `estudiante` *dentro de cada `clase`*, ordenando de mayor a menor puntaje. Muestra `clase`, `estudiante`, `puntaje` y `ranking`.

9.  Utilizando la vista `ventas_temp` del ejercicio 3 (añade una columna `fecha_venta` si no la tienes, ej. `"2023-01-01"` para todos por simplicidad, o mejor, crea nuevos datos con fechas distintas para cada venta en la misma región):
    `[("Norte", "A", 10, "2023-01-01"), ("Norte", "B", 15, "2023-01-02"), ("Norte", "A", 5, "2023-01-03")]`.

    Registra como `ventas_fecha_temp`.
    Calcula la `cantidad_acumulada` de ventas por `region`, ordenada por `fecha_venta`.

10. Crea dos DataFrames: `usuarios` (`user_id`, `name`) y `compras` (`purchase_id`, `user_id`, `amount`, `purchase_date`).
    `usuarios` ejemplo: `[(1, "User A"), (2, "User B")]`
    `compras` ejemplo: `[(101, 1, 50.0, "2023-01-01"), (102, 1, 75.0, "2023-01-10"), (103, 2, 120.0, "2023-01-15")]`

    Registra ambos como vistas temporales (`users_temp`, `purchases_temp`).
    Usa una CTE para encontrar el `total_comprado` por cada `user_id`. Luego, en la consulta principal, une la CTE con `users_temp` para mostrar `name` y su `total_comprado`.

