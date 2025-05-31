# 1. Introducción

## Tema 1.5 Primeros pasos con PySpark

**Objetivo**:

Adquirir las habilidades fundamentales para configurar un entorno de desarrollo PySpark, inicializar una sesión Spark, cargar y explorar datos, y realizar transformaciones básicas de DataFrames, sentando las bases para el análisis y procesamiento de Big Data.

**Introducción**:

PySpark es la API de Python para Apache Spark, que permite a los desarrolladores y científicos de datos aprovechar el poder computacional distribuido de Spark utilizando la familiaridad y versatilidad del lenguaje Python. Es una herramienta indispensable para el procesamiento de datos a gran escala, Machine Learning y análisis en entornos Big Data. Este tema te guiará a través de los primeros pasos esenciales con PySpark, desde la configuración de tu entorno hasta la ejecución de tus primeras operaciones con DataFrames.

**Desarrollo**:

En este tema, exploraremos cómo empezar a trabajar con PySpark de forma práctica. Comenzaremos configurando un entorno de desarrollo Python adecuado e instalando las librerías necesarias. Luego, aprenderemos a inicializar una `SparkSession`, que es el punto de entrada principal para cualquier aplicación Spark. Una vez que tengamos un contexto Spark, nos centraremos en cómo cargar datos desde diversas fuentes en DataFrames y cómo realizar operaciones básicas de exploración para entender la estructura y el contenido de nuestros datos. Finalmente, cubriremos las transformaciones más comunes que te permitirán manipular y preparar tus datos para análisis más avanzados.

### 1.5.1 Entorno de desarrollo para PySpark

Un entorno de desarrollo bien configurado es crucial para trabajar eficientemente con PySpark. Esto implica tener una instalación de Python gestionada, PySpark instalado como una librería de Python, y un entorno para escribir y ejecutar código, como Jupyter Notebooks o un IDE.

##### Configuración de un entorno Python (Anaconda/Miniconda, virtualenv)

Para gestionar las dependencias de Python y evitar conflictos entre proyectos, es altamente recomendable utilizar entornos virtuales. **Anaconda/Miniconda** son distribuciones de Python que vienen con su propio gestor de paquetes (`conda`) y facilitan la creación y gestión de entornos. **virtualenv** es otra herramienta estándar de Python para crear entornos virtuales aislados.

1.  Crear un nuevo entorno conda para PySpark: `conda create -n pyspark_env python=3.9`.
2.  Activar el entorno recién creado: `conda activate pyspark_env`.
3.  Usar `virtualenv` para crear un entorno: `python -m venv pyspark_venv` y activarlo con `source pyspark_venv/bin/activate` (Linux/macOS) o `pyspark_venv\Scripts\activate` (Windows).

##### Instalación de PySpark (`pip install pyspark`)

Una vez que tu entorno Python está activado, la instalación de PySpark es tan sencilla como usar `pip`. Esto descargará la librería de PySpark y sus dependencias, permitiéndote importar `pyspark` en tus scripts.

1.  Instalar la última versión de PySpark: `pip install pyspark`.
2.  Instalar una versión específica de PySpark para asegurar compatibilidad: `pip install pyspark==3.5.0`.
3.  Verificar la instalación abriendo un intérprete de Python y ejecutando `import pyspark`. Si no hay errores, la instalación fue exitosa.

##### Integración con Jupyter Notebooks o IDEs (VS Code, PyCharm)

Para escribir y ejecutar código PySpark de forma interactiva y con herramientas de desarrollo avanzadas, puedes integrar PySpark con Jupyter Notebooks o IDEs como VS Code o PyCharm.

1.  Para usar PySpark en **Jupyter Notebooks**, instala `jupyter` (`pip install jupyter`). Luego, al iniciar un notebook, puedes importar `SparkSession` y usarlo directamente.

```python
# En una celda de Jupyter
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyFirstPySparkApp").getOrCreate()
```

2.  En **VS Code**, instala la extensión de Python y abre una carpeta de proyecto. Puedes configurar el intérprete de Python para que sea el de tu entorno virtual de PySpark. Para ejecutar scripts `.py`, configura `SPARK_HOME` y `PYTHONPATH` en tu terminal antes de ejecutar `spark-submit`.
3.  En **PyCharm**, puedes configurar un intérprete de Python basado en tu entorno virtual. Para ejecutar aplicaciones Spark, puedes configurar una "Run Configuration" que utilice `spark-submit` internamente.

##### Acceso a la Spark UI

La **Spark UI** es una interfaz web que proporciona monitoreo en tiempo real de las aplicaciones Spark en ejecución. Es invaluable para depurar, optimizar y entender el rendimiento de tus aplicaciones PySpark. Cuando ejecutas una aplicación Spark, el Driver de Spark lanza un servidor web para la UI.

1.  Al ejecutar una aplicación PySpark localmente, la Spark UI suele estar disponible en `http://localhost:4040`. Si ya hay una aplicación ejecutándose, el puerto puede incrementarse (ej. 4041, 4042).
2.  Acceder a la pestaña "Jobs" para ver el DAG de ejecución, las etapas y las tareas, y cuánto tiempo tardó cada una.
3.  Utilizar la pestaña "Executors" para monitorear el uso de memoria y CPU de los ejecutores y revisar sus logs en busca de errores.

### 1.5.2 Inicialización de SparkSession

La `SparkSession` es el punto de entrada principal para programar Spark con la API de DataFrame y Dataset. Sustituyó a `SparkContext` y `SQLContext` a partir de Spark 2.0, unificando todas las funcionalidades en una sola interfaz.

##### El papel de `SparkSession`

`SparkSession` es el objeto unificado para interactuar con Spark. Proporciona acceso a todas las funcionalidades de Spark, incluyendo la creación de DataFrames, la ejecución de SQL, la lectura y escritura de datos, y el acceso al `SparkContext` subyacente. Se encarga de la comunicación con el clúster y la gestión de recursos.

1.  Crear una `SparkSession` con un nombre de aplicación específico y el modo de ejecución local:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MiPrimeraAppPySpark") \
    .master("local[*]") \
    .getOrCreate()
```

2.  Si intentas crear una segunda `SparkSession` en la misma aplicación, `getOrCreate()` devolverá la instancia existente, asegurando que solo haya una activa.
3.  Utilizar el objeto `spark` para acceder a funcionalidades como `spark.read` (para cargar datos) o `spark.sql` (para ejecutar consultas SQL).

##### Creación de una `SparkSession`

La `SparkSession` se crea utilizando el patrón `builder`. Puedes encadenar métodos para configurar diferentes aspectos de la sesión antes de llamar a `getOrCreate()` para obtener la instancia.

1.  Crear una `SparkSession` simple para desarrollo local:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LocalTestApp").master("local[*]").getOrCreate()
```

2.  Configurar la memoria del driver y los ejecutores al crear la `SparkSession`:

```python
spark = SparkSession.builder \
    .appName("BigDataJob") \
    .master("yarn") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
```

3.  Detener la `SparkSession` al finalizar la aplicación para liberar recursos: `spark.stop()`. Esto es importante, especialmente en entornos de producción o scripts.

### 1.5.3 Carga de datos con PySpark

Una de las tareas más comunes en el procesamiento de datos es cargar información desde diversas fuentes. PySpark proporciona APIs flexibles y potentes para leer datos en DataFrames desde una variedad de formatos y sistemas de archivos distribuidos.

##### Lectura de archivos CSV (inferSchema, header, delimiter)

El formato CSV es uno de los más utilizados para el intercambio de datos. PySpark ofrece opciones robustas para leer archivos CSV, incluyendo la inferencia automática del esquema, el manejo de encabezados y la especificación de delimitadores.

1.  Cargar un archivo CSV, indicando que la primera fila es el encabezado y que Spark debe inferir el esquema (tipos de datos de las columnas):

```python
df_csv = spark.read.csv("data/clientes.csv", header=True, inferSchema=True)
df_csv.show()
```

2.  Cargar un CSV con un delimitador diferente (ej. `;`) y sin encabezado:

```python
df_semicolon = spark.read.csv("data/productos.txt", sep=";", header=False)
df_semicolon.printSchema() # Mostrará _c0, _c1, etc.
```

3.  Deshabilitar la inferencia de esquema para mayor control y especificar el esquema manualmente para un CSV grande (mejora el rendimiento):

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True)
])
df_manual_schema = spark.read.csv("data/usuarios.csv", header=True, schema=schema)
```

##### Lectura de archivos JSON

Los archivos JSON son comunes para datos semiestructurados. PySpark puede leer JSON de forma sencilla, tratándolos como objetos anidados y creando un esquema basado en su estructura.

1.  Cargar un archivo JSON (cada línea es un objeto JSON válido):

```python
df_json = spark.read.json("data/eventos.json")
df_json.show()
df_json.printSchema() # Muestra la estructura inferida
```

2.  Cargar múltiples archivos JSON de un directorio:

```python
df_multi_json = spark.read.json("data/json_logs/*.json")
```

3.  Si los archivos JSON tienen un formato más complejo o se distribuyen en múltiples líneas, Spark puede necesitar una configuración adicional, aunque por defecto asume un objeto JSON por línea.

### 1.5.4 Exploración básica de DataFrames

Una vez que los datos se han cargado en un DataFrame, el siguiente paso es explorarlos para entender su estructura, contenido y calidad. PySpark proporciona métodos intuitivos para visualizar, inspeccionar y obtener estadísticas descriptivas de tus DataFrames.

##### Visualización de datos (`show()`, `printSchema()`, `describe()`)

Estos métodos son esenciales para obtener una primera impresión rápida de tu DataFrame. `show()` muestra las primeras filas, `printSchema()` revela la estructura de las columnas y sus tipos de datos, y `describe()` proporciona estadísticas resumidas para columnas numéricas y de cadena.

1.  Mostrar las primeras 5 filas del DataFrame:

```python
df.show(5)
# Por defecto muestra 20 filas, show(n) para n filas, show(n, truncate=False) para no truncar cadenas largas
```

2.  Imprimir el esquema del DataFrame para ver nombres de columnas y tipos de datos:

```python
df.printSchema()
# Output:
# root
#  |-- id: integer (nullable = true)
#  |-- nombre: string (nullable = true)
#  |-- edad: integer (nullable = true)
```

3.  Obtener estadísticas descriptivas para todas las columnas numéricas y de cadena:

```python
df.describe().show()
# Output (ejemplo para 'edad' y 'nombre'):
# summary  id       nombre   edad
# -------- -------- -------- ----
# count    100      100      100
# mean     50.5     null     35.0
# stddev   29.01    null     10.0
# min      1        Alice    20
# max      100      Zoe      50
```

##### Selección de columnas (`select()`)

La operación `select()` te permite elegir un subconjunto de columnas de un DataFrame, o incluso crear nuevas columnas basadas en transformaciones de las existentes. Es fundamental para reducir el volumen de datos o preparar datos para análisis específicos.

1.  Seleccionar una o varias columnas por su nombre:

```python
df_selected = df.select("nombre", "edad")
df_selected.show()
```

2.  Renombrar una columna mientras se selecciona:

```python
from pyspark.sql.functions import col
df_renamed = df.select(col("nombre").alias("nombre_completo"), "edad")
df_renamed.show()
```

3.  Crear una nueva columna aplicando una función a una columna existente:

```python
df_with_new_col = df.select("nombre", "edad", (col("edad") * 12).alias("edad_meses"))
df_with_new_col.show()
```

##### Filtrado de filas (`filter()` / `where()`)

Las operaciones `filter()` y `where()` son equivalentes y se utilizan para seleccionar filas que satisfacen una o más condiciones. Son cruciales para limpiar datos o para concentrarse en subconjuntos específicos de información.

1.  Filtrar filas donde la edad sea mayor de 30:

```python
df_adultos = df.filter(df.edad > 30)
df_adultos.show()
```

2.  Aplicar múltiples condiciones de filtrado usando operadores lógicos (`&` para AND, `|` para OR, `~` para NOT):

```python
df_filtered = df.filter((df.edad >= 25) & (df.nombre.contains("a")))
df_filtered.show()
```

3.  Usar una expresión SQL para el filtrado:

```python
df_sql_filter = df.where("edad < 30 AND id % 2 = 0")
df_sql_filter.show()
```

### 1.5.5 Operaciones comunes de transformación

Las transformaciones son el corazón del procesamiento de datos en Spark. Permiten modificar DataFrames de diversas maneras, desde añadir o eliminar columnas hasta realizar agregaciones y uniones, preparando los datos para análisis más complejos.

##### Renombrar y eliminar columnas (`withColumnRenamed()`, `drop()`)

Estas operaciones son fundamentales para limpiar y organizar tu DataFrame, haciéndolo más legible y adecuado para los análisis posteriores.

1.  Renombrar una columna:

```python
df_renamed_col = df.withColumnRenamed("nombre", "nombre_del_cliente")
df_renamed_col.show()
```

2.  Eliminar una o varias columnas:

```python
df_dropped_col = df.drop("id", "nombre_del_cliente") # Si se renombro antes
df_dropped_col.show()
```

3.  Renombrar una columna y luego eliminar otra en una secuencia:

```python
df_processed = df.withColumnRenamed("edad", "age").drop("id")
df_processed.show()
```

##### Añadir y modificar columnas (`withColumn()`)

El método `withColumn()` es extremadamente versátil. Permite añadir una nueva columna a un DataFrame o modificar una existente, basándose en expresiones o funciones.

1.  Añadir una nueva columna calculada, por ejemplo, `es_mayor_edad` basada en `edad`:

```python
from pyspark.sql.functions import when
df_with_flag = df.withColumn("es_mayor_edad", when(df.edad >= 18, "Sí").otherwise("No"))
df_with_flag.show()
```

2.  Modificar una columna existente, por ejemplo, convertir `nombre` a mayúsculas:

```python
from pyspark.sql.functions import upper
df_upper_name = df.withColumn("nombre", upper(df.nombre))
df_upper_name.show()
```

3.  Crear una columna a partir de un valor literal:

```python
from pyspark.sql.functions import lit
df_with_constant = df.withColumn("fuente", lit("sistema_A"))
df_with_constant.show()
```

##### Operaciones de agregación (`groupBy()`, `agg()`, `sum()`, `avg()`, `min()`, `max()`)

Las agregaciones son fundamentales para resumir datos. `groupBy()` se utiliza para agrupar filas que tienen el mismo valor en una o más columnas, y `agg()` se utiliza para aplicar funciones de agregación (como suma, promedio, conteo) a los grupos resultantes.

1.  Calcular el promedio de edad por sexo:

```python
df_agg = df.groupBy("sexo").agg({"edad": "avg"}).show()
# Alternativa más explícita con funciones:
# from pyspark.sql.functions import avg
# df.groupBy("sexo").agg(avg("edad").alias("edad_promedio")).show()
```

2.  Contar el número de clientes por ciudad y la edad máxima en cada ciudad:

```python
from pyspark.sql.functions import count, max
df.groupBy("ciudad").agg(count("*").alias("num_clientes"), max("edad").alias("edad_maxima")).show()
```

3.  Agregación de múltiples columnas y funciones:

```python
from pyspark.sql.functions import sum, min
df.groupBy("departamento").agg(
    sum("ventas").alias("total_ventas"),
    min("fecha_pedido").alias("primer_pedido")
).show()
```

### 1.5.6 Escritura de datos con PySpark

Una vez que has procesado y transformado tus datos con PySpark, el paso final es guardar los resultados en un formato y ubicación deseados. PySpark soporta la escritura en varios formatos y ofrece modos para controlar cómo se manejan los datos existentes.

##### Guardar DataFrames en formato CSV, JSON, Parquet

PySpark permite guardar DataFrames en diversos formatos de archivo, como CSV, JSON y Parquet. El formato Parquet es generalmente preferido en el ecosistema Big Data por su eficiencia en el almacenamiento y el rendimiento de las consultas, ya que es un formato columnar optimizado.

1.  Guardar un DataFrame en formato CSV:

```python
df_resultado.write.csv("output/clientes_procesados.csv", header=True, mode="overwrite")
# Esto creará un directorio con múltiples archivos CSV (uno por partición)
```

2.  Guardar un DataFrame en formato JSON:

```python
df_resultado.write.json("output/eventos_limpios.json", mode="append")
```

3.  Guardar un DataFrame en formato Parquet (recomendado para eficiencia):

```python
df_resultado.write.parquet("output/datos_analiticos.parquet", mode="overwrite")
```

##### Modos de escritura (append, overwrite, ignore, errorIfExists)

PySpark ofrece diferentes modos para manejar la situación cuando un archivo o directorio de salida ya existe, lo que te da control sobre la persistencia de tus datos.

1.  **`overwrite`**: Sobrescribe el directorio de salida si ya existe. ¡Útil pero peligroso si no se usa con cuidado!

```python
df.write.mode("overwrite").parquet("output/mi_data")
```

2.  **`append`**: Si el directorio de salida ya existe, los nuevos datos se añadirán a los datos existentes.

```python
df.write.mode("append").csv("output/registros.csv")
```

3.  **`ignore`**: Si el directorio de salida ya existe, la operación de escritura no hará nada y los datos existentes permanecerán intactos.

```python
df.write.mode("ignore").json("output/datos_seguros.json")
```

4.  **`errorIfExists`** (por defecto): Si el directorio de salida ya existe, lanzará una excepción, evitando la sobrescritura accidental.

```python
# df.write.mode("errorIfExists").csv("output/error.csv") # Esto fallará si el directorio existe
df.write.csv("output/nuevo_csv.csv") # El modo por defecto es errorIfExists
```

##### Particionamiento de la salida (`partitionBy()`)

El método `partitionBy()` permite organizar la salida de tu DataFrame en subdirectorios basados en los valores de una o más columnas. Esto mejora el rendimiento de lectura para consultas que filtran por esas columnas, ya que Spark puede escanear solo los subdirectorios relevantes.

1.  Particionar los datos de ventas por año y mes:

```python
df_ventas.write.partitionBy("anio", "mes").parquet("output/ventas_particionadas")
# Esto creará una estructura como: ventas_particionadas/anio=2023/mes=01/part-*.parquet
```

2.  Guardar datos de usuarios particionados por país:

```python
df_usuarios.write.mode("overwrite").partitionBy("pais").json("output/usuarios_por_pais")
```

3.  Combinar particionamiento con un formato de archivo específico:

```python
df_logs.write.partitionBy("fecha").csv("output/logs_diarios", header=True)
```

## Tarea

Aquí tienes 8 ejercicios de programación PySpark para practicar los conceptos aprendidos:

1.  **Inicialización y Carga Básica**:
    * Crea una `SparkSession` llamada "MiPrimeraAppPySpark".
    * Crea una lista de tuplas en Python que represente datos de empleados (ej. `[(1, "Alice", 30, "IT"), (2, "Bob", 24, "HR"), (3, "Charlie", 35, "IT")]`). Define un esquema explícito para este DataFrame.
    * Crea un DataFrame a partir de esta lista y el esquema.
    * Muestra el DataFrame y su esquema.

2.  **Lectura de CSV y Exploración**:
    * Descarga un archivo CSV público, como "data_sales.csv" (puedes buscarlo en Kaggle o crear uno simple con datos de ventas: `ID_Venta,Producto,Cantidad,Precio,Fecha,Ciudad`).
    * Carga este archivo CSV en un DataFrame, asegurándote de que el encabezado sea reconocido y el esquema sea inferido automáticamente.
    * Muestra las primeras 10 filas del DataFrame.
    * Imprime el esquema inferido.
    * Genera estadísticas descriptivas para el DataFrame y muéstralas.

3.  **Selección y Filtrado**:
    * Usando el DataFrame de ventas del ejercicio 2, selecciona las columnas `Producto`, `Cantidad` y `Precio`.
    * Filtra el DataFrame para mostrar solo las ventas donde la `Cantidad` sea mayor que 5.
    * Filtra el DataFrame para mostrar las ventas de "Producto_A" realizadas en la "Ciudad_X" (ajusta a tus datos de prueba).

4.  **Añadir y Modificar Columnas**:
    * En el DataFrame de ventas, añade una nueva columna llamada `Total_Venta` que sea el producto de `Cantidad` por `Precio`.
    * Modifica la columna `Producto` para que todos los nombres de los productos estén en mayúsculas.
    * Añade una columna llamada `Es_Gran_Venta` que sea "Sí" si `Total_Venta` es mayor que 100 y "No" en caso contrario.

5.  **Agregaciones**:
    * Calcula la `Cantidad` total vendida por cada `Producto`.
    * Encuentra el `Precio` promedio de los productos por cada `Ciudad`.
    * Determina el número de ventas (`ID_Venta` o conteo de filas) y el `Total_Venta` máximo por cada `Fecha`.

6.  **Uniones de DataFrames**:
    * Crea un segundo DataFrame llamado `df_productos` con la siguiente estructura: `ID_Producto, Nombre_Producto, Categoria` (ej. `[(1, "Laptop", "Electrónica"), (2, "Mouse", "Accesorios")]`). Asegúrate de que `Nombre_Producto` coincida con algunos nombres en tu DataFrame de ventas.
    * Une el DataFrame de ventas con el DataFrame de productos usando el `Producto` (o `Nombre_Producto`) como clave común.
    * Muestra las ventas junto con la categoría del producto.

7.  **Escritura de Datos y Modos**:
    * Guarda el DataFrame de ventas procesado (con `Total_Venta` y `Es_Gran_Venta`) en un nuevo directorio llamado `output/ventas_analisis_parquet` en formato Parquet, usando el modo `overwrite`.
    * Intenta guardar el mismo DataFrame en el mismo directorio usando el modo `errorIfExists`. Observa el error.
    * Cambia el modo a `ignore` y reintenta la operación.

8.  **Particionamiento de Salida**:
    * Utilizando el DataFrame de ventas procesado, guarda los datos en formato CSV en el directorio `output/ventas_particionadas_por_ciudad` y particiónalos por la columna `Ciudad`.
    * Verifica la estructura de directorios creada en `output/ventas_particionadas_por_ciudad`.
    * Carga solo los datos de una `Ciudad` específica (ej. "Madrid" o "Bogota") usando la ruta particionada y verifica que solo se carguen esas filas.

