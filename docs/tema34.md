# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.4. Manejo de Esquemas y Calidad de Datos

**Objetivo**:

Prevenir fallos estructurales y mantener la integridad de los datos mediante el diseño, control y validación de esquemas, así como la aplicación de técnicas de calidad y monitoreo de datos durante los procesos ETL.

**Introducción**:

En los sistemas de Big Data, los datos provienen de múltiples fuentes, formatos y estructuras. Asegurar que los esquemas de datos sean consistentes, flexibles ante cambios, y que los datos cumplan con criterios de calidad definidos, es esencial para garantizar flujos ETL robustos, escalables y seguros. Este tema aborda los conceptos y herramientas clave que permiten controlar la estructura de los datos y mitigar riesgos comunes como fallos por incompatibilidad de esquemas, errores de transformación o baja calidad de los datos.

**Desarrollo**:

El manejo de esquemas en flujos ETL implica definir reglas claras sobre la estructura de los datos que se procesan, ya sea de forma explícita (definida por el ingeniero de datos) o inferida automáticamente por herramientas como Apache Spark. A medida que los sistemas evolucionan, también lo hacen los esquemas, lo que obliga a gestionar adecuadamente su evolución sin interrumpir los pipelines. Además, es indispensable validar la calidad de los datos mediante reglas automáticas, controlar los errores y registrar las excepciones de transformación. Este tema presenta técnicas, herramientas y ejemplos prácticos para el manejo avanzado de esquemas y la calidad de datos en arquitecturas modernas.

### 3.4.1 Esquemas explícitos vs. inferidos

La definición del esquema de datos puede realizarse de manera **explícita** o ser **inferida automáticamente** por las herramientas de procesamiento como Spark. Cada enfoque tiene implicaciones directas sobre la robustez, flexibilidad y trazabilidad de los pipelines ETL.

##### Ventajas y riesgos de los esquemas explícitos

Un esquema explícito proporciona una estructura rigurosa y bien definida para los datos, lo que es esencial en entornos productivos y críticos donde la integridad es clave. Al establecer de antemano los tipos de datos, nombres de columnas y estructuras esperadas, se minimizan errores derivados de datos mal formateados o inconsistencias, permitiendo validaciones más estrictas y optimización en el procesamiento. Esto mejora el rendimiento en consultas y operaciones, ya que el motor de ejecución puede aprovechar la información estructural para optimizar los planes de ejecución. Además, facilita la interoperabilidad entre sistemas al garantizar que los datos siempre siguen un formato preestablecido.

Sin embargo, esta rigidez también puede representar desafíos. En entornos donde la flexibilidad es esencial, como procesamiento de datos semi-estructurados o ingestión de datos dinámicos, un esquema explícito puede ser limitante. Cambios en la estructura de los datos pueden requerir modificaciones en el esquema, lo que conlleva tiempo y esfuerzo de mantenimiento. Además, en grandes volúmenes de datos, definir un esquema fijo sin conocer completamente la naturaleza de los datos puede generar problemas de compatibilidad y pérdida de información. Por estas razones, en casos de exploración de datos, se puede preferir un enfoque más adaptable como el esquema inferido.

| Aspecto | Ventajas | Riesgos |
|---------|---------|---------|
| **Integridad y validación** | Garantiza datos consistentes y previene errores de formato | Puede bloquear datos que no se ajusten al esquema, limitando flexibilidad |
| **Optimización del rendimiento** | Mejora la velocidad de consultas y procesamiento al aprovechar la estructura definida | Puede generar sobrecarga en modificaciones o adaptaciones futuras |
| **Interoperabilidad** | Facilita integración con otros sistemas mediante formatos predefinidos | Requiere coordinación rigurosa para cambios y actualizaciones |
| **Adaptabilidad** | Ideal para datos estructurados y fuentes confiables | Poco adecuado para datos semi-estructurados o en evolución constante |
| **Mantenimiento** | Reduce la posibilidad de errores operacionales y facilita auditoría | Necesita cambios manuales si se modifica la fuente de datos |

Si el contexto demanda estabilidad y predictibilidad, un esquema explícito es una gran ventaja. Sin embargo, si el objetivo es manejar datos cambiantes o no estructurados, es recomendable evaluar alternativas más flexibles.

**Pipeline de Datos de IoT en Manufactura**

Garantiza que las alertas críticas de temperatura y presión siempre tengan tipos correctos, evitando fallos en sistemas de seguridad industrial.

```python
from pyspark.sql.types import *

# Esquema explícito para sensores industriales
sensor_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), False)
])

df_sensors = spark.read.schema(sensor_schema).json("hdfs://sensors/data/")
```

**Data Warehouse de E-commerce**

Evita que montos con formato incorrecto contaminen reportes financieros críticos.

```python
# Esquema para tabla de órdenes con validaciones estrictas
orders_schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("order_date", DateType(), False),
    StructField("total_amount", DecimalType(10,2), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), False)
])

# Cualquier registro que no cumpla el esquema será rechazado
df_orders = spark.read.schema(orders_schema).option("mode", "FAILFAST").parquet("s3://orders/")
```

**Pipeline de Logs de Aplicación**

Garantiza que campos críticos como timestamp y level siempre estén presentes para monitoreo y alertas.

```python
# Esquema para logs estructurados con campos obligatorios
log_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("level", StringType(), False),
    StructField("service", StringType(), False),
    StructField("message", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("duration_ms", IntegerType(), True)
])

df_logs = spark.readStream.schema(log_schema).json("kafka://logs-topic")
```

##### Ventajas y riesgos de los esquemas inferidos

Los esquemas inferidos ofrecen una solución flexible y ágil en escenarios donde la naturaleza de los datos puede variar o evolucionar con el tiempo. En entornos de exploración y prototipado, permiten cargar y procesar datos sin una definición estricta, lo que agiliza el desarrollo y facilita la integración con fuentes heterogéneas. Esta adaptabilidad es especialmente útil en sistemas que reciben datos de múltiples orígenes o formatos desconocidos, permitiendo ajustes automáticos sin intervención manual. Además, en arquitecturas de big data como PySpark, el esquema inferido puede mejorar la facilidad de uso al eliminar la necesidad de definir explícitamente cada estructura antes de su procesamiento.

Sin embargo, esta flexibilidad conlleva ciertos riesgos. La falta de control en la definición de los datos puede generar errores silenciosos si los valores cambian inesperadamente, afectando la integridad de los procesos downstream. Asimismo, si los datos presentan inconsistencias, el motor de ejecución podría inferir tipos incorrectos, generando problemas de compatibilidad y fallos difíciles de detectar. En sistemas de producción, depender de esquemas inferidos puede provocar ineficiencias al requerir validaciones posteriores y ajustes constantes. Por ello, es fundamental evaluar el contexto antes de optar por este enfoque, combinándolo con estrategias que mitiguen posibles inconvenientes.

| Aspecto | Ventajas | Riesgos |
|---------|---------|---------|
| **Flexibilidad** | Admite datos dinámicos sin definir esquemas rígidos | Puede derivar en inconsistencias si los datos cambian inesperadamente |
| **Rapidez de implementación** | Reduce la necesidad de configuración manual en entornos de exploración | Puede generar errores difíciles de detectar en sistemas críticos |
| **Interoperabilidad** | Facilita la ingestión de múltiples formatos sin restricciones previas | Menor control sobre el formato y calidad de los datos |
| **Adaptabilidad** | Ideal para prototipado y análisis de datos desconocidos | Puede ocasionar problemas de compatibilidad en procesos posteriores |
| **Mantenimiento** | Minimiza esfuerzo inicial en definición de datos | Puede generar costos adicionales de corrección y validación en producción |

Un esquema inferido puede ser invaluable para exploración y modelos de datos altamente cambiantes, pero es crucial establecer controles para minimizar errores ocultos. 

**Análisis de Redes Sociales**

Permite procesar datos de múltiples plataformas sociales sin definir esquemas específicos para cada una.

```python
# Esquema inferido para datos variables de APIs sociales
df_social = spark.read.option("multiline", "true").json("s3://social-data/*/")

# Los campos pueden variar según la plataforma (Twitter, Facebook, Instagram)
df_social.printSchema()  # Muestra la estructura inferida dinámicamente

# Manejo seguro de campos opcionales
df_processed = df_social.select(
    col("user_id"),
    col("timestamp"),
    col("text").alias("content"),
    col("likes").cast("int").alias("engagement_likes"),
    col("shares").cast("int").alias("engagement_shares")  # Puede no existir en todas las plataformas
)
```

**Migración de Bases de Datos Legacy**

Facilita la migración de sistemas legacy donde la documentación del esquema puede estar desactualizada o perdida.

```python
# Inferencia automática para tablas con esquemas desconocidos
df_legacy = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://legacy-db:3306/old_system") \
    .option("dbtable", "unknown_table") \
    .option("inferSchema", "true") \
    .load()

# Exploración rápida de estructura
df_legacy.describe().show()
df_legacy.dtypes  # Verificar tipos inferidos

# Transformación adaptativa
for col_name, col_type in df_legacy.dtypes:
    if col_type == 'string' and 'date' in col_name.lower():
        df_legacy = df_legacy.withColumn(col_name, to_date(col(col_name)))
```

**Prototipado de ML con Datos Externos**

Permite iniciar rápidamente experimentos de ML sin invertir tiempo en definición manual de esquemas.

```python
# Carga rápida para experimentación con datasets públicos
df_experiment = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3://public-datasets/kaggle-competition/")

# Análisis exploratorio inmediato
df_experiment.summary().show()

# Feature engineering rápido sin conocimiento previo del esquema
numeric_columns = [col_name for col_name, col_type in df_experiment.dtypes 
                  if col_type in ['int', 'double', 'float']]

df_scaled = df_experiment.select(
    *[col(c) for c in numeric_columns]
).fillna(0)
```

##### Enfoque Híbrido

El enfoque híbrido combina lo mejor de los esquemas explícitos e inferidos, equilibrando estabilidad y flexibilidad. La estrategia consiste en definir un esquema explícito para los aspectos críticos del procesamiento de datos, garantizando integridad y eficiencia en las consultas, mientras se permite la inferencia de ciertos campos menos estructurados o de datos con variabilidad impredecible. Esto facilita la compatibilidad con datos dinámicos sin comprometer la robustez del sistema. En entornos de big data y procesamiento distribuido como Spark, este enfoque puede aprovechar las ventajas de optimización y validación de los esquemas explícitos mientras mantiene adaptabilidad a cambios inesperados en los datos.

Para implementarlo, se pueden definir estructuras clave con esquemas preestablecidos y, al mismo tiempo, permitir la inferencia en áreas donde la predictibilidad no es esencial. Por ejemplo, en la ingestión de logs, los campos críticos como identificadores y fechas pueden tener tipos de datos definidos, mientras que los mensajes o metadatos pueden inferirse para mantener flexibilidad. Este modelo reduce el riesgo de errores silenciosos al mismo tiempo que permite eficiencia en exploración y expansión de datos, lo que lo convierte en una opción ideal para sistemas que necesitan adaptabilidad sin comprometer la estabilidad operativa.

```python
# Esquema base explícito para campos críticos
base_schema = StructType([
    StructField("id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("amount", DecimalType(10,2), False)
])

# Lectura con esquema parcial + inferencia para campos adicionales
df = spark.read.schema(base_schema) \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("data/transactions/")

# Manejo de campos adicionales inferidos
additional_fields = [c for c in df.columns if c not in ["id", "timestamp", "amount", "_corrupt_record"]]
```

Esta estrategia híbrida maximiza tanto la robustez como la flexibilidad del pipeline.

### 3.4.2 Schema evolution en Spark (Avro, Parquet, Delta)

La evolución de esquemas en Spark es una característica fundamental para manejar cambios en la estructura de datos sin afectar la compatibilidad con versiones anteriores. Esto es particularmente útil en sistemas de big data donde las fuentes de datos pueden cambiar con el tiempo, pero aún se requiere accesibilidad a datos históricos. Spark ofrece soporte para la evolución de esquemas en formatos como Avro, Parquet y Delta Lake, cada uno con su propia manera de gestionar modificaciones sin comprometer la integridad del sistema.

En Avro, la evolución de esquemas se basa en el concepto de compatibilidad mediante reglas predefinidas. Un esquema nuevo debe ser compatible con las versiones anteriores, lo que significa que los cambios pueden incluir la adición de nuevos campos opcionales, cambios en nombres de campos con alias, o reordenación de elementos sin afectar la legibilidad de los datos. Avro mantiene metadatos estructurados que permiten interpretar versiones antiguas de datos con esquemas más recientes, lo que facilita la interoperabilidad entre diferentes versiones de la misma estructura.

Parquet, por otro lado, soporta la evolución de esquemas principalmente mediante la adición de columnas. Debido a su estructura basada en columnas, Parquet permite agregar nuevos atributos sin alterar los datos existentes. Sin embargo, la modificación de tipos de datos o la eliminación de columnas puede generar incompatibilidades, lo que obliga a realizar transformaciones adicionales para garantizar que las aplicaciones sean capaces de leer los datos correctamente. La metadata de Parquet juega un papel clave en la compatibilidad, ya que almacena información detallada sobre las versiones de esquema utilizadas.

Delta Lake ofrece una evolución de esquemas más avanzada al permitir modificaciones como la adición o eliminación de columnas, cambios en tipos de datos y adaptaciones estructurales sin necesidad de reescribir por completo los datos almacenados. Esto se logra mediante la gestión de versiones dentro de un registro transaccional que mantiene la coherencia del esquema en diferentes momentos del tiempo. Con Delta Lake, Spark puede leer y escribir datos bajo múltiples versiones sin perder compatibilidad, ofreciendo una flexibilidad notable en entornos donde los datos evolucionan rápidamente.

Estos enfoques de evolución de esquemas en Spark proporcionan soluciones efectivas para manejar cambios estructurales sin afectar el acceso a datos históricos, lo que resulta esencial en sistemas que deben ser robustos frente a modificaciones sin comprometer la integridad ni la eficiencia del procesamiento.

##### Compatibilidad de evolución: adición, eliminación y cambios de tipo

La compatibilidad en la evolución de esquemas permite modificaciones estructurales en los datos sin afectar la capacidad de lectura de versiones previas, siempre que se respeten ciertas reglas de compatibilidad hacia atrás. Esto significa que es posible agregar nuevas columnas sin afectar consultas existentes, reordenar campos sin alterar su interpretación, e incluso cambiar tipos de datos si la transformación es segura (por ejemplo, de `int` a `bigint` o de `string` a `text`). Sin embargo, eliminaciones y cambios de tipo más drásticos pueden generar problemas de compatibilidad, por lo que deben manejarse con estrategias como versiones de esquema o transformaciones explícitas.

1. **Adición de columna en Avro:** Se agrega un nuevo campo opcional `email` a un esquema de usuarios, asegurando que versiones anteriores aún puedan interpretar los datos sin errores.
2. **Cambio de tipo en Parquet:** Se transforma una columna `edad` de `int` a `bigint` para admitir valores más grandes sin afectar consultas existentes.
3. **Eliminación en Delta Lake:** Se elimina una columna obsoleta `dirección` y los datos históricos siguen accesibles mediante versiones anteriores registradas en el log transaccional de Delta.  

Estos cambios reflejan cómo Spark maneja la evolución sin comprometer la estabilidad del sistema.

##### Herramientas para manejar la evolución de esquemas

Las herramientas de evolución de esquemas en Spark permiten manejar cambios estructurales sin comprometer la integridad de los datos. Parámetros como `spark.sql.parquet.mergeSchema=true` en Parquet y `--mergeSchema` en Delta Lake facilitan la integración de nuevas columnas o estructuras sin requerir una migración completa, manteniendo compatibilidad con versiones anteriores. En Avro, la evolución de esquemas mediante `AvroSchema` asegura que los datos históricos y actuales puedan coexistir sin afectar procesos analíticos.

1. Una plataforma de monitoreo ambiental activa `mergeSchema` en Delta para incorporar sensores adicionales sin interrupciones operativas.
2. Un equipo de ingeniería de datos usa Avro con evolución de esquemas para adaptar su pipeline de predicciones sin reescribir datos antiguos.
3. Una empresa de retail ajusta automáticamente su modelo Parquet para agregar atributos nuevos sobre tendencias de compra sin afectar reportes históricos.

Estas soluciones garantizan adaptabilidad y estabilidad en entornos dinámicos de big data.

### 3.4.3 Control de versiones de esquemas

En sistemas de procesamiento distribuido, el control de versiones de esquemas es un mecanismo clave para garantizar la trazabilidad, validación y compatibilidad de los datos a lo largo del tiempo. Esto es especialmente relevante en arquitecturas de big data, donde los datos evolucionan constantemente y deben seguir siendo accesibles sin interrupciones. Sin un sistema de versionado adecuado, los cambios en los esquemas pueden generar errores al momento de consumir o transformar datos, afectando la confiabilidad del sistema. 

Existen dos enfoques principales para gestionar la evolución de esquemas: el uso de **Schema Registry**, que permite registrar y validar los esquemas de manera centralizada, y el **versionado de tablas en Delta Lake**, que ofrece un historial de cambios con soporte para consultas en diferentes puntos del tiempo.

##### Uso de Schema Registry

Un **Schema Registry** es un servicio que almacena y gestiona las versiones de los esquemas de datos utilizados en un sistema. Soluciones como **Confluent Schema Registry** (para Apache Kafka) o **AWS Glue Schema Registry** permiten registrar, versionar y validar esquemas al momento de producir o consumir datos. Esto asegura que los productores de datos sigan un formato predefinido y que los consumidores puedan interpretar los datos correctamente, incluso si se han realizado cambios en la estructura.

El **Schema Registry** actúa como un intermediario que evita incompatibilidades entre diferentes versiones de datos. Al registrar un esquema, el sistema verifica que los cambios sean compatibles con versiones anteriores, permitiendo agregar nuevos campos opcionales sin afectar la lectura de datos históricos. Además, si un consumidor utiliza una versión anterior del esquema, el registro puede proporcionar un mecanismo de compatibilidad que transforma los datos para que sigan siendo accesibles.

**Ejemplo**: Supongamos que una empresa de telecomunicaciones utiliza Kafka para procesar registros de llamadas. Cada mensaje en Kafka sigue un esquema Avro gestionado por **Confluent Schema Registry**. Cuando se añade un nuevo campo `ubicación` a los datos de llamadas, el Schema Registry asegura que los consumidores existentes puedan seguir procesando los registros sin errores, manteniendo compatibilidad con versiones anteriores.

##### Versionado de tablas con Delta Lake

**Delta Lake** ofrece un sistema de versionado de esquemas incorporado en su arquitectura transaccional. A diferencia de otros formatos como Parquet o Avro, Delta Lake permite el **"time travel"**, es decir, la capacidad de consultar versiones previas de una tabla y revertir cambios si es necesario. Este mecanismo es esencial para garantizar la estabilidad en sistemas donde los datos cambian con frecuencia.

Las operaciones estructurales como **ADD COLUMN**, **CHANGE TYPE** y **DROP COLUMN** son registradas en el log de transacciones de Delta, lo que permite rastrear cómo evolucionó el esquema a lo largo del tiempo. Si un cambio genera problemas en los consumidores de datos, se pueden restaurar versiones anteriores sin afectar la integridad de los datos almacenados.

**Ejemplo**: Una startup que trabaja con datos de sensores usa Delta Lake para almacenar registros de mediciones. Para incorporar nuevas métricas sin afectar sistemas que dependen de datos históricos, habilitan `--mergeSchema` en Delta Lake. Así, los nuevos datos con métricas adicionales son integrados sin necesidad de realizar migraciones complejas y sin impactar consultas anteriores.

### 3.4.4 Validación y limpieza de datos

La validación y limpieza de datos son procesos fundamentales para garantizar que la información utilizada en sistemas analíticos o de negocio sea precisa y confiable. La validación implica la verificación de la integridad de los datos mediante reglas predefinidas, como la comprobación de formatos, rangos aceptables y valores nulos. Este proceso asegura que los datos ingresados cumplan con las expectativas y requisitos del sistema antes de ser utilizados en cálculos o reportes. Por otro lado, la limpieza de datos aborda problemas como valores duplicados, inconsistencias, errores tipográficos y registros incompletos, permitiendo transformar datos crudos en información estructurada y libre de anomalías. Sin estos mecanismos, los sistemas corren el riesgo de generar análisis erróneos, afectar la toma de decisiones y comprometer la confiabilidad de modelos predictivos.

Implementar una estrategia efectiva de validación y limpieza requiere el uso de herramientas especializadas, como reglas definidas en bases de datos, funciones en frameworks de procesamiento de datos como PySpark y SparkSQL, o soluciones avanzadas como Data Quality Services en entornos empresariales. La automatización de estos procesos es clave para mantener la escalabilidad y eficiencia, reduciendo la carga operativa en equipos de datos y garantizando que la información esté siempre en condiciones óptimas para su uso. Además, estrategias como la detección de valores atípicos, normalización de formatos y enriquecimiento de datos pueden mejorar la calidad de la información disponible y potenciar la capacidad de los sistemas para ofrecer insights precisos y relevantes.

##### Reglas de calidad: nulos, tipos, rangos, unicidad

Las reglas de calidad de datos son validaciones sistemáticas que garantizan que los datos cumplan con estándares específicos antes de ser procesados o almacenados. En Spark, estas reglas se pueden implementar como **filtros** (que excluyen registros no válidos) o como **excepciones** (que detienen el procesamiento cuando se detectan problemas).

**1. Reglas de Nulos**

Las reglas de nulos verifican que los campos críticos no contengan valores faltantes o que los campos opcionales manejen correctamente los valores nulos.

**Ejemplo PySpark**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, when, count

# Datos de ejemplo
data = [
    ("001", "Juan Pérez", 25, "juan@email.com"),
    ("002", None, 30, "ana@email.com"),
    ("003", "Carlos López", None, None),
    ("004", "María García", 28, "maria@email.com")
]

df = spark.createDataFrame(data, ["id", "nombre", "edad", "email"])

# Regla: nombre y email son obligatorios
df_clean = df.filter(
    col("nombre").isNotNull() & 
    col("email").isNotNull()
)

print("Registros válidos:")
df_clean.show()

# Registros rechazados para auditoría
df_rejected = df.filter(
    col("nombre").isNull() | 
    col("email").isNull()
)

print("Registros rechazados:")
df_rejected.show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear vista temporal
CREATE OR REPLACE TEMPORARY VIEW usuarios AS
SELECT * FROM VALUES 
    ('001', 'Juan Pérez', 25, 'juan@email.com'),
    ('002', NULL, 30, 'ana@email.com'),
    ('003', 'Carlos López', NULL, NULL),
    ('004', 'María García', 28, 'maria@email.com')
AS t(id, nombre, edad, email);

-- Filtrar registros válidos
SELECT * FROM usuarios 
WHERE nombre IS NOT NULL 
  AND email IS NOT NULL;

-- Contar registros con problemas de calidad
SELECT 
    COUNT(*) as total_registros,
    SUM(CASE WHEN nombre IS NULL THEN 1 ELSE 0 END) as nombres_nulos,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as emails_nulos
FROM usuarios;
```

**Ejemplo PySpark**:

```python
def validate_nulls(df, required_columns):
    """Valida que las columnas requeridas no tengan nulos"""
    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Columna '{col_name}' tiene {null_count} valores nulos")
    return df

# Aplicar validación estricta
try:
    df_validated = validate_nulls(df, ["nombre", "email"])
    print("Validación exitosa")
except ValueError as e:
    print(f"Error de calidad: {e}")
```

**2. Reglas de Tipos**

Las reglas de tipos verifican que los datos tengan el formato correcto según su tipo esperado (números, fechas, emails, etc.).

**Ejemplo PySpark**:

```python
from pyspark.sql.functions import regexp_match, length, when
from pyspark.sql.types import IntegerType

# Datos con problemas de tipos
data = [
    ("001", "25", "2023-12-01", "juan@email.com"),
    ("002", "treinta", "2023/12/02", "ana.email.com"),
    ("003", "28", "invalid_date", "carlos@email.com"),
    ("004", "-5", "2023-12-03", "maria@domain")
]

df = spark.createDataFrame(data, ["id", "edad_str", "fecha_str", "email"])

# Regla 1: Edad debe ser numérica y positiva
df_with_edad = df.withColumn(
    "edad_valida", 
    when(col("edad_str").rlike("^[0-9]+$") & 
         (col("edad_str").cast(IntegerType()) > 0), 
         col("edad_str").cast(IntegerType())
    ).otherwise(None)
)

# Regla 2: Email debe tener formato válido
df_with_email = df_with_edad.withColumn(
    "email_valido",
    when(col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
         col("email")
    ).otherwise(None)
)

# Regla 3: Fecha debe tener formato ISO
df_with_fecha = df_with_email.withColumn(
    "fecha_valida",
    when(col("fecha_str").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
         to_date(col("fecha_str"))
    ).otherwise(None)
)

# Filtrar solo registros completamente válidos
df_clean = df_with_fecha.filter(
    col("edad_valida").isNotNull() &
    col("email_valido").isNotNull() &
    col("fecha_valida").isNotNull()
)

df_clean.select("id", "edad_valida", "fecha_valida", "email_valido").show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear datos de prueba
CREATE OR REPLACE TEMPORARY VIEW datos_raw AS
SELECT * FROM VALUES 
    ('001', '25', '2023-12-01', 'juan@email.com'),
    ('002', 'treinta', '2023/12/02', 'ana.email.com'),
    ('003', '28', 'invalid_date', 'carlos@email.com'),
    ('004', '-5', '2023-12-03', 'maria@domain')
AS t(id, edad_str, fecha_str, email);

-- Validar tipos y aplicar reglas
SELECT 
    id,
    CASE 
        WHEN edad_str RLIKE '^[0-9]+$' AND CAST(edad_str AS INT) > 0 
        THEN CAST(edad_str AS INT)
        ELSE NULL 
    END as edad_valida,
    
    CASE 
        WHEN fecha_str RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
        THEN TO_DATE(fecha_str)
        ELSE NULL 
    END as fecha_valida,
    
    CASE 
        WHEN email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        THEN email
        ELSE NULL 
    END as email_valido
    
FROM datos_raw
WHERE edad_str RLIKE '^[0-9]+$' AND CAST(edad_str AS INT) > 0
  AND fecha_str RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$';
```

**3. Reglas de Rangos**

Las reglas de rangos verifican que los valores numéricos, fechas o strings estén dentro de límites aceptables.

**Ejemplo PySpark**:

```python
from pyspark.sql.functions import col, when, current_date, datediff
from datetime import datetime, date

# Datos de empleados
data = [
    ("001", "Juan", 25, 50000, "2020-01-15"),
    ("002", "Ana", 17, 30000, "2025-06-01"),  # Menor de edad
    ("003", "Carlos", 45, -5000, "2019-03-10"),  # Salario negativo
    ("004", "María", 150, 80000, "1800-12-25"),  # Edad imposible, fecha muy antigua
    ("005", "Luis", 30, 60000, "2022-05-20")
]

df = spark.createDataFrame(data, ["id", "nombre", "edad", "salario", "fecha_ingreso"])
df = df.withColumn("fecha_ingreso", to_date(col("fecha_ingreso")))

# Regla 1: Edad entre 18 y 65 años
df_edad_valida = df.withColumn(
    "edad_en_rango",
    when((col("edad") >= 18) & (col("edad") <= 65), True).otherwise(False)
)

# Regla 2: Salario positivo y menor a 200,000
df_salario_valido = df_edad_valida.withColumn(
    "salario_en_rango",
    when((col("salario") > 0) & (col("salario") <= 200000), True).otherwise(False)
)

# Regla 3: Fecha de ingreso no futura y no anterior a 2000
fecha_minima = date(2000, 1, 1)
df_fecha_valida = df_salario_valido.withColumn(
    "fecha_en_rango",
    when((col("fecha_ingreso") >= lit(fecha_minima)) & 
         (col("fecha_ingreso") <= current_date()), True).otherwise(False)
)

# Filtrar registros válidos
df_valid = df_fecha_valida.filter(
    col("edad_en_rango") & 
    col("salario_en_rango") & 
    col("fecha_en_rango")
)

print("Empleados válidos:")
df_valid.select("id", "nombre", "edad", "salario", "fecha_ingreso").show()

# Reporte de calidad
quality_report = df_fecha_valida.agg(
    count("*").alias("total_registros"),
    sum(when(col("edad_en_rango"), 1).otherwise(0)).alias("edad_valida"),
    sum(when(col("salario_en_rango"), 1).otherwise(0)).alias("salario_valido"),
    sum(when(col("fecha_en_rango"), 1).otherwise(0)).alias("fecha_valida")
)

quality_report.show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear datos de empleados
CREATE OR REPLACE TEMPORARY VIEW empleados AS
SELECT * FROM VALUES 
    ('001', 'Juan', 25, 50000, '2020-01-15'),
    ('002', 'Ana', 17, 30000, '2025-06-01'),
    ('003', 'Carlos', 45, -5000, '2019-03-10'),
    ('004', 'María', 150, 80000, '1800-12-25'),
    ('005', 'Luis', 30, 60000, '2022-05-20')
AS t(id, nombre, edad, salario, fecha_ingreso_str);

-- Aplicar reglas de rango
SELECT 
    id, nombre, edad, salario, 
    TO_DATE(fecha_ingreso_str) as fecha_ingreso,
    
    -- Validaciones de rango
    CASE WHEN edad BETWEEN 18 AND 65 THEN 'VÁLIDO' ELSE 'INVÁLIDO' END as edad_estado,
    CASE WHEN salario > 0 AND salario <= 200000 THEN 'VÁLIDO' ELSE 'INVÁLIDO' END as salario_estado,
    CASE WHEN TO_DATE(fecha_ingreso_str) BETWEEN '2000-01-01' AND CURRENT_DATE() 
         THEN 'VÁLIDO' ELSE 'INVÁLIDO' END as fecha_estado

FROM empleados
WHERE edad BETWEEN 18 AND 65
  AND salario > 0 AND salario <= 200000  
  AND TO_DATE(fecha_ingreso_str) BETWEEN '2000-01-01' AND CURRENT_DATE();

-- Estadísticas de calidad por rango
SELECT 
    COUNT(*) as total_empleados,
    SUM(CASE WHEN edad BETWEEN 18 AND 65 THEN 1 ELSE 0 END) as edad_valida,
    SUM(CASE WHEN salario > 0 AND salario <= 200000 THEN 1 ELSE 0 END) as salario_valido,
    SUM(CASE WHEN TO_DATE(fecha_ingreso_str) BETWEEN '2000-01-01' AND CURRENT_DATE() THEN 1 ELSE 0 END) as fecha_valida
FROM empleados;
```

**4. Reglas de Unicidad**

Las reglas de unicidad verifican que no existan duplicados en campos que deben ser únicos (IDs, emails, códigos, etc.).

**Ejemplo PySpark**:

```python
from pyspark.sql.functions import col, count, desc
from pyspark.sql.window import Window

# Datos con duplicados
data = [
    ("001", "juan@email.com", "12345678", "Juan Pérez"),
    ("002", "ana@email.com", "87654321", "Ana García"),
    ("003", "carlos@email.com", "12345678", "Carlos López"),  # DNI duplicado
    ("004", "juan@email.com", "11111111", "Juan Martínez"),   # Email duplicado
    ("005", "maria@email.com", "22222222", "María Rodríguez"),
    ("001", "pedro@email.com", "33333333", "Pedro Sánchez")   # ID duplicado
]

df = spark.createDataFrame(data, ["id", "email", "dni", "nombre"])

# Método 1: Identificar duplicados por campo
print("=== Análisis de Duplicados ===")

# Duplicados por ID
duplicados_id = df.groupBy("id").count().filter(col("count") > 1)
print("IDs duplicados:")
duplicados_id.show()

# Duplicados por Email
duplicados_email = df.groupBy("email").count().filter(col("count") > 1)
print("Emails duplicados:")
duplicados_email.show()

# Duplicados por DNI
duplicados_dni = df.groupBy("dni").count().filter(col("count") > 1)
print("DNIs duplicados:")
duplicados_dni.show()

# Método 2: Marcar registros duplicados
window_id = Window.partitionBy("id")
window_email = Window.partitionBy("email")
window_dni = Window.partitionBy("dni")

df_marked = df.withColumn("count_id", count("*").over(window_id)) \
              .withColumn("count_email", count("*").over(window_email)) \
              .withColumn("count_dni", count("*").over(window_dni))

# Identificar registros con algún tipo de duplicado
df_duplicates = df_marked.filter(
    (col("count_id") > 1) | 
    (col("count_email") > 1) | 
    (col("count_dni") > 1)
)

print("Registros con duplicados:")
df_duplicates.orderBy("id").show()

# Método 3: Mantener solo registros únicos (primera ocurrencia)
df_unique = df.dropDuplicates(["id", "email", "dni"])
print("Registros únicos:")
df_unique.show()

# Método 4: Validación estricta con excepción
def validate_uniqueness(df, unique_columns):
    """Valida que las columnas especificadas sean únicas"""
    for col_name in unique_columns:
        duplicate_count = df.groupBy(col_name).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            duplicates = df.groupBy(col_name).count().filter(col("count") > 1).collect()
            raise ValueError(f"Columna '{col_name}' tiene duplicados: {[row[col_name] for row in duplicates]}")
    return True

try:
    validate_uniqueness(df_unique, ["id", "email", "dni"])
    print("Validación de unicidad exitosa")
except ValueError as e:
    print(f"Error de unicidad: {e}")
```

**Ejemplo SparkSQL**:

```sql
-- Crear datos con duplicados
CREATE OR REPLACE TEMPORARY VIEW usuarios_duplicados AS
SELECT * FROM VALUES 
    ('001', 'juan@email.com', '12345678', 'Juan Pérez'),
    ('002', 'ana@email.com', '87654321', 'Ana García'),
    ('003', 'carlos@email.com', '12345678', 'Carlos López'),
    ('004', 'juan@email.com', '11111111', 'Juan Martínez'),
    ('005', 'maria@email.com', '22222222', 'María Rodríguez'),
    ('001', 'pedro@email.com', '33333333', 'Pedro Sánchez')
AS t(id, email, dni, nombre);

-- Análisis de duplicados por campo
SELECT 'ID' as campo, id as valor, COUNT(*) as cantidad
FROM usuarios_duplicados 
GROUP BY id 
HAVING COUNT(*) > 1

UNION ALL

SELECT 'EMAIL' as campo, email as valor, COUNT(*) as cantidad
FROM usuarios_duplicados 
GROUP BY email 
HAVING COUNT(*) > 1

UNION ALL

SELECT 'DNI' as campo, dni as valor, COUNT(*) as cantidad
FROM usuarios_duplicados 
GROUP BY dni 
HAVING COUNT(*) > 1;

-- Identificar registros duplicados con detalles
WITH duplicados_marcados AS (
    SELECT *,
        COUNT(*) OVER (PARTITION BY id) as count_id,
        COUNT(*) OVER (PARTITION BY email) as count_email,
        COUNT(*) OVER (PARTITION BY dni) as count_dni,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY nombre) as rn_id
    FROM usuarios_duplicados
)
SELECT 
    id, email, dni, nombre,
    CASE WHEN count_id > 1 THEN 'DUPLICADO' ELSE 'ÚNICO' END as estado_id,
    CASE WHEN count_email > 1 THEN 'DUPLICADO' ELSE 'ÚNICO' END as estado_email,
    CASE WHEN count_dni > 1 THEN 'DUPLICADO' ELSE 'ÚNICO' END as estado_dni
FROM duplicados_marcados
WHERE count_id > 1 OR count_email > 1 OR count_dni > 1
ORDER BY id;

-- Obtener registros únicos (primera ocurrencia por cada campo)
WITH ranked_records AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY nombre) as rn_id,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY nombre) as rn_email,
        ROW_NUMBER() OVER (PARTITION BY dni ORDER BY nombre) as rn_dni
    FROM usuarios_duplicados
)
SELECT id, email, dni, nombre
FROM ranked_records
WHERE rn_id = 1 AND rn_email = 1 AND rn_dni = 1;

-- Reporte de calidad de unicidad
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT id) as ids_unicos,
    COUNT(DISTINCT email) as emails_unicos,
    COUNT(DISTINCT dni) as dnis_unicos,
    COUNT(*) - COUNT(DISTINCT id) as duplicados_id,
    COUNT(*) - COUNT(DISTINCT email) as duplicados_email,
    COUNT(*) - COUNT(DISTINCT dni) as duplicados_dni
FROM usuarios_duplicados;
```

##### Pipeline Integrado de Calidad

**Ejemplo PySpark**:

```python
def apply_data_quality_rules(df):
    """Aplica todas las reglas de calidad de datos"""
    from pyspark.sql.functions import *
    
    # 1. Reglas de nulos
    df = df.filter(col("id").isNotNull() & col("email").isNotNull())
    
    # 2. Reglas de tipos  
    df = df.filter(col("edad").rlike("^[0-9]+$")) \
           .withColumn("edad", col("edad").cast("int"))
    
    # 3. Reglas de rangos
    df = df.filter((col("edad") >= 18) & (col("edad") <= 65))
    df = df.filter((col("salario") > 0) & (col("salario") <= 200000))
    
    # 4. Reglas de unicidad
    df = df.dropDuplicates(["id", "email"])
    
    return df

# Aplicar pipeline de calidad
df_clean = apply_data_quality_rules(df_raw)
```

**Ejemplo SparkSQL**:

```sql
-- Pipeline completo de calidad de datos
WITH quality_pipeline AS (
    SELECT *
    FROM raw_data
    WHERE 
        -- Reglas de nulos
        id IS NOT NULL AND email IS NOT NULL
        -- Reglas de tipos
        AND edad RLIKE '^[0-9]+$'
        -- Reglas de rangos  
        AND CAST(edad AS INT) BETWEEN 18 AND 65
        AND salario > 0 AND salario <= 200000
        -- Fecha válida
        AND fecha_ingreso BETWEEN '2000-01-01' AND CURRENT_DATE()
),
unique_records AS (
    SELECT DISTINCT *  -- Regla de unicidad básica
    FROM quality_pipeline
)
SELECT * FROM unique_records;
```

Estos ejemplos muestran cómo implementar sistemáticamente reglas de calidad de datos en Spark, tanto usando la API de Python como SparkSQL, proporcionando robustez y confiabilidad a los pipelines de datos.

##### Detección y manejo de duplicados

La detección y manejo de duplicados es crucial para mantener la integridad de los datos y evitar problemas como conteos erróneos, cargas incorrectas y análisis sesgados. En Spark, podemos implementar diferentes estrategias de detección usando claves primarias, combinaciones únicas o funciones hash.

**1. Detección por Claves Primarias**

Las claves primarias son identificadores únicos que no deben repetirse en un dataset. Su duplicación indica problemas serios de calidad de datos.

**Ejemplo PySpark**:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, first, last, max as spark_max, min as spark_min
from pyspark.sql.window import Window

# Datos de ejemplo con duplicados en clave primaria
data = [
    ("USR001", "Juan Pérez", "juan@email.com", "2023-01-15", 1000),
    ("USR002", "Ana García", "ana@email.com", "2023-01-16", 1500),
    ("USR001", "Juan Pérez", "juan.perez@email.com", "2023-01-17", 1200),  # ID duplicado
    ("USR003", "Carlos López", "carlos@email.com", "2023-01-18", 800),
    ("USR002", "Ana García", "ana.garcia@email.com", "2023-01-19", 1600),  # ID duplicado
    ("USR004", "María Rodríguez", "maria@email.com", "2023-01-20", 2000)
]

df = spark.createDataFrame(data, ["user_id", "nombre", "email", "fecha_registro", "saldo"])

# Detectar duplicados por clave primaria
print("=== Detección de Duplicados por Clave Primaria ===")

duplicados_pk = df.groupBy("user_id") \
                  .count() \
                  .filter(col("count") > 1) \
                  .orderBy(desc("count"))

print("Claves primarias duplicadas:")
duplicados_pk.show()

# Mostrar todos los registros duplicados
df_duplicados = df.join(duplicados_pk.select("user_id"), ["user_id"])
print("Registros con claves primarias duplicadas:")
df_duplicados.orderBy("user_id", "fecha_registro").show()

# Estrategia 1: Mantener el registro más reciente
window_spec = Window.partitionBy("user_id").orderBy(desc("fecha_registro"))
df_latest = df.withColumn("rn", row_number().over(window_spec)) \
              .filter(col("rn") == 1) \
              .drop("rn")

print("Registros únicos (más recientes):")
df_latest.orderBy("user_id").show()

# Estrategia 2: Mantener el registro con mayor saldo
window_spec_saldo = Window.partitionBy("user_id").orderBy(desc("saldo"))
df_max_saldo = df.withColumn("rn", row_number().over(window_spec_saldo)) \
                 .filter(col("rn") == 1) \
                 .drop("rn")

print("Registros únicos (mayor saldo):")
df_max_saldo.orderBy("user_id").show()

# Estrategia 3: Consolidar información de duplicados
df_consolidated = df.groupBy("user_id") \
                    .agg(
                        first("nombre").alias("nombre"),
                        first("email").alias("email_principal"),
                        spark_max("fecha_registro").alias("ultima_fecha"),
                        spark_max("saldo").alias("saldo_maximo"),
                        count("*").alias("num_registros")
                    )

print("Registros consolidados:")
df_consolidated.show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear tabla con duplicados en clave primaria
CREATE OR REPLACE TEMPORARY VIEW usuarios_duplicados AS
SELECT * FROM VALUES 
    ('USR001', 'Juan Pérez', 'juan@email.com', '2023-01-15', 1000),
    ('USR002', 'Ana García', 'ana@email.com', '2023-01-16', 1500),
    ('USR001', 'Juan Pérez', 'juan.perez@email.com', '2023-01-17', 1200),
    ('USR003', 'Carlos López', 'carlos@email.com', '2023-01-18', 800),
    ('USR002', 'Ana García', 'ana.garcia@email.com', '2023-01-19', 1600),
    ('USR004', 'María Rodríguez', 'maria@email.com', '2023-01-20', 2000)
AS t(user_id, nombre, email, fecha_registro, saldo);

-- Detectar duplicados por clave primaria
SELECT user_id, COUNT(*) as num_duplicados
FROM usuarios_duplicados
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY num_duplicados DESC;

-- Mostrar registros duplicados con detalles
WITH duplicados AS (
    SELECT user_id
    FROM usuarios_duplicados
    GROUP BY user_id
    HAVING COUNT(*) > 1
)
SELECT u.*, 'DUPLICADO' as estado
FROM usuarios_duplicados u
INNER JOIN duplicados d ON u.user_id = d.user_id
ORDER BY u.user_id, u.fecha_registro;

-- Estrategia 1: Mantener registro más reciente
WITH ranked_records AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY fecha_registro DESC) as rn
    FROM usuarios_duplicados
)
SELECT user_id, nombre, email, fecha_registro, saldo
FROM ranked_records
WHERE rn = 1
ORDER BY user_id;

-- Estrategia 2: Consolidar información
SELECT 
    user_id,
    FIRST(nombre) as nombre,
    FIRST(email) as email_principal,
    MAX(fecha_registro) as ultima_fecha,
    MAX(saldo) as saldo_maximo,
    COUNT(*) as num_registros_originales
FROM usuarios_duplicados
GROUP BY user_id
ORDER BY user_id;
```

**2. Detección por Combinaciones Únicas**

Las combinaciones únicas involucran múltiples campos que juntos deben ser únicos, como combinaciones de nombre+email, producto+fecha, etc.

**Ejemplo PySpark**:

```python
from pyspark.sql.functions import concat_ws, md5, col, count, desc, collect_list

# Datos de transacciones con duplicados
data = [
    ("TXN001", "USR001", "2023-01-15", "COMPRA", 100.50, "Producto A"),
    ("TXN002", "USR002", "2023-01-15", "COMPRA", 200.00, "Producto B"),
    ("TXN003", "USR001", "2023-01-15", "COMPRA", 100.50, "Producto A"),  # Duplicado potencial
    ("TXN004", "USR003", "2023-01-16", "VENTA", 150.00, "Producto C"),
    ("TXN005", "USR002", "2023-01-15", "COMPRA", 200.00, "Producto B"),  # Duplicado potencial
    ("TXN006", "USR001", "2023-01-17", "COMPRA", 100.50, "Producto A")   # Mismo usuario y producto, fecha diferente
]

df = spark.createDataFrame(data, ["txn_id", "user_id", "fecha", "tipo", "monto", "producto"])

print("=== Detección por Combinaciones Únicas ===")

# Combinación 1: user_id + fecha + producto (transacciones idénticas)
duplicados_combo1 = df.groupBy("user_id", "fecha", "producto") \
                      .agg(count("*").alias("count"), 
                           collect_list("txn_id").alias("txn_ids")) \
                      .filter(col("count") > 1)

print("Duplicados por usuario + fecha + producto:")
duplicados_combo1.show(truncate=False)

# Combinación 2: user_id + tipo + monto (transacciones similares)
duplicados_combo2 = df.groupBy("user_id", "tipo", "monto") \
                      .agg(count("*").alias("count"),
                           collect_list("txn_id").alias("txn_ids"),
                           collect_list("fecha").alias("fechas")) \
                      .filter(col("count") > 1)

print("Duplicados por usuario + tipo + monto:")
duplicados_combo2.show(truncate=False)

# Crear clave compuesta para análisis
df_with_key = df.withColumn("clave_compuesta", 
                           concat_ws("|", col("user_id"), col("fecha"), col("producto")))

# Detectar duplicados exactos
duplicados_exactos = df_with_key.groupBy("clave_compuesta") \
                                .agg(count("*").alias("count"),
                                     collect_list("txn_id").alias("txn_ids")) \
                                .filter(col("count") > 1)

print("Duplicados exactos (clave compuesta):")
duplicados_exactos.show(truncate=False)

# Estrategia: Eliminar duplicados manteniendo el primer registro
df_sin_duplicados = df.dropDuplicates(["user_id", "fecha", "producto"])
print("Registros sin duplicados:")
df_sin_duplicados.orderBy("user_id", "fecha").show()

# Estrategia: Marcar duplicados para auditoría
window_spec = Window.partitionBy("user_id", "fecha", "producto").orderBy("txn_id")
df_marked = df.withColumn("es_duplicado", 
                         when(row_number().over(window_spec) > 1, True).otherwise(False))

print("Registros marcados (duplicados identificados):")
df_marked.orderBy("user_id", "fecha", "txn_id").show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear tabla de transacciones
CREATE OR REPLACE TEMPORARY VIEW transacciones AS
SELECT * FROM VALUES 
    ('TXN001', 'USR001', '2023-01-15', 'COMPRA', 100.50, 'Producto A'),
    ('TXN002', 'USR002', '2023-01-15', 'COMPRA', 200.00, 'Producto B'),
    ('TXN003', 'USR001', '2023-01-15', 'COMPRA', 100.50, 'Producto A'),
    ('TXN004', 'USR003', '2023-01-16', 'VENTA', 150.00, 'Producto C'),
    ('TXN005', 'USR002', '2023-01-15', 'COMPRA', 200.00, 'Producto B'),
    ('TXN006', 'USR001', '2023-01-17', 'COMPRA', 100.50, 'Producto A')
AS t(txn_id, user_id, fecha, tipo, monto, producto);

-- Detectar duplicados por combinaciones únicas
SELECT 
    user_id, fecha, producto, tipo, monto,
    COUNT(*) as num_duplicados,
    COLLECT_LIST(txn_id) as txn_ids
FROM transacciones
GROUP BY user_id, fecha, producto, tipo, monto
HAVING COUNT(*) > 1
ORDER BY num_duplicados DESC;

-- Análisis de duplicados con detalles
WITH duplicados_identificados AS (
    SELECT user_id, fecha, producto,
        COUNT(*) as count_duplicados
    FROM transacciones
    GROUP BY user_id, fecha, producto
    HAVING COUNT(*) > 1
)
SELECT 
    t.*,
    'DUPLICADO' as estado,
    ROW_NUMBER() OVER (PARTITION BY t.user_id, t.fecha, t.producto ORDER BY t.txn_id) as orden_duplicado
FROM transacciones t
INNER JOIN duplicados_identificados d 
    ON t.user_id = d.user_id 
    AND t.fecha = d.fecha 
    AND t.producto = d.producto
ORDER BY t.user_id, t.fecha, t.producto, t.txn_id;

-- Eliminar duplicados manteniendo el primero
WITH ranked_transactions AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id, fecha, producto ORDER BY txn_id) as rn
    FROM transacciones
)
SELECT txn_id, user_id, fecha, tipo, monto, producto
FROM ranked_transactions
WHERE rn = 1
ORDER BY user_id, fecha;

-- Crear clave compuesta para análisis
SELECT 
    *,
    CONCAT(user_id, '|', fecha, '|', producto) as clave_compuesta,
    COUNT(*) OVER (PARTITION BY user_id, fecha, producto) as count_grupo
FROM transacciones
ORDER BY clave_compuesta;
```

**3. Detección por Funciones Hash**

Las funciones hash permiten detectar duplicados de manera eficiente, especialmente útil para registros con muchos campos o contenido textual.

**Ejemplo PySpark**:

```python
from pyspark.sql.functions import md5, sha1, sha2, concat_ws, col, count, collect_set

# Datos de documentos con contenido similar
data = [
    ("DOC001", "Contrato de Servicios", "Este es un contrato para servicios de consultoría", "2023-01-15", "Juan Pérez"),
    ("DOC002", "Propuesta Comercial", "Propuesta para desarrollo de software", "2023-01-16", "Ana García"),
    ("DOC003", "Contrato de Servicios", "Este es un contrato para servicios de consultoría", "2023-01-17", "Carlos López"),  # Contenido duplicado
    ("DOC004", "Manual de Usuario", "Guía completa para el uso del sistema", "2023-01-18", "María Rodríguez"),
    ("DOC005", "Propuesta Comercial", "Propuesta para desarrollo de software", "2023-01-19", "Luis Martín"),  # Contenido duplicado
    ("DOC006", "Contrato Modificado", "Este es un contrato para servicios de consultoría especializada", "2023-01-20", "Pedro Sánchez")  # Contenido similar
]

df = spark.createDataFrame(data, ["doc_id", "titulo", "contenido", "fecha", "autor"])

print("=== Detección por Funciones Hash ===")

# Hash del contenido completo
df_with_hash = df.withColumn("hash_contenido", md5(col("contenido"))) \
                 .withColumn("hash_titulo", md5(col("titulo"))) \
                 .withColumn("hash_completo", md5(concat_ws("|", col("titulo"), col("contenido"))))

# Detectar duplicados exactos por contenido
duplicados_contenido = df_with_hash.groupBy("hash_contenido") \
                                   .agg(count("*").alias("count"),
                                        collect_set("doc_id").alias("doc_ids"),
                                        first("contenido").alias("contenido_ejemplo")) \
                                   .filter(col("count") > 1)

print("Duplicados por contenido (hash MD5):")
duplicados_contenido.show(truncate=False)

# Detectar duplicados por título
duplicados_titulo = df_with_hash.groupBy("hash_titulo") \
                                .agg(count("*").alias("count"),
                                     collect_set("doc_id").alias("doc_ids"),
                                     first("titulo").alias("titulo_ejemplo")) \
                                .filter(col("count") > 1)

print("Duplicados por título:")
duplicados_titulo.show(truncate=False)

# Hash de múltiples algoritmos para comparación
df_multi_hash = df.withColumn("md5_hash", md5(col("contenido"))) \
                  .withColumn("sha1_hash", sha1(col("contenido"))) \
                  .withColumn("sha256_hash", sha2(col("contenido"), 256))

print("Hashes múltiples:")
df_multi_hash.select("doc_id", "titulo", "md5_hash", "sha1_hash").show(truncate=False)

# Detectar duplicados con diferentes criterios de hash
print("Análisis de duplicados por diferentes hash:")
df_multi_hash.groupBy("md5_hash") \
             .agg(count("*").alias("count_md5"),
                  collect_set("doc_id").alias("docs_md5")) \
             .filter(col("count_md5") > 1) \
             .show(truncate=False)

# Estrategia: Eliminar duplicados basado en hash
df_unique_content = df_with_hash.dropDuplicates(["hash_contenido"])
print("Documentos únicos por contenido:")
df_unique_content.select("doc_id", "titulo", "autor", "fecha").show()

# Estrategia: Crear registro de duplicados para auditoría
window_hash = Window.partitionBy("hash_contenido").orderBy("fecha")
df_audit = df_with_hash.withColumn("es_original", 
                                  when(row_number().over(window_hash) == 1, True).otherwise(False)) \
                       .withColumn("orden_duplicado", row_number().over(window_hash))

print("Auditoría de duplicados:")
df_audit.select("doc_id", "titulo", "autor", "fecha", "es_original", "orden_duplicado").show()
```

**Ejemplo SparkSQL**:

```sql
-- Crear tabla de documentos
CREATE OR REPLACE TEMPORARY VIEW documentos AS
SELECT * FROM VALUES 
    ('DOC001', 'Contrato de Servicios', 'Este es un contrato para servicios de consultoría', '2023-01-15', 'Juan Pérez'),
    ('DOC002', 'Propuesta Comercial', 'Propuesta para desarrollo de software', '2023-01-16', 'Ana García'),
    ('DOC003', 'Contrato de Servicios', 'Este es un contrato para servicios de consultoría', '2023-01-17', 'Carlos López'),
    ('DOC004', 'Manual de Usuario', 'Guía completa para el uso del sistema', '2023-01-18', 'María Rodríguez'),
    ('DOC005', 'Propuesta Comercial', 'Propuesta para desarrollo de software', '2023-01-19', 'Luis Martín'),
    ('DOC006', 'Contrato Modificado', 'Este es un contrato para servicios de consultoría especializada', '2023-01-20', 'Pedro Sánchez')
AS t(doc_id, titulo, contenido, fecha, autor);

-- Crear hashes para detección de duplicados
CREATE OR REPLACE TEMPORARY VIEW documentos_hash AS
SELECT *,
    MD5(contenido) as hash_contenido,
    MD5(titulo) as hash_titulo,
    MD5(CONCAT(titulo, '|', contenido)) as hash_completo,
    SHA1(contenido) as sha1_contenido,
    SHA2(contenido, 256) as sha256_contenido
FROM documentos;

-- Detectar duplicados por contenido
SELECT 
    hash_contenido,
    COUNT(*) as num_duplicados,
    COLLECT_SET(doc_id) as documentos_duplicados,
    FIRST(contenido) as contenido_ejemplo
FROM documentos_hash
GROUP BY hash_contenido
HAVING COUNT(*) > 1
ORDER BY num_duplicados DESC;

-- Detectar duplicados por título
SELECT 
    hash_titulo,
    COUNT(*) as num_duplicados,
    COLLECT_SET(doc_id) as documentos_duplicados,
    FIRST(titulo) as titulo_ejemplo
FROM documentos_hash
GROUP BY hash_titulo
HAVING COUNT(*) > 1;

-- Análisis comparativo de diferentes algoritmos hash
SELECT 
    doc_id, titulo, autor,
    hash_contenido as md5_hash,
    sha1_contenido,
    SUBSTR(sha256_contenido, 1, 16) as sha256_preview
FROM documentos_hash
ORDER BY hash_contenido;

-- Eliminar duplicados manteniendo el documento más antiguo
WITH ranked_docs AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY hash_contenido ORDER BY fecha ASC) as rn
    FROM documentos_hash
)
SELECT doc_id, titulo, contenido, fecha, autor
FROM ranked_docs
WHERE rn = 1
ORDER BY fecha;

-- Crear reporte de auditoría de duplicados
WITH duplicate_analysis AS (
    SELECT 
        doc_id, titulo, autor, fecha, hash_contenido,
        COUNT(*) OVER (PARTITION BY hash_contenido) as total_duplicados,
        ROW_NUMBER() OVER (PARTITION BY hash_contenido ORDER BY fecha) as orden_cronologico
    FROM documentos_hash
)
SELECT 
    doc_id, titulo, autor, fecha,
    CASE 
        WHEN total_duplicados = 1 THEN 'ÚNICO'
        WHEN orden_cronologico = 1 THEN 'ORIGINAL'
        ELSE 'DUPLICADO'
    END as estado_documento,
    total_duplicados,
    orden_cronologico
FROM duplicate_analysis
ORDER BY hash_contenido, orden_cronologico;
```

**4. Estrategias Avanzadas de Manejo de Duplicados**

**Ejemplo PySpark**:

```python
def comprehensive_duplicate_detection(df, primary_key_cols, unique_combination_cols, content_cols):
    """
    Pipeline completo para detección y manejo de duplicados
    """
    from pyspark.sql.functions import *
    
    print("=== Pipeline Completo de Detección de Duplicados ===")
    
    # 1. Duplicados por clave primaria
    if primary_key_cols:
        pk_duplicates = df.groupBy(*primary_key_cols).count().filter(col("count") > 1)
        pk_count = pk_duplicates.count()
        print(f"Duplicados por clave primaria: {pk_count}")
    
    # 2. Duplicados por combinación única
    if unique_combination_cols:
        combo_duplicates = df.groupBy(*unique_combination_cols).count().filter(col("count") > 1)
        combo_count = combo_duplicates.count()
        print(f"Duplicados por combinación única: {combo_count}")
    
    # 3. Duplicados por contenido (hash)
    if content_cols:
        content_hash = concat_ws("|", *[col(c) for c in content_cols])
        df_hash = df.withColumn("content_hash", md5(content_hash))
        hash_duplicates = df_hash.groupBy("content_hash").count().filter(col("count") > 1)
        hash_count = hash_duplicates.count()
        print(f"Duplicados por contenido: {hash_count}")
    
    # 4. Crear dataset limpio con múltiples estrategias
    df_clean = df
    
    # Eliminar duplicados por clave primaria (mantener más reciente si hay fecha)
    if primary_key_cols and "fecha" in df.columns:
        window_pk = Window.partitionBy(*primary_key_cols).orderBy(desc("fecha"))
        df_clean = df_clean.withColumn("rn_pk", row_number().over(window_pk)) \
                          .filter(col("rn_pk") == 1).drop("rn_pk")
    
    # Eliminar duplicados por combinación única
    if unique_combination_cols:
        df_clean = df_clean.dropDuplicates(unique_combination_cols)
    
    # Eliminar duplicados por contenido
    if content_cols:
        content_hash = concat_ws("|", *[col(c) for c in content_cols])
        df_clean = df_clean.withColumn("content_hash", md5(content_hash)) \
                          .dropDuplicates(["content_hash"]) \
                          .drop("content_hash")
    
    print(f"Registros originales: {df.count()}")
    print(f"Registros después de limpiar: {df_clean.count()}")
    print(f"Registros eliminados: {df.count() - df_clean.count()}")
    
    return df_clean

# Aplicar pipeline completo
df_clean = comprehensive_duplicate_detection(
    df_original,
    primary_key_cols=["doc_id"],
    unique_combination_cols=["titulo", "autor"],
    content_cols=["contenido"]
)
```

**Ejemplo SparkSQL**:

```sql
-- Reporte completo de análisis de duplicados
WITH duplicate_summary AS (
    -- Duplicados por clave primaria
    SELECT 'PRIMARY_KEY' as tipo_duplicado, 
           COUNT(*) as grupos_duplicados,
           SUM(count_duplicados - 1) as registros_duplicados
    FROM (
        SELECT doc_id, COUNT(*) as count_duplicados
        FROM documentos
        GROUP BY doc_id
        HAVING COUNT(*) > 1
    )
    
    UNION ALL
    
    -- Duplicados por combinación única
    SELECT 'UNIQUE_COMBINATION' as tipo_duplicado,
           COUNT(*) as grupos_duplicados,
           SUM(count_duplicados - 1) as registros_duplicados
    FROM (
        SELECT titulo, autor, COUNT(*) as count_duplicados
        FROM documentos
        GROUP BY titulo, autor
        HAVING COUNT(*) > 1
    )
    
    UNION ALL
    
    -- Duplicados por contenido
    SELECT 'CONTENT_HASH' as tipo_duplicado,
           COUNT(*) as grupos_duplicados,
           SUM(count_duplicados - 1) as registros_duplicados
    FROM (
        SELECT MD5(contenido) as hash_contenido, COUNT(*) as count_duplicados
        FROM documentos
        GROUP BY MD5(contenido)
        HAVING COUNT(*) > 1
    )
),
total_records AS (
    SELECT COUNT(*) as total_registros FROM documentos
)
SELECT 
    ds.*,
    ROUND((ds.registros_duplicados * 100.0) / tr.total_registros, 2) as porcentaje_duplicados
FROM duplicate_summary ds
CROSS JOIN total_records tr
ORDER BY ds.registros_duplicados DESC;

-- Pipeline de limpieza completo
CREATE OR REPLACE TEMPORARY VIEW documentos_limpios AS
WITH paso1_pk_clean AS (
    -- Eliminar duplicados por clave primaria (mantener más reciente)
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY fecha DESC) as rn
        FROM documentos
    ) WHERE rn = 1
),
paso2_combo_clean AS (
    -- Eliminar duplicados por combinación única
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY titulo, autor ORDER BY fecha ASC) as rn
        FROM paso1_pk_clean
    ) WHERE rn = 1
),
paso3_content_clean AS (
    -- Eliminar duplicados por contenido
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY MD5(contenido) ORDER BY fecha ASC) as rn
        FROM paso2_combo_clean
    ) WHERE rn = 1
)
SELECT doc_id, titulo, contenido, fecha, autor
FROM paso3_content_clean;

-- Verificar resultado final
SELECT 
    'ORIGINAL' as dataset, COUNT(*) as total_registros
FROM documentos
UNION ALL
SELECT 
    'LIMPIO' as dataset, COUNT(*) as total_registros  
FROM documentos_limpios;
```

**5. Mejores Prácticas para Manejo de Duplicados**

**Estrategia de Detección Progresiva**:

```python
def progressive_duplicate_detection(df):
    """Detección progresiva de duplicados con diferentes niveles de strictness"""
    
    # Nivel 1: Duplicados exactos (más estricto)
    exact_duplicates = df.groupBy(*df.columns).count().filter(col("count") > 1)
    
    # Nivel 2: Duplicados por campos clave
    key_duplicates = df.groupBy("id", "email").count().filter(col("count") > 1)
    
    # Nivel 3: Duplicados por similitud (menos estricto)
    content_duplicates = df.withColumn("content_hash", md5(col("content"))) \
                          .groupBy("content_hash").count().filter(col("count") > 1)
    
    return {
        "exact": exact_duplicates.count(),
        "key": key_duplicates.count(), 
        "content": content_duplicates.count()
    }
```

**Audit Trail de Duplicados**:

```python
def create_duplicate_audit_trail(df):
    """Crear rastro de auditoría para duplicados eliminados"""
    
    # Identificar duplicados antes de eliminar
    duplicates_info = df.groupBy("id").agg(
        count("*").alias("count_duplicates"),
        collect_list(struct(*df.columns)).alias("all_versions")
    ).filter(col("count_duplicates") > 1)
    
    # Guardar información de auditoría
    duplicates_info.write.mode("overwrite").parquet("audit/duplicates_removed/")
    
    return duplicates_info
```

Estas estrategias permiten un manejo robusto y trazable de duplicados, garantizando la integridad de los datos mientras se mantiene un registro completo de las transformaciones realizadas.

### 3.4.5 Logging de errores y manejo de excepciones durante la transformación

El registro y manejo de errores durante la transformación de datos es fundamental para garantizar la estabilidad y confiabilidad de un pipeline. Sin un sistema de logging adecuado, los fallos pueden pasar desapercibidos, generando inconsistencias en los datos procesados y afectando decisiones estratégicas. Un enfoque bien estructurado permite detectar y solucionar problemas rápidamente, minimizando el impacto en los sistemas y asegurando una trazabilidad clara para análisis posteriores. Además, integrar estrategias de recuperación evita interrupciones innecesarias y mejora la resiliencia del sistema ante fallos inesperados.  

#### Logging estructurado de errores  

Un sistema de **logging estructurado** facilita la identificación y resolución de problemas al proporcionar información detallada sobre el contexto del fallo. En lugar de registrar mensajes genéricos, se recomienda incluir detalles como el identificador de la transformación, el tipo de error, el origen de los datos y los valores específicos que causaron la falla. Esto permite a los ingenieros de datos rastrear con precisión el origen del problema y aplicar correcciones eficientes.  

Por ejemplo, si una conversión de tipos en PySpark falla debido a un valor inesperado en una columna, el sistema de logging debería registrar el nombre de la columna, el valor conflictivo y la operación en la que ocurrió el fallo. Este enfoque es crucial para evitar diagnósticos erróneos y reducir el tiempo de respuesta ante incidentes. Herramientas como **log4j** en Spark o sistemas centralizados como **Elastic Stack** pueden almacenar logs estructurados con niveles de severidad y trazabilidad para análisis en tiempo real.  

#### Estrategias de recuperación y manejo de excepciones  

En lugar de detener todo el pipeline por un error inesperado, es recomendable implementar técnicas de **manejo de excepciones** que permitan continuar con el procesamiento sin comprometer la calidad de los datos. Una estrategia común es el uso de bloques **try-catch**, que capturan errores específicos y permiten definir acciones correctivas.  

Otra técnica es la implementación de **rutas alternas**, donde los registros problemáticos se redirigen a un área de revisión sin afectar el flujo principal del procesamiento. Esto es útil en sistemas que requieren alta disponibilidad, como plataformas de análisis en tiempo real. Además, marcar registros inválidos con etiquetas específicas en lugar de eliminarlos permite que los analistas revisen y corrijan problemas sin perder información valiosa.  

Por ejemplo, en un pipeline de Spark, los datos con valores inconsistentes pueden enviarse a una tabla de auditoría en Delta Lake, donde se almacenan con información adicional sobre el error para su posterior revisión. Esto no solo mejora la calidad del procesamiento, sino que también facilita la corrección proactiva de datos y el aprendizaje sobre patrones de fallos recurrentes.  

## Tarea

1. Implementa un esquema explícito en PySpark para una tabla de pedidos. Agrega validaciones para tipos y valores.
2. Simula la evolución de esquema en una tabla Delta agregando columnas nuevas, eliminando otras y cambiando tipos. Evalúa los efectos.
3. Diseña un proceso de control de calidad que incluya detección de duplicados y validación de rangos para un conjunto de datos de clientes.
4. Implementa un log estructurado de errores en un flujo Spark. Incluye tipo de error, registro afectado y timestamp.
5. Integra un esquema en Avro a un Schema Registry local o en la nube. Genera dos versiones del esquema y simula un caso de incompatibilidad.

