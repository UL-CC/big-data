# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.1. Diseño y Orquestación de Pipelines ETL

**Objetivo**:

Comprender la estructura lógica y la gestión de la ejecución de flujos de datos mediante el diseño y la orquestación de pipelines ETL eficientes. El estudiante será capaz de aplicar principios fundamentales del proceso ETL, diferenciar entre ETL y ELT, diseñar pipelines desacoplados y escalables, e introducirse en herramientas de orquestación como Apache Airflow para la coordinación de tareas distribuidas en el tiempo.

**Introducción**:

Los pipelines ETL son la columna vertebral de cualquier solución de integración de datos en el ecosistema Big Data. Desde la recopilación de datos de múltiples fuentes hasta su transformación y carga en sistemas de almacenamiento analíticos, su diseño debe ser eficiente, escalable y mantenible. Además, la orquestación adecuada permite automatizar estos procesos, garantizando su ejecución en el orden correcto y en el momento adecuado.

**Desarrollo**:

Este tema explora los fundamentos de los procesos ETL (Extracción, Transformación y Carga), sus variantes como ELT, y las decisiones arquitectónicas que conllevan. Se introduce el concepto de diseño modular para facilitar la reutilización y el mantenimiento de componentes. Finalmente, se estudian herramientas como Apache Airflow, que permiten orquestar tareas complejas y distribuidas a través de la definición de flujos de trabajo (DAGs) y la gestión de dependencias y programación.

¡Claro que sí! Aquí tienes una versión más completa y clara del sub-tema 3.1.1, con ejemplos detallados y adicionales para cada etapa del proceso ETL:

### 3.1.1 Principios y etapas del proceso ETL

El **proceso ETL** (Extracción, Transformación y Carga) es una metodología fundamental en el mundo del Big Data para la integración de datos. Su objetivo principal es mover datos de diversas fuentes a un sistema de destino, como un data warehouse o un data lake, asegurando su **calidad, consistencia y disponibilidad** para análisis y toma de decisiones. Este proceso se divide en tres etapas principales:

##### Extracción de datos

La **extracción** es la primera fase del proceso ETL y consiste en la recolección de datos desde sus fuentes originales para llevarlos a un área de **staging** o procesamiento inicial. Esta área temporal es crucial, ya que permite trabajar con los datos sin afectar el rendimiento de las fuentes de origen. Las fuentes pueden ser muy variadas y complejas, desde bases de datos relacionales hasta sensores en tiempo real.

1.  **Extracción desde bases de datos relacionales (usando JDBC)**:
    * **Escenario**: Necesitas extraer los datos de ventas diarias de una base de datos de producción (por ejemplo, PostgreSQL o MySQL) para un informe de ventas.
    * **Proceso**: Se utiliza una conexión **JDBC (Java Database Connectivity)** para establecer comunicación con la base de datos. Puedes ejecutar consultas SQL complejas para seleccionar solo los datos relevantes, aplicar filtros por fecha o estado, y unirlos con otras tablas si es necesario. Por ejemplo, extraer solo las transacciones del último día con estado "completado" y que superen un cierto monto.
    * **Ejemplo**: Extraer datos de clientes de una base de datos Oracle, seleccionando aquellos que han realizado compras en los últimos 6 meses y excluyendo los clientes inactivos. Se podría usar una consulta como `SELECT * FROM clientes WHERE ultima_compra >= CURRENT_DATE - INTERVAL '6 months' AND estado = 'activo';`.

2.  **Recolección de datos de APIs REST con autenticación OAuth2**:
    * **Escenario**: Quieres obtener datos de interacción de usuarios de una plataforma de redes sociales o un sistema de CRM externo que expone sus datos a través de una API REST.
    * **Proceso**: Para acceder a estas APIs, a menudo se requiere **autenticación OAuth2**. Esto implica un flujo donde tu aplicación solicita un token de acceso al servidor de autorización después de que el usuario (o la aplicación misma) ha otorgado los permisos necesarios. Con este token, puedes realizar solicitudes HTTP (GET, POST) a los endpoints de la API para recuperar los datos. Es común manejar la paginación y los límites de tasa (rate limiting) de la API para asegurar una extracción completa y eficiente.
    * **Ejemplo**: Extraer datos de órdenes de una plataforma de comercio electrónico como Shopify o Stripe. Se configura la aplicación para obtener tokens de acceso OAuth2 y luego se hacen llamadas a endpoints como `/admin/api/2023-10/orders.json` para obtener los detalles de las órdenes, filtrando por fecha o estado de pago.

3.  **Consumo de archivos planos (CSV, JSON) desde buckets en la nube**:
    * **Escenario**: Una empresa recibe archivos CSV con datos de transacciones de sus socios comerciales o archivos JSON con logs de aplicaciones que se almacenan en servicios de almacenamiento en la nube como Amazon S3, Google Cloud Storage o Azure Blob Storage.
    * **Proceso**: Se utilizan los SDKs (Software Development Kits) o APIs de estos servicios en la nube para listar y descargar los archivos. Es fundamental tener en cuenta la estructura de los directorios (por ejemplo, `s3://bucket-name/raw_data/2025/06/03/transacciones.csv`) para una extracción organizada. Para archivos grandes, se pueden usar técnicas de descarga por partes o streaming para evitar problemas de memoria.
    * **Ejemplo**: Consumir archivos de logs de servidores web en formato JSON que se suben diariamente a un bucket de Azure Blob Storage. Se podría programar un script para que a las 2 AM descargue todos los archivos JSON del día anterior que se encuentren en un prefijo específico, como `logs/webserver/2025/06/03/`.

##### Transformación de datos

Una vez extraídos los datos, la etapa de **transformación** es donde el valor real se agrega. Aquí, los datos son limpiados, combinados, enriquecidos y modificados para ajustarse a los requisitos del negocio y a la estructura del sistema de destino. Esta fase es crítica para garantizar la **calidad y la usabilidad** de los datos para análisis. Es común que esta etapa sea la más intensiva en recursos computacionales.

1.  **Conversión de formatos de fecha y limpieza de valores nulos en Spark**:
    * **Escenario**: Los datos extraídos contienen fechas en diferentes formatos (por ejemplo, "MM/DD/YYYY", "DD-MM-YY", "YYYYMMDD") y tienen muchos valores nulos en campos importantes como el ID de cliente o el monto de la transacción.
    * **Proceso con Spark**: Utilizando Apache Spark, puedes leer los datos (por ejemplo, desde un DataFrame). Para la conversión de fechas, se usan funciones como `to_date()` y `date_format()` para estandarizar a un formato único (por ejemplo, "YYYY-MM-DD"). Para los valores nulos, Spark ofrece funciones como `fillna()` para reemplazar nulos con un valor por defecto (0 para números, "Desconocido" para cadenas) o `dropna()` para eliminar filas que contengan nulos en columnas específicas.
    * **Ejemplo**: Un conjunto de datos de productos tiene una columna `precio` con algunos valores nulos y una columna `fecha_ingreso` con formatos inconsistentes. En Spark, puedes rellenar los nulos de `precio` con el promedio de los precios existentes y estandarizar `fecha_ingreso` a 'YYYY-MM-DD' usando algo como:
        ```python
        df = df.withColumn("fecha_ingreso", to_date(col("fecha_ingreso"), "MM/dd/yyyy")) \
               .fillna({"precio": df.agg({"precio": "avg"}).collect()[0][0]})
        ```

2.  **Enriquecimiento de registros con datos geográficos desde una tabla de referencia externa**:
    * **Escenario**: Tienes un conjunto de datos de ventas con el ID de la tienda, pero necesitas analizar las ventas por región geográfica (ciudad, estado, país). Dispones de una tabla maestra de tiendas con sus coordenadas geográficas y ubicaciones.
    * **Proceso**: Se realiza una operación de **join** (unión) entre el conjunto de datos de ventas y la tabla de referencia de tiendas utilizando el ID de la tienda como clave. Una vez unidos, puedes agregar columnas como `ciudad`, `estado`, `país` a cada registro de venta, permitiendo análisis geográficos detallados. Si la tabla de referencia es muy grande, se pueden usar técnicas de broadcast join en Spark para optimizar el rendimiento.
    * **Ejemplo**: Enriquecer un registro de llamadas de clientes con información del plan de servicio del cliente. Si la llamada solo contiene el `ID_cliente`, puedes unirla con una tabla maestra de `clientes_servicios` que contenga `ID_cliente` y `tipo_plan` para agregar esa información a cada registro de llamada.

3.  **Validación de estructuras de datos usando esquemas definidos (p. ej., Avro/Parquet)**:
    * **Escenario**: Se reciben datos de diferentes sistemas que deben cumplir con una estructura y tipo de datos predefinidos para asegurar la consistencia en el data warehouse.
    * **Proceso**: Se utilizan **esquemas predefinidos** (como Avro o Parquet, o incluso JSON Schema) para validar que los datos entrantes cumplan con las especificaciones. Esto implica verificar que todas las columnas esperadas estén presentes, que los tipos de datos sean correctos (por ejemplo, que una columna de `edad` sea un entero y no una cadena), y que no haya datos inesperados. Si los datos no cumplen el esquema, pueden ser rechazados, puestos en cuarentena para revisión o corregidos automáticamente si es posible. Spark, al trabajar con Parquet o Avro, infiere o aplica esquemas, permitiendo la validación automática.
    * **Ejemplo**: Validar que los datos de un feed de productos contengan siempre las columnas `id_producto` (entero), `nombre_producto` (cadena), `precio` (decimal) y `disponible` (booleano). Si un archivo de entrada tiene `precio` como cadena o le falta la columna `disponible`, el proceso de validación lo marcará como un error.

##### Carga de datos

La etapa final del proceso ETL es la **carga**, donde los datos transformados se mueven al sistema de destino final. Este sistema puede ser un data warehouse (para análisis estructurados y reportes), un data lake (para almacenar datos brutos o semi-estructurados para futuros usos), o una base de datos analítica (optimizada para consultas complejas). La eficiencia y la robustez de esta etapa son cruciales para la disponibilidad de los datos.

1.  **Carga de datos en Amazon Redshift mediante COPY desde S3**:
    * **Escenario**: Tienes grandes volúmenes de datos transformados en archivos Parquet o CSV en un bucket de Amazon S3, y necesitas cargarlos eficientemente en tu data warehouse columnar, Amazon Redshift.
    * **Proceso**: Redshift ofrece el comando `COPY` que es altamente optimizado para cargar datos masivos directamente desde S3. Este comando puede inferir el esquema o utilizar uno definido, manejar la compresión de archivos, y distribuir los datos de manera óptima entre los nodos de Redshift para un rendimiento de consulta superior. Es mucho más rápido que insertar fila por fila.
    * **Ejemplo**: Cargar los datos de logs de aplicaciones procesados en S3 a una tabla `app_logs` en Redshift. El comando Redshift sería algo como:
        ```sql
        COPY app_logs FROM 's3://your-bucket/processed_logs/2025/06/03/'
        CREDENTIALS 'aws_access_key_id=YOUR_ACCESS_KEY_ID;aws_secret_access_key=YOUR_SECRET_ACCESS_KEY'
        FORMAT AS PARQUET;
        ```

2.  **Inserción por lotes en Snowflake con control de errores**:
    * **Escenario**: Necesitas cargar datos de transacciones financieras en Snowflake, y es crítico que cualquier fila con errores (por ejemplo, datos faltantes o incorrectos) sea identificada y aislada sin detener el proceso de carga.
    * **Proceso**: Snowflake soporta la **carga por lotes (batch inserts)** y ofrece robustas capacidades de **manejo de errores**. Puedes usar el comando `COPY INTO <table>` con opciones como `ON_ERROR = CONTINUE` o `ON_ERROR = ABORT_STATEMENT` y `VALIDATION_MODE = RETURN_ERRORS` para especificar cómo manejar los errores. Esto permite cargar las filas válidas y registrar las filas con errores en una tabla separada o un archivo para su posterior revisión y corrección.
    * **Ejemplo**: Cargar datos de clientes desde archivos CSV a una tabla `clientes` en Snowflake. Si una fila CSV tiene una fecha de nacimiento inválida, Snowflake puede saltar esa fila y continuar con el resto, registrando el error:
        ```sql
        COPY INTO clientes FROM @my_s3_stage/clientes.csv
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        ON_ERROR = 'SKIP_FILE'; -- O 'CONTINUE' para registrar errores por fila
        ```

3.  **Escritura de particiones optimizadas en HDFS para procesamiento posterior**:
    * **Escenario**: Estás procesando grandes volúmenes de datos en un clúster de Hadoop (por ejemplo, con Spark), y los datos transformados serán utilizados por otros procesos analíticos (como Hive o Impala) que se benefician enormemente de la partición de datos.
    * **Proceso**: Al escribir los datos en **HDFS (Hadoop Distributed File System)**, se pueden **particionar** los datos por una o más columnas (por ejemplo, `año`, `mes`, `día`, `región`). Esto crea una estructura de directorios que permite que las consultas posteriores lean solo los datos relevantes, mejorando drásticamente el rendimiento. Además, se pueden guardar los datos en formatos optimizados para lectura como Parquet o ORC, que son auto-descriptivos y soportan compresión columnar.
    * **Ejemplo**: Guardar datos de eventos de usuario procesados en Spark en HDFS, particionando por `fecha` y `tipo_evento`. Esto resultaría en una estructura de directorios como `/user/events/fecha=2025-06-03/tipo_evento=clic/` o `/user/events/fecha=2025-06-03/tipo_evento=vista_pagina/`. Cuando se consulta, por ejemplo, `SELECT * FROM events WHERE fecha = '2025-06-03' AND tipo_evento = 'clic'`, el motor de consulta solo leerá el directorio específico, no todo el conjunto de datos. En Spark, esto se hace con:
        ```python
        df_processed.write.partitionBy("fecha", "tipo_evento").parquet("/user/events/")
        ```

### 3.1.2 Diferencias entre ETL y ELT

**ETL (Extract, Transform, Load)** y **ELT (Extract, Load, Transform)** son dos enfoques fundamentales para la integración de datos, diseñados para mover y preparar información de diversas fuentes para su análisis y uso. Aunque ambos buscan el mismo objetivo final de disponibilizar datos transformados, se distinguen crucialmente en la secuencia de sus operaciones y en los entornos tecnológicos más adecuados para cada uno.

##### Características del patrón ETL

En el modelo ETL tradicional, la **transformación** de los datos se realiza **antes de la carga** en el sistema de destino. Esto típicamente ocurre en un servidor o motor de procesamiento intermedio, distinto del sistema de origen y del sistema de destino final.

**Útil en entornos donde la transformación debe ocurrir antes de tocar el sistema de destino**: Este patrón es ideal cuando los datos necesitan ser limpiados, validados, enriquecidos o agregados de manera significativa antes de ser almacenados. Esto asegura que solo datos de alta calidad y estructurados de una forma específica lleguen al sistema de destino, lo cual es crucial para bases de datos transaccionales, sistemas de reportes operativos o aplicaciones que dependen de una estructura de datos rígida y predefinida.

* **Ejemplo**: Una empresa de telecomunicaciones que procesa registros de llamadas (**CDRs - Call Detail Records**). Antes de cargar estos datos en un sistema de facturación o un data warehouse para análisis de uso, los datos de los CDRs (que pueden ser crudos, contener errores o duplicados) se extraen. Luego, se transforman: se eliminan duplicados, se estandarizan formatos de números telefónicos, se calculan duraciones de llamadas, se asocian a clientes específicos y se validan contra catálogos de tarifas. Solo después de estas rigurosas transformaciones, los datos limpios y estructurados se cargan en el sistema de destino, asegurando la precisión de la facturación y los reportes.

**Común en soluciones *on-premise* con control sobre la lógica de negocio previa**: Históricamente, el ETL ha sido el pilar de los entornos de *data warehousing* *on-premise*, donde las organizaciones tienen un control completo sobre la infraestructura y los servidores de procesamiento intermedios. Permite implementar reglas de negocio complejas y detalladas antes de que los datos ingresen al *data warehouse*, garantizando la consistencia y la integridad.

* **Ejemplo**: Una institución bancaria que gestiona datos de transacciones financieras. Dada la sensibilidad y la necesidad de cumplimiento normativo, los datos de transacciones de múltiples sistemas (cajeros automáticos, banca en línea, sucursales) se extraen. Se aplican transformaciones en servidores *on-premise* para anonimizar información sensible, validar la coherencia entre cuentas, agregar transacciones diarias por cliente y aplicar reglas de negocio para detectar fraudes o patrones sospechosos. Esta preparación se realiza en entornos controlados y seguros antes de cargar los datos en un *data mart* departamental o un *data warehouse* central para análisis financiero y regulatorio.

##### Características del patrón ELT

En el modelo ELT, los datos se **cargan** en el sistema de destino **tal como están** (o con transformaciones mínimas) y la **transformación** ocurre **después**, directamente en el sistema de destino. Este enfoque capitaliza la capacidad de cómputo y almacenamiento escalable de las plataformas de datos modernas.

**Escenarios en los que el almacenamiento y procesamiento escalable está disponible (BigQuery, Snowflake, Amazon Redshift, Azure Synapse Analytics)**: El ELT florece en el ecosistema de *Big Data* y la computación en la nube. Los *data warehouses* en la nube, *data lakes* y motores de procesamiento distribuido ofrecen una escalabilidad masiva tanto para el almacenamiento como para el cómputo, lo que permite cargar grandes volúmenes de datos crudos rápidamente y luego procesarlos internamente.

* **Ejemplo**: Una empresa de *e-commerce* recopila clics de usuarios, eventos de navegación, datos de carritos abandonados y registros de *logs* de servidores web. Estos datos, a menudo semi-estructurados o no estructurados y en volúmenes masivos, se ingieren directamente en un *data lake* (por ejemplo, Amazon S3) o un *data warehouse* en la nube (como Google BigQuery) con mínimas transformaciones iniciales. Una vez cargados, los analistas de datos o los ingenieros de datos utilizan las capacidades de procesamiento del propio *data warehouse* (SQL, UDFs, etc.) o motores de procesamiento sobre el *data lake* (Spark, Presto) para limpiar, unir, agregar y transformar los datos según sea necesario para análisis de comportamiento del usuario, personalización o *machine learning*.

**Útil para agilizar la carga y reducir el tiempo de espera en ingestión de datos crudos**: Al posponer las transformaciones, el ELT permite una ingestión de datos mucho más rápida. Los datos se cargan "crudos" o "lo más crudos posible" en el sistema de destino, minimizando los cuellos de botella en la fase de carga. Esto es especialmente valioso para datos en tiempo real o casi real, donde la velocidad de ingestión es crítica.

* **Ejemplo**: Una plataforma de redes sociales que ingiere constantemente millones de publicaciones, comentarios, "me gusta" y metadatos de usuarios. Sería ineficiente y lento aplicar transformaciones complejas a cada pieza de datos antes de cargarla. En cambio, todos estos datos se ingieren rápidamente en un *data lake* escalable. Posteriormente, los equipos de análisis utilizan herramientas y *frameworks* que operan directamente sobre el *data lake* para transformar estos datos crudos en conjuntos de datos agregados (por ejemplo, conteo de "me gusta" por publicación, análisis de sentimiento de comentarios) para reportes, *feeds* personalizados o modelos de recomendación.

##### Cuándo usar cada patrón

**Usar ETL cuando la calidad y la estructura de los datos debe garantizarse antes de la carga**: Este patrón es preferible cuando el sistema de destino es sensible a la calidad de los datos, tiene esquemas rígidos, o se requiere que los datos estén completamente limpios y conformes a reglas de negocio antes de su almacenamiento.

* **Ejemplo**: Un sistema de *Enterprise Resource Planning* (**ERP**) o un sistema de gestión de clientes (**CRM**) que requiere datos maestros (clientes, productos, proveedores) con una estructura y validación estrictas. Si se integran datos de múltiples fuentes (sistemas legados, hojas de cálculo, sistemas de terceros) para poblar estos sistemas, el ETL se asegura de que la calidad y el formato de los datos sean impecables antes de la carga, evitando inconsistencias o errores que podrían afectar las operaciones diarias.

**Usar ELT cuando el destino tiene suficiente capacidad de cómputo y permite transformaciones complejas dentro del motor**: Este enfoque es ideal para entornos de *Big Data* y la nube, donde la escalabilidad de almacenamiento y procesamiento del destino es una ventaja. Permite a los analistas y científicos de datos trabajar directamente con los datos crudos y transformarlos *ad-hoc* según sus necesidades, sin la necesidad de un paso intermedio de transformación previo.

* **Ejemplo**: Una empresa que busca realizar análisis predictivos y *machine learning* sobre vastos conjuntos de datos de comportamiento del cliente, datos de sensores IoT o registros de transacciones financieras a gran escala. Cargar todos los datos crudos en un *data lake* o *data warehouse* en la nube y luego utilizar herramientas como Apache Spark, SQL en *data warehouses* o incluso lenguajes de programación como Python y R directamente sobre la plataforma para realizar transformaciones complejas, ingenierías de características y construir modelos. Esto permite una mayor flexibilidad y experimentación con los datos.

### 3.1.3 Diseño modular y desacoplado de pipelines

El diseño modular es una estrategia fundamental en la construcción de **pipelines de datos eficientes y resilientes**. Consiste en descomponer un pipeline complejo en unidades más pequeñas, autónomas y bien definidas, conocidas como **módulos o componentes**. Cada uno de estos módulos tiene una **responsabilidad única y bien delimitada**, lo que los hace independientes entre sí. Este enfoque no solo simplifica enormemente el **mantenimiento y la depuración** de los pipelines, sino que también mejora significativamente su **escalabilidad, flexibilidad y la capacidad de integrar sistemas de monitoreo y versionado** de manera efectiva. Al dividir el problema en partes manejables, se facilita la colaboración entre equipos y se reduce el riesgo de errores en sistemas de gran escala.

##### Principios del diseño modular

* **Separación de responsabilidades**: Este principio es la piedra angular del diseño modular. Cada módulo debe tener una única razón para cambiar, es decir, debe encargarse de una **función específica y bien definida**. Por ejemplo, un módulo podría ser responsable exclusivamente de la extracción de datos, otro de las transformaciones de limpieza y enriquecimiento, y un tercero de la carga en un destino final. Esto evita que los cambios en una parte del sistema afecten a otras áreas no relacionadas.
* **Reutilización**: Los módulos diseñados bajo este principio son **genéricos y parametrizables**, lo que permite su utilización en múltiples pipelines con diferentes configuraciones o fuentes de datos. En lugar de escribir código desde cero para cada nueva tarea, se pueden ensamblar pipelines a partir de una biblioteca de módulos existentes, lo que **acelera el desarrollo y reduce la duplicación de código**.
* **Testabilidad**: La independencia de los módulos facilita la **prueba unitaria y la integración continua**. Cada componente puede ser **verificado de manera aislada** para asegurar que cumple con su función esperada, simplificando la detección y corrección de errores antes de que se propaguen a todo el pipeline. Esto lleva a una mayor **confiabilidad del sistema** y ciclos de desarrollo más rápidos.

**Ejemplo en un pipeline de datos**

* **Módulo de Extracción de Datos (Extract Layer)**: Este módulo se especializa en la lectura de datos desde diversas fuentes y su preparación para las etapas posteriores.
    * Un módulo de extracción genérico que puede leer datos de diferentes bases de datos relacionales (MySQL, PostgreSQL) usando JDBC, así como de servicios de almacenamiento en la nube como Amazon S3 o Google Cloud Storage. Este módulo se encargaría de normalizar la lectura, manejar las credenciales de conexión de forma segura y escribir los datos crudos en un formato intermedio eficiente como **Parquet o Avro** en un Data Lake (por ejemplo, HDFS o un bucket S3). Podría configurarse mediante parámetros como la URL de la base de datos, la tabla a leer, las credenciales, la ruta de S3 o el nombre del bucket.

* **Módulo de Transformación de Calidad y Enriquecimiento (Transform Layer)**: Se enfoca en la aplicación de reglas de negocio y limpieza de datos.
    * Un módulo de transformación que recibe un DataFrame de Spark. Este módulo podría incluir una serie de funciones parametrizables para:
        * **Validación de tipos de datos**: Asegurando que las columnas contengan los tipos esperados (ej. `precio` es numérico).
        * **Eliminación o imputación de valores nulos**: Rellenando valores faltantes con promedios, medianas o valores por defecto, o eliminando registros incompletos.
        * **Estandarización de formatos**: Por ejemplo, convertir todas las fechas a un formato ISO 8601 o estandarizar códigos postales.
        * **Enriquecimiento de datos**: Uniendo el conjunto de datos actual con otras fuentes (ej. tablas de referencia de clientes o catálogos de productos) para añadir información relevante. Este módulo recibiría las reglas de transformación y las fuentes de datos adicionales como configuraciones, permitiendo su reutilización en diferentes contextos.

* **Módulo de Carga de Datos (Load Layer)**: Responsable de persistir los datos transformados en los destinos finales.
    * Un módulo de carga que puede conectarse a diversos destinos de almacenamiento o bases de datos analíticas. Podría configurarse para:
        * Cargar datos en un **data warehouse columnar** como Snowflake o Amazon Redshift, optimizando la escritura para el rendimiento de consultas.
        * Escribir datos en un **Data Lakehouse** (como Delta Lake en Databricks) para permitir futuras consultas con SQL o herramientas de BI.
        * Publicar datos en un **servicio de mensajería** como Apache Kafka para consumo en tiempo real por otras aplicaciones o microservicios. Este módulo sería configurable a través de parámetros que definan el destino (ej. tipo de base de datos, nombre del cluster, tema de Kafka), el modo de escritura (ej. `append`, `overwrite`, `upsert`) y las credenciales de autenticación.

##### Desacoplamiento de componentes

El **desacoplamiento** es un principio de diseño que busca minimizar las dependencias directas entre los módulos de un pipeline. Su objetivo principal es asegurar que los cambios o fallas en un componente no tengan un impacto en cascada sobre los demás, lo que aumenta la **tolerancia a fallos, la flexibilidad y la capacidad de evolución del sistema**. Cuando los componentes están fuertemente acoplados, un cambio en uno de ellos podría requerir modificaciones en otros, lo que complica el mantenimiento y ralentiza el desarrollo. El desacoplamiento promueve la independencia operativa y tecnológica.

* **Uso de colas de mensajes para desacoplar operaciones asíncronas**: Las colas de mensajes actúan como intermediarios entre los productores y consumidores de datos. Por ejemplo, Apache Kafka es una plataforma de streaming distribuida que permite que un módulo de extracción escriba eventos o registros en un **tópico de Kafka**, mientras que un módulo de transformación lee de ese tópico de forma **asíncrona e independiente**. Si el módulo de transformación experimenta una falla temporal, el módulo de extracción puede seguir produciendo datos sin interrupción, ya que los mensajes se acumulan en la cola. Esto mejora la **resiliencia del pipeline** y permite que los módulos operen a diferentes velocidades.
* **Serialización y persistencia de datos intermedios**: En lugar de pasar datos directamente entre módulos en memoria, se puede **serializar y almacenar los resultados intermedios** de un módulo en un sistema de almacenamiento distribuido como **HDFS (Hadoop Distributed File System)**, **Amazon S3** o **Azure Data Lake Storage**. Esto permite que cada módulo "lea" los datos que necesita del almacenamiento y "escriba" sus resultados de vuelta, rompiendo la dependencia directa. Por ejemplo, el módulo de extracción podría escribir datos brutos en S3, y luego el módulo de transformación leería desde S3, procesaría los datos y escribiría los resultados transformados en otra ubicación en S3. Esto no solo desacopla, sino que también proporciona **puntos de recuperación** en caso de fallas y facilita la **auditoría** de las etapas del pipeline.
* **Configuración dinámica y externa de pipelines**: En lugar de codificar las dependencias y la lógica del pipeline directamente en el código, se utilizan **archivos de configuración externos** (como YAML o JSON) para definir la secuencia de ejecución de los módulos, las fuentes de datos, los destinos y los parámetros específicos. Esto permite **modificar el comportamiento de un pipeline sin necesidad de recompilar o redeployar el código**. Por ejemplo, un archivo YAML podría especificar que el módulo de extracción `ModuloExtraccionDB` debe leer de una base de datos `db_produccion` y escribir en `s3://raw-data/`, y que posteriormente el `ModuloTransformacionCalidad` debe leer de `s3://raw-data/` y escribir en `s3://cleaned-data/`. Este enfoque promueve la **flexibilidad y la agilidad** en la gestión de pipelines.

### 3.1.4 Introducción a Apache Airflow

**Apache Airflow** es una potente plataforma de código abierto diseñada para la **programación, orquestación y monitoreo de flujos de trabajo (workflows)**. En esencia, permite definir, programar y supervisar secuencias complejas de tareas de manera programática, utilizando un enfoque de **DAGs (Directed Acyclic Graphs)**. Su capacidad para manejar dependencias, reintentos y la ejecución distribuida lo convierte en una herramienta fundamental para la automatización de procesos de datos, ETL y otras operaciones de TI.

##### DAGs (Directed Acyclic Graphs)

Los **DAGs** son el corazón de Airflow. Representan un flujo de tareas con dependencias claramente definidas y, como su nombre lo indica, no pueden contener ciclos. Cada nodo en un DAG es una tarea, y las aristas representan las dependencias entre ellas, indicando qué tareas deben completarse antes de que otras puedan comenzar.

* **Definición de un DAG en Python para ejecutar una serie de tareas de limpieza y carga diaria**: Un DAG en Airflow se define completamente en código Python. Esto permite una gran flexibilidad y control de versiones. Por ejemplo, un DAG para un proceso ETL diario podría incluir tareas como la extracción de datos de una base de datos, la limpieza y transformación de esos datos, y finalmente la carga en un data warehouse.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='etl_diario',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'datos']
) as dag:
    # Tarea 1: Extracción de datos
    extraer_datos = BashOperator(
        task_id='extraer_datos',
        bash_command='python /app/scripts/extraer_datos.py'
    )

    # Tarea 2: Limpieza de datos
    limpiar_datos = BashOperator(
        task_id='limpiar_datos',
        bash_command='python /app/scripts/limpiar_datos.py'
    )

    # Tarea 3: Carga de datos
    cargar_datos = BashOperator(
        task_id='cargar_datos',
        bash_command='python /app/scripts/cargar_datos.py'
    )

    extraer_datos >> limpiar_datos >> cargar_datos
```

* **Representación visual del flujo de tareas en la interfaz de Airflow**: Una de las características más destacadas de Airflow es su interfaz de usuario web intuitiva. Esta interfaz permite visualizar el DAG de forma gráfica, mostrando las tareas, sus dependencias y el estado de cada ejecución. Esto facilita el monitoreo y la depuración de los flujos de trabajo.
* **Control de versiones y reutilización de DAGs en entornos dev/test/prod**: Dado que los DAGs son código, se benefician de las prácticas de control de versiones (Git, por ejemplo). Esto permite un desarrollo colaborativo, seguimiento de cambios y la promoción de DAGs a través de diferentes entornos (desarrollo, pruebas, producción) de manera controlada y reproducible, asegurando la consistencia del flujo de trabajo en todas las fases.

##### Operadores, sensores y tareas

En Airflow, las **tareas** son las unidades básicas de trabajo que un DAG ejecuta. Estas tareas se definen utilizando **operadores** y **sensores**.

* **Operadores**: Son clases predefinidas que encapsulan la lógica para ejecutar una acción específica. Airflow ofrece una amplia variedad de operadores para interactuar con diferentes sistemas y tecnologías.

  * **BashOperator**: Permite ejecutar comandos de shell. Es útil para scripts sencillos, mover archivos o cualquier comando que pueda ejecutarse en la terminal.

```python
from airflow.operators.bash import BashOperator
# ... dentro de un DAG ...
ejecutar_script_shell = BashOperator(
    task_id='ejecutar_script_shell',
    bash_command='sh /opt/airflow/scripts/mi_script.sh arg1 arg2'
)
```

  * **PythonOperator**: Permite ejecutar cualquier función de Python. Esto es extremadamente flexible para lógica de negocio personalizada, transformaciones de datos complejas o integración con bibliotecas de Python.

```python
from airflow.operators.python import PythonOperator
import pandas as pd

def transformar_datos_py():
    # Ejemplo: Cargar, transformar y guardar datos con Pandas
    df = pd.read_csv('/tmp/raw_data.csv')
    df['columna_nueva'] = df['columna_existente'] * 2
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print("Datos transformados exitosamente con Python.")

# ... dentro de un DAG ...
transformacion_python = PythonOperator(
    task_id='transformacion_python',
    python_callable=transformar_datos_py
)
```

  * **SparkSubmitOperator**: Facilita la ejecución de jobs de Apache Spark en un clúster. Es ideal para procesamientos de Big Data distribuidos.

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# ... dentro de un DAG ...
ejecutar_spark_job = SparkSubmitOperator(
    task_id='ejecutar_spark_job',
    application='/opt/airflow/dags/spark_jobs/procesar_ventas.py',
    conn_id='spark_default',  # Conexión Spark configurada en Airflow
    total_executor_cores='2',
    executor_memory='2g',
    num_executors='10',
    application_args=['--input', '/path/to/input', '--output', '/path/to/output']
)
```

* **Sensores**: Son un tipo especial de operador que espera a que se cumpla una condición externa antes de que una tarea pueda continuar. Esto es crucial para flujos de trabajo que dependen de la disponibilidad de recursos o eventos externos.

  * **Implementación de un sensor que espera archivos nuevos en S3**: Un `S3KeySensor` puede ser utilizado para pausar un flujo de trabajo hasta que un archivo específico (o un patrón de archivos) aparezca en un bucket de S3.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# ... dentro de un DAG ...
esperar_archivo_s3 = S3KeySensor(
    task_id='esperar_archivo_s3',
    bucket_name='mi-bucket-s3',
    bucket_key='entrada_diaria/{{ ds_nodash }}.csv', # Espera un archivo con la fecha de ejecución
    aws_conn_id='aws_default',
    poke_interval=60,  # Chequea cada 60 segundos
    timeout=60 * 60 * 24 # Timeout de 24 horas
)
```

* **Combinación de tareas condicionales con BranchPythonOperator**: El `BranchPythonOperator` permite ejecutar una función Python que retorna el `task_id` de la siguiente tarea a ejecutar, creando así flujos de trabajo condicionales. Esto es útil para ramificar la ejecución basándose en la lógica de negocio o en resultados de tareas previas.

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator # Utilizado para tareas que no hacen nada
from random import random

def decidir_ramal():
    if random() > 0.5:
        return 'procesar_datos_a'
    else:
        return 'procesar_datos_b'

# ... dentro de un DAG ...
iniciar_proceso = DummyOperator(task_id='iniciar_proceso')
decidir_camino = BranchPythonOperator(
    task_id='decidir_camino',
    python_callable=decidir_ramal
)
procesar_datos_a = BashOperator(
    task_id='procesar_datos_a',
    bash_command='echo "Procesando datos con la lógica A"'
)
procesar_datos_b = BashOperator(
    task_id='procesar_datos_b',
    bash_command='echo "Procesando datos con la lógica B"'
)
finalizar_proceso = DummyOperator(task_id='finalizar_proceso', trigger_rule='none_failed_min_one_success')

iniciar_proceso >> decidir_camino
decidir_camino >> [procesar_datos_a, procesar_datos_b]
[procesar_datos_a, procesar_datos_b] >> finalizar_proceso
```

##### Gestión de dependencias y programación

Una gestión eficiente de las dependencias y la programación es crucial para la estabilidad y el rendimiento de los flujos de trabajo ETL. Airflow proporciona mecanismos robustos para esto.

* **Programar un DAG para que se ejecute cada hora y recupere los datos más recientes**: Airflow utiliza el parámetro `schedule_interval` para definir la frecuencia de ejecución de un DAG. Este puede ser una expresión cron, un `timedelta`, o una cadena predefinida como `@hourly`, `@daily`, etc. Para recuperar los datos más recientes, las tareas dentro del DAG pueden hacer uso de las variables de contexto de Airflow, como `{{ ds }}` (fecha de ejecución) o `{{ data_interval_start }}` y `{{ data_interval_end }}` (rango de tiempo de los datos a procesar).

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='ingesta_horaria',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(hours=1), # Ejecución cada hora
    catchup=False,
    tags=['ingesta']
) as dag:
    ingestar_datos = BashOperator(
        task_id='ingestar_datos',
        # Usando macros para el rango de tiempo de ejecución
        bash_command='python /app/scripts/ingestar_datos.py --start_time {{ data_interval_start }} --end_time {{ data_interval_end }}'
    )
```

* **Gestión de tareas paralelas con `depends_on_past=False` para mejorar el rendimiento**: Por defecto, las tareas de un DAG pueden tener una dependencia implícita de las ejecuciones previas exitosas. Sin embargo, en muchos escenarios, especialmente con grandes volúmenes de datos, es deseable que las tareas se ejecuten de forma paralela sin esperar la finalización de ejecuciones anteriores del mismo DAG. El parámetro `depends_on_past=False` a nivel de DAG o tarea permite que una tarea se ejecute incluso si su instancia anterior (de una ejecución previa del DAG) falló o aún no ha terminado, mejorando el rendimiento al permitir el paralelismo en el tiempo.
* **Implementación de `backfill` para recuperar ejecuciones pasadas no procesadas**: Cuando un DAG se define por primera vez o ha estado inactivo y se necesita procesar datos de un período anterior, la función `backfill` de Airflow es invaluable. Permite ejecutar el DAG para un rango de fechas específico en el pasado, como si el DAG hubiera estado activo y programado durante ese tiempo. Esto es fundamental para cargar datos históricos o para recuperarse de interrupciones prolongadas en el procesamiento. Se puede ejecutar mediante la línea de comandos de Airflow:

```bash
airflow dags backfill -s 2024-01-01 -e 2024-01-31 mi_dag_id
```

Esto ejecutaría `mi_dag_id` para cada `schedule_interval` entre el 1 y el 31 de enero de 2024.

### 3.1.5 Orquestación de tareas distribuidas

La **orquestación de tareas distribuidas** es el proceso de gestionar y coordinar la ejecución de múltiples procesos de datos que operan en sistemas distribuidos. Esto implica no solo controlar cuándo y cómo se ejecutan estas tareas, sino también asegurar que se sincronicen correctamente y que el sistema sea capaz de recuperarse de fallos inesperados. En el contexto de Big Data y ETL, esto es fundamental para manejar volúmenes masivos de información de manera eficiente y confiable.

##### Coordinación temporal de tareas

La **coordinación temporal de tareas** se refiere a la capacidad de definir y hacer cumplir el orden y el momento de ejecución de las tareas. Las tareas en un flujo de trabajo de datos a menudo tienen interdependencias, lo que significa que una tarea solo puede comenzar una vez que otra ha finalizado o ha producido ciertos resultados. Los sistemas de orquestación permiten establecer **horarios** fijos (por ejemplo, ejecutar un ETL todas las noches a medianoche), **condiciones de ejecución** (como iniciar una transformación solo si se ha recibido un archivo específico) o **secuencias dependientes** (ejecutar el pre-procesamiento de datos antes de la carga en un *data warehouse*). Esta coordinación asegura que los datos se procesen en el orden correcto y que se cumplan los acuerdos de nivel de servicio (SLA).

**Ejemplos**:

* **Pipeline diario que inicia a las 3am y espera la finalización de cargas previas**

    Este pipeline implementa un ETL nocturno que procesa datos transaccionales del día anterior. Utiliza `schedule_interval='0 3 * * *'` para ejecutarse diariamente a las 3:00 AM. La orquestación incluye sensores que verifican la disponibilidad de archivos fuente, tareas de extracción desde múltiples sistemas (CRM, ERP, logs web), transformaciones de limpieza y normalización de datos, y finalmente la carga hacia el data warehouse. Las dependencias aseguran que cada etapa complete exitosamente antes de proceder, con mecanismos de retry automático y alertas por email en caso de fallos. El pipeline también incluye checkpoints de validación que verifican la integridad de los datos antes de cada transformación mayor.

* **Pipeline semanal que consolida los datos de los últimos siete días**

    Este proceso se ejecuta cada lunes a las 6:00 AM usando `schedule_interval='0 6 * * 1'` para generar reportes ejecutivos y métricas de negocio. La orquestación coordina la agregación de datos desde múltiples fuentes diarias ya procesadas, calculando KPIs como retention de usuarios, revenue por segmento, y métricas de calidad de servicio. Incluye tareas paralelas que procesan diferentes dimensiones de análisis (geográfico, demográfico, temporal), seguidas por una tarea de consolidación final que genera dashboards actualizados y envía reportes automatizados a stakeholders. El pipeline implementa branching logic para manejar semanas con días festivos o datos faltantes.

* **Tarea condicional que solo se ejecute si la calidad de los datos supera cierto umbral**

    Esta implementación utiliza `BranchPythonOperator` para evaluar métricas de calidad como completitud, consistencia y validez de los datos. La función de decisión consulta reglas de negocio predefinidas (por ejemplo, <5% de valores nulos, fechas dentro de rangos esperados, concordancia entre sistemas). Si los datos pasan las validaciones, se ejecuta el flujo principal de procesamiento; si no, se activa un flujo alternativo que registra los problemas, notifica al equipo de datos, y potencialmente ejecuta rutinas de limpieza automática. La orquestación incluye tareas downstream que solo se ejecutan tras confirmación manual o automática de que los datos corregidos cumplen los estándares de calidad.

* **Pipeline de ML con reentrenamiento condicional basado en drift de datos**

    Este pipeline monitorea continuamente la performance de modelos de machine learning en producción. Se ejecuta cada 4 horas usando sensores que evalúan métricas como accuracy, precision y recall contra umbrales predefinidos. Cuando detecta degradación del modelo (data drift o concept drift), automaticamente activa un flujo de reentrenamiento que incluye feature engineering actualizado, validación cruzada, y despliegue A/B testing del nuevo modelo. La orquestación coordina tareas paralelas para entrenar múltiples algoritmos, seleccionar el mejor performante, y actualizar gradualmente el modelo en producción solo después de validaciones exhaustivas en ambiente staging.

* **Pipeline de procesamiento de eventos en tiempo real con ventanas deslizantes**

    Este flujo procesa streams de datos de IoT o clickstream usando ventanas temporales de 15 minutos que se solapan cada 5 minutos. La orquestación maneja la complejidad de procesar datos que llegan con diferentes latencias, implementando buffers y mecanismos de ordenamiento temporal. Las tareas incluyen detección de anomalías en tiempo real, cálculo de métricas agregadas (promedios móviles, percentiles), y triggers automáticos para alertas cuando se detectan patrones críticos. El pipeline coordina la persistencia de resultados en sistemas OLTP para consultas rápidas y OLAP para análisis histórico, manejando backpressure y failover automático entre clusters de procesamiento.

* **Pipeline de compliance y auditoría con retención de datos personalizada**

    Este sistema implementa políticas de governanza de datos automatizadas que se ejecutan mensualmente para cumplir regulaciones como GDPR o HIPAA. La orquestación coordina tareas que identifican datos sensibles usando classificación automática, aplican técnicas de anonimización o pseudonimización según políticas definidas, y gestionan la retención/eliminación de datos basada en reglas legales específicas por jurisdicción. Incluye generación automática de reportes de compliance, logs de auditoría inmutables, y workflows de aprobación que requieren intervención humana para operaciones críticas como eliminación masiva de datos personales o cambios en políticas de retención.

##### Distribución de cargas y escalabilidad

La **distribución de cargas y escalabilidad** es una de las mayores ventajas de la orquestación en entornos distribuidos. Al diseñar los flujos de trabajo para que se ejecuten en paralelo o en nodos separados, es posible procesar grandes volúmenes de datos de manera mucho más rápida y eficiente. Esto permite que el sistema **escale horizontalmente**, lo que significa que se pueden añadir más recursos (nodos o máquinas) para manejar un aumento en la demanda de procesamiento sin afectar el rendimiento. Los orquestadores facilitan la asignación dinámica de recursos y la gestión de la concurrencia.

**Ejemplos**:

* **Ejecución paralela de tareas de carga para múltiples regiones geográficas**

    Imagina una empresa global que recopila datos de ventas de sus operaciones en Norteamérica, Europa y Asia. Un sistema de orquestación podría lanzar simultáneamente tres tareas de carga de datos, una para cada región, cada una procesando los datos de su respectiva fuente en paralelo. Esto reduce significativamente el tiempo total necesario para cargar todos los datos, en comparación con un enfoque secuencial.

* **Distribución de la transformación por particiones de tiempo usando Spark**

    Considera un *pipeline* ETL que procesa grandes volúmenes de datos transaccionales diarios. Usando Apache Spark, un orquestador podría dividir la transformación de los datos en **particiones de tiempo**. Por ejemplo, los datos de la primera mitad del día se procesan en un conjunto de *workers* de Spark, mientras que los datos de la segunda mitad se procesan simultáneamente en otro conjunto. Esto maximiza la utilización de recursos y acelera la finalización de la transformación.

* **Escalado automático de *workers* de Airflow según la demanda de procesamiento**

    Si un flujo de trabajo de datos tiene picos de demanda (por ejemplo, a fin de mes cuando se generan informes), un orquestador como Apache Airflow, integrado con plataformas de nube (AWS, GCP, Azure), puede configurarse para **escalar automáticamente** el número de *workers* disponibles. Durante los períodos de alta carga, se aprovisionan más *workers* para manejar las tareas concurrentes, y una vez que la demanda disminuye, los *workers* adicionales se desaprovisionan para optimizar costos.

## Tarea

1. Diseña un pipeline ETL en pseudocódigo que extraiga datos desde una base de datos PostgreSQL, los transforme en Spark y los cargue en un bucket S3 particionado por fecha.
2. Compara las ventajas y desventajas de ETL y ELT en el contexto de una empresa que utiliza Snowflake como data warehouse.
3. Define un DAG de Airflow simple que incluya tres tareas secuenciales: lectura de archivo, transformación y carga en base de datos.
4. Identifica tres puntos donde aplicarías modularidad en un pipeline ETL y justifica su beneficio.
5. Escribe una estrategia de orquestación para un pipeline que debe ejecutarse cada 15 minutos y detectar datos faltantes si alguna ejecución falla.

