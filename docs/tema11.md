# 1. Introducción

## Tema 1.1. Fundamentos de Big Data

**Objetivo**:

Al finalizar este tema, el estudiante comprenderá qué es Big Data, identificará sus características principales (las 'Vs'), reconocerá los desafíos y oportunidades que presenta, y diferenciará los tipos de datos y los modelos de procesamiento de datos más comunes en el contexto del Big Data.

**Introducción**:

En la era digital actual, la cantidad de datos generados crece exponencialmente cada segundo. Desde nuestras interacciones en redes sociales hasta transacciones comerciales y sensores IoT, todo produce datos. Sin embargo, no es solo el volumen lo que define el "Big Data", sino también la velocidad a la que se generan y la variedad de sus formatos. Este tema sentará las bases para entender este fenómeno, explorando sus dimensiones, las problemáticas que resuelve y las nuevas oportunidades de negocio y análisis que habilita, preparando al estudiante para adentrarse en las herramientas diseñadas para manejar este desafío.

**Desarrollo**:

El concepto de "Big Data" ha evolucionado para describir conjuntos de datos tan grandes y complejos que los métodos tradicionales de procesamiento y análisis de datos no son adecuados. Va más allá del tamaño, abarcando la complejidad y la velocidad con la que los datos son generados, procesados y analizados.

### 1.1.1 ¿Qué es Big Data?

Big Data se refiere al conjunto de tecnologías, arquitecturas y metodologías orientadas al manejo de grandes volúmenes de datos que, por su tamaño, velocidad de generación y variedad de formatos, exceden las capacidades de los sistemas tradicionales de gestión y procesamiento de datos, para realizar la labor en un tiempo razonable. Este paradigma implica trabajar con datos estructurados, semi-estructurados y no estructurados que son generados de manera continua desde múltiples fuentes, como sensores IoT, redes sociales, logs de sistemas, transacciones financieras, aplicaciones móviles, entre otros.

El objetivo de Big Data no es solo almacenar grandes cantidades de información, sino habilitar su análisis a través de herramientas y plataformas distribuidas como Apache Hadoop, Apache Spark, y sistemas en la nube (AWS, Azure, GCP), que permiten descubrir patrones, correlaciones, comportamientos y tendencias en tiempo casi real. Esto proporciona una base sólida para la toma de decisiones basada en datos (data-driven decision making) en contextos empresariales, científicos, industriales y sociales.

### 1.1.2 Las 5 **V**s del Big Data:

Estas cinco características definen intrínsecamente lo que consideramos Big Data:

##### Volumen

Se refiere a la **cantidad masiva de datos** generados por fuentes heterogéneas a una escala que supera la capacidad de almacenamiento, procesamiento y análisis de los sistemas tradicionales. El volumen en Big Data se mide en terabytes (TB), petabytes (PB), exabytes (EB) y más, y exige arquitecturas distribuidas para su gestión eficiente. Esta dimensión implica diseñar soluciones capaces de almacenar datos a gran escala (como HDFS, S3, Snowflake o BigQuery) y realizar operaciones analíticas paralelas mediante frameworks como Apache Spark o Hive.

* Walmart procesa más de un millón de transacciones de clientes por hora, lo que se traduce en más de 2.5 petabytes de datos generados diariamente.
* El **Large Hadron Collider (LHC)** del CERN genera aproximadamente 1 petabyte de datos por segundo durante sus experimentos, de los cuales solo una fracción se conserva para análisis posterior.
* Plataformas como **Facebook** almacenan y gestionan más de 300 petabytes de datos generados por interacciones de usuarios, contenido multimedia, mensajes y logs de actividad.

##### Velocidad

Hace referencia a la **rapidez con la que los datos son generados, transmitidos, recibidos y procesados**. En aplicaciones modernas, como monitoreo en tiempo real, detección de fraude, análisis de redes sociales o telemetría industrial, los datos deben ser procesados en milisegundos o segundos. Esto requiere sistemas capaces de ingerir flujos de datos continuos (streaming) utilizando tecnologías como Apache Kafka, Apache Flink, Spark Streaming o Google Dataflow, combinadas con almacenamiento en memoria y mecanismos de baja latencia.

* Twitter genera más de 500 millones de tweets al día, que deben ser indexados y disponibles casi instantáneamente para búsquedas y análisis en tiempo real.
* Los **sistemas de detección de fraude bancario** procesan miles de transacciones por segundo, aplicando modelos de machine learning en tiempo real para identificar comportamientos sospechosos.
* En **vehículos autónomos**, los sensores LIDAR, cámaras y radares generan flujos de datos que deben ser analizados al instante para tomar decisiones de navegación y evitar colisiones.

##### Variedad

Describe la **diversidad de formatos, fuentes y estructuras** de los datos disponibles en los entornos Big Data. Incluye datos estructurados (por ejemplo, registros en bases de datos relacionales), semi-estructurados (como JSON, XML, YAML), y no estructurados (texto libre, imágenes, audio, video, logs, etc.). Esta heterogeneidad plantea desafíos en la integración, transformación y análisis de datos, lo que requiere pipelines ETL flexibles y herramientas capaces de manejar múltiples formatos, como Apache NiFi, Databricks, o soluciones basadas en el modelo de Data Lake.

* Un sistema de salud digital recolecta datos de pacientes de diversas fuentes: registros médicos electrónicos (estructurados), notas de doctores (no estructurados), imágenes de resonancia magnética (no estructurados), y datos de dispositivos wearables (semi-estructurados).
* Una **empresa de medios digitales** puede manejar simultáneamente streams de video (no estructurados), logs de visualización (estructurados), comentarios de usuarios (semi-estructurados) y datos de interacción en redes sociales.
* En el **análisis de ciberseguridad**, se integran eventos de red (estructurados), reportes técnicos (no estructurados), y registros de acceso de usuarios (semi-estructurados) para detectar amenazas complejas.

##### Veracidad

Se refiere al **grado de confiabilidad, calidad y precisión de los datos**. Dado que los datos provienen de múltiples fuentes, muchas veces no controladas, pueden estar incompletos, duplicados, inconsistentes o sesgados. La veracidad es crítica para garantizar que los análisis y modelos derivados sean válidos y útiles. Requiere técnicas de limpieza, validación, reconciliación de fuentes, y gobernanza de datos, apoyadas en catálogos de datos, reglas de calidad y mecanismos de trazabilidad.

* Un análisis de sentimiento en redes sociales puede verse afectado por bots o noticias falsas, introduciendo ruido y reduciendo la veracidad de los hallazgos.
* En el sector financiero, **datos inconsistentes entre diferentes fuentes bancarias** pueden provocar errores en modelos predictivos de riesgo crediticio si no se valida adecuadamente la calidad de los datos.
* Los sistemas de sensores industriales (IoT), **lecturas defectuosas o con interferencias** pueden generar falsas alarmas o decisiones erróneas si no se filtran y validan correctamente los datos recolectados.

##### Valor

Representa la **capacidad de los datos para generar conocimiento accionable y ventaja competitiva**. El verdadero objetivo del Big Data no es acumular información, sino transformar grandes volúmenes de datos en insights que soporten decisiones estratégicas, mejoren procesos operativos o creen nuevos productos y servicios. Obtener valor requiere combinar ingeniería de datos, analítica avanzada, inteligencia artificial y visualización de datos, siempre alineado con objetivos de negocio o científicos claramente definidos.

* El análisis de los patrones de compra de clientes en una tienda en línea permite a la empresa optimizar el inventario, personalizar ofertas y mejorar la experiencia del usuario, lo que se traduce en mayores ventas.
* En el sector energético, el análisis de datos de consumo eléctrico permite implementar **modelos de tarificación dinámica**, optimizar la distribución de energía y prevenir apagones.
* En agricultura de precisión, el uso de imágenes satelitales y sensores en campo permite **maximizar el rendimiento de cultivos**, reducir el uso de insumos y tomar decisiones más informadas sobre riego y cosecha.

### 1.1.3 Desafíos del Big Data:

El manejo de Big Data presenta varios desafíos significativos:

* **Almacenamiento**: La necesidad de infraestructuras escalables y distribuidas para almacenar volúmenes masivos de datos de diferentes formatos.
* **Procesamiento**: Desarrollar sistemas capaces de procesar rápidamente datos en constante movimiento y en diferentes formatos.
* **Análisis**: Diseñar algoritmos y técnicas que puedan extraer patrones significativos de conjuntos de datos complejos y heterogéneos.
* **Seguridad y Privacidad**: Proteger la información sensible y cumplir con las regulaciones de privacidad de datos (como GDPR o CCPA) en entornos distribuidos.
* **Gobernanza de Datos**: Establecer políticas y procedimientos para la gestión de la disponibilidad, usabilidad, integridad y seguridad de los datos.

### 1.1.4 Oportunidades del Big Data:

A pesar de los desafíos, Big Data abre un abanico de oportunidades en diversas industrias:

* **Toma de Decisiones Mejorada**: Basadas en análisis de datos en tiempo real y predictivos.
* **Personalización y Experiencia del Cliente**: Entender mejor el comportamiento del cliente para ofrecer productos y servicios más relevantes.
* **Optimización de Operaciones**: Mejora de la eficiencia en la cadena de suministro, logística, mantenimiento predictivo, etc.
* **Nuevos Productos y Servicios**: Desarrollo de innovaciones basadas en la información obtenida de los datos.
* **Detección de Fraude y Riesgos**: Identificación de patrones anómalos que indican actividades fraudulentas o riesgos.
* **Investigación Científica**: Avances en medicina, astronomía, climatología, entre otros.

### 1.1.5 Tipos de Datos en Big Data:

##### Datos Estructurados

Son datos que se encuentran organizados en un **esquema rígido y predefinido**, generalmente en forma de filas y columnas, lo que permite su almacenamiento y consulta eficiente mediante **sistemas de gestión de bases de datos relacionales (RDBMS)** como MySQL, PostgreSQL u Oracle. Están gobernados por modelos tabulares (normalizados o no) con tipos de datos específicos, claves primarias/foráneas y reglas de integridad. Su estructura facilita la indexación, consultas SQL, análisis OLAP, y migraciones entre sistemas.

1. Tablas de clientes en una base de datos bancaria (`tipo_doc`, `num_doc`, `nombre_completo`, `tipo_cta`, `num_cta`, `saldo`).
2. Registros de ventas diarias en un sistema ERP (`FechaTx`, `IdProducto`, `Cantidad`, `Precio`).
3. Inventario de una tienda en formato CSV con columnas bien definidas (`ID`, `descripción`, `categoría`, `stock`).
4. La base de datos de pacientes con campos como `ID_Paciente`, `Nombre`, `Fecha_Nacimiento`, `Grupo_Sanguineo`.

##### Datos Semi-estructurados

Son datos que **no se ajustan completamente a un modelo relacional tradicional**, pero contienen **etiquetas, delimitadores o estructuras jerárquicas** que permiten cierta organización y comprensión automatizada. A menudo se representan en formatos legibles por máquinas como **JSON, XML o YAML**, que pueden modelar relaciones complejas (análogas a objetos o diccionarios) y son comunes en APIs, configuraciones y flujos de integración. Su análisis requiere herramientas capaces de interpretar dicha estructura, como motores NoSQL (MongoDB, Elasticsearch) o lenguajes con soporte nativo para parsing (Python, Java).

1. Documentos XML que describen transacciones electrónicas entre sistemas: [facturación electrónica](ejemplo_xml.md)
2. [Logs de acceso web](ejemplo_log.md) en formato Apache/Nginx donde cada línea sigue una estructura repetitiva (IP, timestamp, recurso, código de estado).
3. Registros de sensores de equipos médicos ([monitores de signos vitales](ejemplo_json.md)) en formato JSON, donde cada registro puede tener diferentes campos dependiendo del tipo de sensor.

##### Datos No Estructurados

Son datos que **carecen de un modelo predefinido o estructura fija**, lo que los hace difíciles de organizar y analizar mediante métodos tradicionales. Representan la **mayor parte del volumen de datos generados a nivel global**, y suelen requerir **técnicas avanzadas de procesamiento** como análisis de texto (NLP), reconocimiento de imágenes, extracción de entidades, o clasificación mediante machine learning. Su almacenamiento se realiza comúnmente en sistemas distribuidos o Data Lakes.

1. Publicaciones en redes sociales que combinan texto libre, emojis, hashtags e imágenes.
2. Archivos de video provenientes de cámaras de seguridad sin etiquetas asociadas.
3. Grabaciones de audio de llamadas en centros de atención al cliente.
4. Imágenes de rayos X, grabaciones de voz de consultas médicas, informes médicos en formato de texto libre.

### 1.1.6 Modelos de Procesamiento de Datos

##### Procesamiento Batch (por Lotes)

Es un enfoque de procesamiento de datos que consiste en **acumular grandes volúmenes de información durante un intervalo de tiempo determinado** para luego ser procesados de forma **masiva y secuencial**, sin requerir intervención humana durante la ejecución. Este tipo de procesamiento es ideal para cargas de trabajo donde la **latencia no es crítica**, y permite realizar tareas computacionalmente intensivas como agregaciones, transformaciones, limpieza y carga de datos históricos. Se implementa comúnmente con herramientas como **Apache Hadoop, Apache Spark en modo batch, AWS Glue, o Azure Data Factory**.

1. Procesamiento nocturno de transacciones bancarias para generar extractos y actualizar balances.
2. Carga diaria de datos históricos desde un sistema transaccional a un data warehouse para análisis BI.
3. Generación mensual de reportes de nómina y deducciones en una empresa.

##### Procesamiento en Tiempo Real (Streaming)

Este modelo se refiere al procesamiento de **flujos de datos de forma continua e inmediata**, a medida que los datos son generados o ingresan al sistema. Está diseñado para manejar eventos de alto volumen y baja latencia, permitiendo a los sistemas reaccionar casi en tiempo real. Utiliza frameworks especializados como **Apache Kafka, Apache Flink, Apache Spark Streaming, Google Dataflow o AWS Kinesis**, y se aplica en contextos donde **la inmediatez en la toma de decisiones es crítica**.

1. Detección de fraudes en tarjetas de crédito, analizando patrones de transacciones mientras ocurren.
2. Monitoreo de infraestructura tecnológica (DevOps/Observabilidad) para detectar errores o picos de carga en servidores.
3. Recomendaciones personalizadas en plataformas de streaming o e-commerce, basadas en el comportamiento del usuario en tiempo real.

##### Procesamiento Interactivo (Ad-Hoc/Exploratorio)

Se refiere a la capacidad de los usuarios para **ejecutar consultas dinámicas y obtener resultados rápidamente**, con el objetivo de **explorar, analizar o visualizar datos en forma directa**, sin depender de procesos predefinidos o programación previa. Este tipo de procesamiento requiere **sistemas optimizados para baja latencia y acceso aleatorio eficiente**, como motores SQL distribuidos o motores de exploración columnar (por ejemplo, **Presto, Trino, Google BigQuery, Snowflake, Dremio, Databricks SQL**). Es fundamental en entornos analíticos donde los científicos o analistas de datos realizan exploración iterativa.

1. Consultas ad-hoc sobre un data lake para identificar patrones de ventas por región y temporada.
2. Exploración de grandes volúmenes de logs en tiempo casi real para diagnosticar errores de aplicaciones.
3. Análisis interactivo de cohortes de usuarios en herramientas BI como Tableau o Power BI conectadas a un motor distribuido.

## Tarea

Para consolidar tu comprensión sobre los fundamentos de Big Data, investiga y responde las siguientes preguntas. Documenta tus respuestas y las fuentes consultadas.

1. **Explora las "V" adicionales**: Además de Volumen, Velocidad, Variedad, Veracidad y Valor, algunos expertos proponen otras "V" (como Variabilidad, Visualización, Viabilidad, etc.). Elige al menos dos "V" adicionales y explica su relevancia en el contexto del Big Data actual.
2. **Tecnologías emergentes para Big Data**: Investiga una tecnología o paradigma emergente (aparte de Spark, que veremos en el siguiente tema) que esté ganando tracción en el ámbito del Big Data (ej. Lakehouses, Data Meshes, Procesamiento Serverless, etc.). Describe brevemente qué problema resuelve y cómo se integra con el ecosistema Big Data existente.
3. **Comparación de arquitecturas Big Data**: Investiga las diferencias fundamentales entre una arquitectura tradicional de Data Warehouse y una arquitectura de Data Lake. ¿Cuándo sería más apropiado usar una u otra, o una combinación de ambas (Data Lakehouse)?

