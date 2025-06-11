# 3. Arquitectura y Diseño de Flujos ETL

## Tema 3.6. Seguridad en ETL y Protección de Datos

**Objetivo**:

Asegurar la confidencialidad, integridad y acceso controlado a la información dentro de los pipelines ETL, mediante técnicas de encriptación, gestión segura de credenciales, políticas de control de acceso, auditoría de operaciones y cumplimiento normativo.

**Introducción**:

Los flujos ETL manejan información sensible en distintos puntos del ciclo de vida de los datos: desde su extracción en fuentes externas hasta su carga en almacenes analíticos o lagos de datos. En este contexto, la seguridad no es opcional. Es imprescindible diseñar procesos ETL que protejan los datos en todo momento, mitigando riesgos de fuga, alteración o uso indebido. La seguridad en pipelines ETL debe considerarse desde el inicio del diseño arquitectónico y mantenerse en cada etapa del proceso.

**Desarrollo**:

Este tema abarca las prácticas y herramientas clave para garantizar la protección de los datos en los pipelines ETL. Desde la encriptación en tránsito y en reposo, pasando por el uso adecuado de gestores de secretos, hasta la implementación de políticas de control de acceso y auditoría, el objetivo es dotar a los estudiantes de una visión integral de seguridad. Además, se introducen las principales normativas internacionales que regulan el tratamiento de datos, aportando un marco legal y ético al diseño de soluciones de datos.

### 3.6.1 Encriptación en Tránsito (TLS/SSL) y en Reposo (AES, KMS)

La **encriptación** es una piedra angular en la **seguridad de los datos**, asegurando su **confidencialidad e integridad** tanto durante su **movimiento entre sistemas (en tránsito)** como cuando están **almacenados (en reposo)**. Este subtema explorará los **mecanismos de cifrado modernos** y su **implementación práctica** en el contexto de los **pipelines ETL (Extracción, Transformación y Carga)**, vital para cualquier arquitectura de datos robusta.

##### Encriptación en Tránsito (TLS/SSL)

La **encriptación en tránsito** se enfoca en proteger los datos mientras **viajan a través de una red**. Ya sea que los datos se extraigan de fuentes externas, se consuman a través de APIs, o se escriban en sistemas de almacenamiento en red, el objetivo es evitar que actores maliciosos los intercepten o modifiquen. 

El protocolo **TLS (Transport Layer Security)**, sucesor de SSL, es el estándar de oro para lograr esta protección. TLS establece un **canal de comunicación seguro** entre dos aplicaciones, garantizando la **autenticación de los extremos**, la **confidencialidad de los datos** (mediante cifrado) y la **integridad de los datos** (asegurando que no han sido alterados).

**Configuración de Spark para leer desde una base de datos PostgreSQL usando conexiones TLS**:

Cuando Spark se conecta a una base de datos externa como PostgreSQL, es crucial que la comunicación esté cifrada. Esto protege credenciales y datos sensibles durante el proceso de extracción.

Para habilitar TLS/SSL en una conexión JDBC de Spark a PostgreSQL, se deben configurar propiedades específicas en la cadena de conexión. Estas propiedades instruyen al driver JDBC para que establezca una conexión segura, validando el certificado del servidor PostgreSQL.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SecurePostgreSQLRead").getOrCreate()

# Configuración de la conexión JDBC segura a PostgreSQL
jdbc_url = "jdbc:postgresql://your_db_host:5432/your_database"
# Es crucial incluir 'ssl=true' y, opcionalmente, 'sslmode=require' o 'sslmode=verify-full'
# para asegurar que el certificado del servidor sea validado.
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require" # 'require' fuerza SSL, 'verify-full' añade verificación de certificado
}

# Leer datos de la tabla de PostgreSQL de forma segura
df = spark.read.jdbc(url=jdbc_url, table="your_table", properties=connection_properties)

df.show()
spark.stop()
```
    
Para `sslmode=verify-full`, necesitarás un TrustStore con el certificado de la CA que emitió el certificado de tu servidor PostgreSQL, o el propio certificado del servidor, y configurar las JVM options de Spark para que lo usen.

**Consumo de APIs REST que exigen HTTPS con certificados válidos**:

Muchas fuentes de datos externas se exponen a través de APIs REST. Si estas APIs manejan información sensible, es imperativo que requieran HTTPS (HTTP sobre TLS/SSL) para garantizar la comunicación segura.

Al interactuar con una API HTTPS, el cliente (nuestro pipeline ETL) verifica el certificado SSL/TLS presentado por el servidor para asegurar su autenticidad y la validez de la conexión cifrada. Esto previene ataques "Man-in-the-Middle". Bibliotecas HTTP modernas manejan la validación de certificados de forma predeterminada, pero es importante comprender cómo funcionan y cómo configurar excepciones o certificados personalizados si es necesario (aunque esto último debe hacerse con precaución).

```python
import requests
import json

api_url = "https://api.example.com/data" # URL que exige HTTPS
headers = {"Authorization": "Bearer your_api_token"}

try:
    # requests verifica certificados SSL/TLS por defecto
    response = requests.get(api_url, headers=headers, timeout=10)
    response.raise_for_status() # Lanza una excepción para códigos de estado HTTP erróneos (4xx o 5xx)

    data = response.json()
    print("Datos recibidos de la API de forma segura:")
    print(json.dumps(data, indent=2))

except requests.exceptions.SSLError as e:
    print(f"Error SSL: Problema con el certificado TLS/SSL. {e}")
except requests.exceptions.ConnectionError as e:
    print(f"Error de conexión: No se pudo conectar a la API. {e}")
except requests.exceptions.Timeout as e:
    print(f"Error de tiempo de espera: La solicitud a la API tardó demasiado. {e}")
except requests.exceptions.RequestException as e:
    print(f"Error general de la solicitud: {e}")
```

**Comunicación segura entre tareas de Airflow mediante redes privadas virtuales y TLS**:

En entornos de orquestación como Apache Airflow, las tareas a menudo se ejecutan en diferentes nodos o incluso en servicios en la nube. Proteger la comunicación entre el programador, los workers y las bases de datos de metadatos es vital.

Una estrategia común es desplegar Airflow dentro de una **red privada virtual (VPN)** o una **Virtual Private Cloud (VPC)** en la nube, lo que segmenta el tráfico de red y lo aísla de la internet pública. Dentro de esta red privada, la comunicación interna (por ejemplo, entre el scheduler de Airflow y los workers, o entre los workers y la base de datos de metadatos de Airflow) aún puede beneficiarse de TLS para mayor granularidad en la seguridad. Esto asegura que, incluso si un actor malicioso gana acceso a la red privada, los datos en tránsito siguen estando cifrados.

* **Airflow en AWS**: Desplegar un cluster de Airflow (usando EC2, ECS o EKS) dentro de una VPC. Configurar **Security Groups** y **Network ACLs** para restringir el tráfico solo a puertos y orígenes necesarios. La base de datos de metadatos (RDS PostgreSQL/MySQL) debe configurarse para aceptar solo conexiones SSL/TLS, y el scheduler y los workers de Airflow deben configurarse para usar estas conexiones seguras.

* **Airflow en Azure/GCP**: Similarmente, utilizar **Virtual Networks (Azure)** o **VPCs (GCP)** con configuraciones de firewall y enrutamiento para aislar el entorno de Airflow. Los servicios de bases de datos gestionadas (Azure Database for PostgreSQL/MySQL, Cloud SQL) ofrecen conexiones SSL/TLS obligatorias o recomendadas.

* **Kubernetes con mTLS**: Si Airflow se ejecuta en Kubernetes, se puede implementar **mTLS (mutual TLS)** entre los diferentes pods (scheduler, workers, webserver) usando una **Service Mesh** como Istio, lo que proporciona encriptación de extremo a extremo y autenticación bidireccional.

##### Encriptación en Reposo (AES, KMS)

La **encriptación en reposo** se refiere a la protección de los datos cuando están **almacenados en dispositivos de almacenamiento persistente**, como discos duros, bases de datos o servicios de almacenamiento en la nube. El objetivo es prevenir el acceso no autorizado a los datos si el almacenamiento físico es comprometido. 

El **cifrado simétrico**, particularmente el estándar **AES (Advanced Encryption Standard)** con una longitud de clave de **256 bits (AES-256)**, es ampliamente utilizado por su robustez y eficiencia. En entornos de nube, los **Servicios de Gestión de Claves (KMS - Key Management Service)** son esenciales para gestionar de forma segura las claves de cifrado, rotarlas, auditar su uso y aplicar políticas de acceso.

**Almacenamiento de archivos Parquet cifrados en S3 con claves gestionadas por AWS KMS**:

Amazon S3 es un almacén de objetos altamente escalable y duradero. Almacenar datos en S3 requiere un cifrado en reposo para cumplir con los requisitos de seguridad y cumplimiento.

AWS S3 ofrece varias opciones de cifrado en reposo. La más segura y recomendada es el **cifrado del lado del servidor con claves administradas por AWS KMS (SSE-KMS)**. Con SSE-KMS, S3 cifra los objetos usando una clave de datos única para cada objeto, y esa clave de datos se cifra con una **clave maestra de cliente (CMK)** almacenada en KMS. KMS proporciona una gestión centralizada de las CMK, incluyendo la rotación automática, políticas de uso detalladas y registros de auditoría de acceso a las claves.

```python
# PySpark y Boto3 para S3/KMS
from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder \
    .appName("EncryptedParquetS3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

bucket_name = "your-encrypted-data-bucket"
kms_key_arn = "arn:aws:kms:your-region:your-account-id:key/your-kms-key-id"
output_path = f"s3a://{bucket_name}/encrypted_data/my_encrypted_data.parquet"

# Crear un DataFrame de ejemplo
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)

# Escribir el DataFrame a S3, especificando la clave KMS para el cifrado
# Spark, a través de S3A, puede ser configurado para usar SSE-KMS
df.write \
    .mode("overwrite") \
    .option("sse.kms.keyId", kms_key_arn) \
    .parquet(output_path)

print(f"Datos escritos cifrados en S3 en: {output_path}")

# Para verificar el cifrado en S3 (opcional, usando boto3)
s3_client = boto3.client('s3')
try:
    obj_head = s3_client.head_object(Bucket=bucket_name, Key="encrypted_data/my_encrypted_data.parquet/_SUCCESS") # El archivo _SUCCESS indica el éxito de la escritura
    encryption_header = obj_head.get('ServerSideEncryption')
    kms_key_used = obj_head.get('SSEKMSKeyId')
    print(f"Cifrado de objeto en S3: {encryption_header}")
    print(f"Clave KMS utilizada: {kms_key_used}")
except Exception as e:
    print(f"Error al verificar el objeto en S3: {e}")

spark.stop()
```

El rol de IAM debe tener permisos para S3 (s3:PutObject, s3:GetObject) y KMS (kms:GenerateDataKey, kms:Decrypt).

**Configuración de HDFS para cifrar automáticamente bloques de datos con claves rotativas**:

Para entornos on-premise o híbridos que utilizan Apache Hadoop HDFS (Hadoop Distributed File System), el cifrado en reposo es igualmente crucial.

HDFS soporta **zonas de cifrado (Encryption Zones)**, que son directorios en HDFS cuyo contenido se cifra automáticamente al ser escrito y se descifra al ser leído. Cada zona de cifrado está asociada a una **clave de zona (EZ Key)** gestionada por un sistema de gestión de claves centralizado como **Apache Ranger KMS** o **HashiCorp Vault**. Cuando se escribe un archivo en una zona de cifrado, HDFS genera una clave de cifrado de datos (Data Encryption Key - DEK) para el archivo, cifra la DEK con la EZ Key, y almacena la DEK cifrada junto con el archivo. Los bloques de datos del archivo se cifran con la DEK. Esto permite la rotación de claves y la revocación granular.

* **Configuración (Conceptual en HDFS - No es código ejecutable)**:

1.  Configurar un KMS (Key Management Server) para HDFS: Esto implica configurar `kms-site.xml` en los nodos de HDFS y el KMS, apuntando a una base de datos segura para almacenar las claves.
2.  Crear una Clave de Zona (EZ Key) en el KMS:

    ```bash
    hdfs kms createKey my_encryption_zone_key
    ```

3.  Crear una Zona de Cifrado en HDFS:

    ```bash
    hdfs crypto createZone -path /user/encrypted_data -keyName my_encryption_zone_key
    ```

    Cualquier archivo escrito en `/user/encrypted_data` (o subdirectorios) se cifrará automáticamente utilizando `my_encryption_zone_key`. Los usuarios necesitarán permisos para acceder a esta clave en el KMS para poder leer los datos.

**Uso de Google Cloud KMS para cifrar datasets en BigQuery**:

Google BigQuery es un data warehouse sin servidor altamente escalable. Aunque BigQuery cifra los datos en reposo por defecto (cifrado gestionado por Google), los usuarios pueden proporcionar sus propias claves de cifrado gestionadas por el cliente (CMEK) a través de Google Cloud KMS para un control adicional.

Con **CMEK (Customer-Managed Encryption Keys)** en BigQuery, tú controlas la clave de cifrado que se utiliza para proteger tus datos. BigQuery utiliza esta CMEK de Cloud KMS para cifrar la clave de cifrado de datos (DEK) del dataset, y esta DEK a su vez se usa para cifrar los datos reales. Esto te da control sobre la vida útil de la clave, su rotación y los permisos de acceso a ella. Es útil para cumplir con requisitos de cumplimiento normativo específicos o para añadir una capa extra de seguridad.

```python
# usando Google Cloud Client Libraries
from google.cloud import bigquery
from google.cloud import kms_v1

# Configuración de tu proyecto y clave KMS
project_id = "your-gcp-project-id"
dataset_id = "your_encrypted_dataset"
table_id = "your_encrypted_table"
# La clave KMS debe estar en la misma región que tu dataset de BigQuery
kms_key_name = "projects/your-gcp-project-id/locations/your-region/keyRings/your-key-ring/cryptoKeys/your-crypto-key"

client = bigquery.Client(project=project_id)

# 1. Crear o actualizar un Dataset con CMEK
dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
dataset.location = "US"  # Asegúrate de que la región coincida con tu clave KMS

# Asignar la clave KMS al dataset
dataset.default_kms_key_name = kms_key_name

try:
    dataset = client.create_dataset(dataset, timeout=30)
    print(f"Dataset '{dataset_id}' creado/actualizado con CMEK.")
except Exception as e:
    print(f"Error al crear/actualizar dataset (ya existe?): {e}")
    dataset = client.get_dataset(dataset) # Si ya existe, obténlo para continuar


# 2. Crear una Tabla dentro del Dataset con CMEK (hereda del dataset o se puede especificar)
# Si el dataset tiene una CMEK, la tabla la heredará automáticamente.
# También puedes especificarla directamente para la tabla si no quieres heredarla.
table_ref = dataset.table(table_id)
schema = [
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
]
table = bigquery.Table(table_ref, schema=schema)

# Opcional: especificar CMEK directamente para la tabla si es diferente a la del dataset
# table.encryption_configuration = bigquery.EncryptionConfiguration(kms_key_name=kms_key_name)

try:
    table = client.create_table(table)
    print(f"Tabla '{table_id}' creada con CMEK.")
except Exception as e:
    print(f"Error al crear tabla (ya existe?): {e}")

# 3. Insertar datos en la tabla (serán cifrados automáticamente por BigQuery con la CMEK)
rows_to_insert = [
    {"name": "Juan", "age": 30},
    {"name": "Maria", "age": 25},
]

errors = client.insert_rows_json(table, rows_to_insert)
if errors:
    print(f"Errores al insertar filas: {errors}")
else:
    print(f"Filas insertadas en '{table_id}' (cifradas con CMEK).")

```

Asegúrate de que la cuenta de servicio que ejecuta el código tenga los roles `roles/bigquery.dataEditor` y `roles/cloudkms.viewer` para BigQuery, y `roles/cloudkms.cryptoKeyEncrypterDecrypter` para la clave KMS.


### 3.6.2 Gestión de secretos y credenciales 

La gestión segura de **secretos y credenciales** es un pilar fundamental en la arquitectura de datos, especialmente en entornos de procesamiento masivo. El uso de credenciales en texto plano o incrustadas directamente en el código de scripts y aplicaciones presenta un riesgo de seguridad inaceptable. Este subtema aborda en detalle los mecanismos y las mejores prácticas para almacenar, distribuir y acceder de forma segura a información sensible, como contraseñas de bases de datos, tokens de API, claves de cifrado y certificados. El objetivo es asegurar que solo las entidades autorizadas tengan acceso a estos secretos, en el momento y lugar adecuados, minimizando la superficie de ataque y garantizando la integridad de los sistemas de datos.

##### Gestores de secretos y credenciales

Los **gestores de secretos** son herramientas especializadas que proporcionan una solución centralizada y segura para almacenar, gestionar y distribuir secretos. Estos servicios no solo protegen la información sensible, sino que también ofrecen funcionalidades avanzadas como la rotación automática de credenciales, el control de versiones, la auditoría de accesos y la integración con sistemas de gestión de identidades y accesos (IAM). Esto permite a los equipos de desarrollo y operaciones implementar un enfoque de "confianza cero" para las credenciales, donde el acceso se otorga solo cuando es estrictamente necesario y por un período limitado.

**Integración de Apache Airflow con AWS Secrets Manager para acceder a credenciales de bases de datos.**

Apache Airflow es una plataforma robusta para orquestar flujos de trabajo de datos. En un entorno de producción, los *Directed Acyclic Graphs* (DAGs) de Airflow a menudo necesitan conectarse a diversas bases de datos, APIs o servicios externos. Almacenar las credenciales directamente en los archivos de configuración de Airflow o en variables de entorno es una práctica insegura. **AWS Secrets Manager** permite almacenar de forma segura estas credenciales y Airflow puede configurarse para recuperarlas dinámicamente en tiempo de ejecución, eliminando la necesidad de codificarlas en el DAG.

Supongamos que tenemos una base de datos PostgreSQL cuyas credenciales (nombre de usuario y contraseña) están almacenadas en AWS Secrets Manager bajo el nombre `my_rds_db_credentials`.

* En AWS Secrets Manager: Crear un secreto con un par clave-valor, por ejemplo: 

```json
{
    "username": "myuser",
    "password": "mYsEcUrEpAsSwOrD"
}
```

* Configuración de Airflow: Se puede configurar Airflow para usar un `SecretsBackend` que se integre con AWS Secrets Manager. Esto generalmente se hace ajustando el `airflow.cfg` o mediante variables de entorno:

```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

* En un DAG de Airflow: Una vez configurado el backend, Airflow puede resolver las conexiones y variables directamente desde Secrets Manager. Por ejemplo, si tu conexión a la base de datos se llama `my_rds_conn` en Airflow, y has almacenado la conexión en AWS Secrets Manager como `airflow/connections/my_rds_conn`, Airflow la resolverá automáticamente.

```python
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='read_from_rds_secrets',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    read_data_task = PostgresOperator(
        task_id='read_data',
        postgres_conn_id='my_rds_conn', # Airflow buscará esta conexión en Secrets Manager
        sql="SELECT * FROM my_table LIMIT 10;"
    )
```
    
Airflow recuperará las credenciales de `my_rds_conn` desde Secrets Manager, evitando que las credenciales sensibles sean expuestas en el código del DAG o en la configuración de Airflow.

**Uso de HashiCorp Vault para otorgar accesos temporales a procesos Spark en clusters Kubernetes.**

**HashiCorp Vault** es una herramienta de gestión de secretos robusta y altamente segura que permite almacenar, acceder y distribuir secretos de manera programática. En un entorno de microservicios o contenedores orquestados por Kubernetes, los trabajos de Spark a menudo necesitan acceder a recursos externos (como sistemas de almacenamiento, bases de datos o servicios de mensajería). Vault puede generar credenciales temporales y de corta duración para estos procesos, lo que reduce drásticamente el riesgo de exposición de credenciales a largo plazo. Esto es crucial en entornos dinámicos donde los pods de Spark pueden ser efímeros.

Consideremos un trabajo de Spark que necesita acceder a un bucket de S3. En lugar de usar credenciales de S3 estáticas, podemos configurar Vault para generar credenciales de AWS temporales que el trabajo de Spark pueda usar.

* Configuración de Vault: Vault se configura con un `secret engine` para AWS que puede generar credenciales IAM temporales.

```bash
vault secrets enable aws
vault write aws/config/root \
    access_key=AKIA... \
    secret_key=...

vault write aws/roles/spark-s3-access \
    credential_type=iam_user \
    policy_arns=arn:aws:iam::123456789012:policy/S3ReadOnlyAccess
```

* Pod de Spark en Kubernetes: El pod de Spark se configura para autenticarse con Vault (por ejemplo, usando Kubernetes Service Account Token authentication) y obtener las credenciales de AWS temporales.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-s3-job
spec:
  template:
    spec:
      serviceAccountName: spark-sa
      containers:
      - name: spark-driver
        image: your-spark-image
        command: ["/bin/bash", "-c"]
        args:
          - |
            # Authenticate with Vault and get temporary AWS credentials
            VAULT_ADDR="http://vault.example.com:8200"
            VAULT_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)

            RESPONSE=$(curl -s --request POST \
                --data "{\"jwt\": \"$VAULT_TOKEN\", \"role\": \"spark-s3-access\"}" \
                "$VAULT_ADDR/v1/auth/kubernetes/login")
            VAULT_CLIENT_TOKEN=$(echo $RESPONSE | jq -r .auth.client_token)

            AWS_CREDS_RESPONSE=$(curl -s --header "X-Vault-Token: $VAULT_CLIENT_TOKEN" \
                "$VAULT_ADDR/v1/aws/creds/spark-s3-access")

            export AWS_ACCESS_KEY_ID=$(echo $AWS_CREDS_RESPONSE | jq -r .data.access_key)
            export AWS_SECRET_ACCESS_KEY=$(echo $AWS_CREDS_RESPONSE | jq -r .data.secret_key)
            export AWS_SESSION_TOKEN=$(echo $AWS_CREDS_RESPONSE | jq -r .data.security_token)

            # Now run your Spark job using these temporary credentials
            /opt/spark/bin/spark-submit \
              --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
              --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
              --conf spark.hadoop.fs.s3a.session.token=$AWS_SESSION_TOKEN \
              your_spark_app.py
        volumeMounts:
          - name: vault-token
            mountPath: /var/run/secrets/kubernetes.io/serviceaccount
      volumes:
        - name: vault-token
          projected:
            sources:
              - serviceAccountToken:
                  path: token
      restartPolicy: OnFailure
```

Este enfoque asegura que las credenciales solo existen en la memoria del pod de Spark durante su ejecución y son automáticamente revocadas por Vault después de un período de tiempo definido.

**Enmascaramiento dinámico de variables sensibles en Databricks notebooks mediante Azure Key Vault.**

**Databricks** es una plataforma unificada de datos y AI que se utiliza ampliamente para el procesamiento de Big Data y el machine learning. Los *notebooks* de Databricks, escritos en lenguajes como Python, Scala o SQL, a menudo necesitan acceder a bases de datos, APIs o servicios de almacenamiento que requieren credenciales. **Azure Key Vault** es el servicio de gestión de secretos de Azure, que ofrece una forma segura de almacenar y acceder a secretos, claves y certificados. La integración de Databricks con Azure Key Vault permite a los usuarios acceder a secretos de forma segura desde sus notebooks sin exponerlos directamente en el código o en la interfaz del notebook.

Imaginemos que necesitamos acceder a una base de datos SQL Server desde un notebook de Databricks y las credenciales están en Azure Key Vault.

* En Azure Key Vault: Crear un secreto llamado `sql-server-password` con la contraseña de la base de datos.

* Configuración de Databricks: En Databricks, se crea un `Secret Scope` respaldado por Azure Key Vault. Esto vincula un ámbito de secretos de Databricks con un Key Vault específico.

```bash
# Usando la CLI de Databricks para crear un secret scope
databricks secrets create-scope \
    --scope my-keyvault-scope \
    --scope-backend-type AZURE_KEYVAULT \
    --resource-id /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.KeyVault/vaults/<key-vault-name> \
    --dns-name https://<key-vault-name>.vault.azure.net/
```

* En un Notebook de Databricks (Python): Una vez configurado el `secret scope`, puedes acceder a los secretos de forma segura desde tu notebook utilizando la utilidad `dbutils.secrets`.

```python
# Acceder a la contraseña de SQL Server desde Azure Key Vault
db_password = dbutils.secrets.get(scope="my-keyvault-scope", key="sql-server-password")
db_user = "your_sql_user" # Este podría venir de otro secreto o ser fijo
db_host = "your_sql_server.database.windows.net"
db_name = "your_database_name"

# Construir la cadena de conexión de forma segura
jdbc_url = f"jdbc:sqlserver://{db_host}:1433;database={db_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Leer datos de la base de datos usando Spark
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "your_table") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()

df.display()
```

`dbutils.secrets.get()` enmascara automáticamente el valor del secreto cuando se muestra en la salida del notebook, garantizando que la contraseña nunca sea visible.

##### Buenas prácticas en el uso de secretos

La implementación de gestores de secretos es solo la primera parte de una estrategia de seguridad integral. Es igualmente crucial adoptar **buenas prácticas** operativas que refuercen la postura de seguridad y mitiguen los riesgos asociados con el uso de credenciales. Estas prácticas se centran en el principio de "menor privilegio" y en la visibilidad completa sobre el ciclo de vida de los secretos.

**Configuración de políticas para que los secretos tengan vigencia limitada.**

Los secretos no deberían tener una vida útil ilimitada. Al implementar políticas de **rotación periódica** y **vigencia limitada**, se reduce significativamente la ventana de oportunidad para que un secreto comprometido sea explotado. La rotación automática es una característica clave ofrecida por la mayoría de los gestores de secretos, que debería ser aprovechada al máximo.

* **AWS Secrets Manager**: Puedes configurar una rotación automática para los secretos. Para una credencial de base de datos RDS, AWS Secrets Manager puede integrarse directamente con RDS para rotar la contraseña cada X días.

```json
# Fragmento de configuración de rotación en AWS Secrets Manager (vía SDK/CLI)
{
  "RotationRules": {
    "AutomaticallyRotateAfterDays": 30, # Rotar cada 30 días
    "Duration": "30m", # La rotación debe completarse en 30 minutos
    "ScheduleExpression": "cron(0 0 ? * MON *)" # Rotar los lunes a medianoche
  },
  "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:SecretsManagerRDSMySQLRotationLambda"
}
```

Esto asegura que, incluso si una credencial se ve comprometida, su validez es temporal, limitando el daño potencial.

* **HashiCorp Vault**: Define *leases* o duraciones de vida para las credenciales generadas.

```bash
vault write aws/roles/spark-s3-access \
    credential_type=iam_user \
    policy_arns=arn:aws:iam::123456789012:policy/S3ReadOnlyAccess \
    default_lease_ttl="1h" \
    max_lease_ttl="24h"
```

Aquí, las credenciales generadas por este rol expirarán después de 1 hora por defecto, y no podrán ser usadas por más de 24 horas, forzando su re-obtención.

**Restricción de acceso a claves solo a servicios autenticados.**

El principio de **menor privilegio** es fundamental. El acceso a los secretos debe ser granular, basado en roles y solo otorgado a las entidades (usuarios, servicios, aplicaciones) que lo necesitan explícitamente para realizar sus funciones. Esto implica el uso de mecanismos de autenticación robustos y políticas de autorización detalladas en el gestor de secretos.

* **Azure Key Vault**: Uso de políticas de acceso o Azure Role-Based Access Control (RBAC).

```json
# Política de acceso en Azure Key Vault para una aplicación/Service Principal
{
  "id": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.KeyVault/vaults/<key-vault-name>/accessPolicies/<policy-id>",
  "properties": {
    "objectId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", # Object ID de un Service Principal de Azure AD
    "tenantId": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy", # Tenant ID de Azure AD
    "permissions": {
      "secrets": ["get", "list"] # Solo permisos para obtener y listar secretos
    }
  }
}
```
        
Esta política asegura que solo un `Service Principal` específico (que representa una aplicación o servicio) puede recuperar y listar los secretos del Key Vault, y no tiene permisos para modificarlos o eliminarlos.

* **AWS Secrets Manager**: Integración con AWS IAM.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "arn:aws:secretsmanager:us-east-1:123456789012:secret:my_rds_db_credentials-*"
        }
    ]
}
```

Esta política IAM puede ser adjuntada a un rol que asuma una instancia EC2 o un pod de EKS, permitiendo que solo esa entidad específica acceda a un secreto particular en Secrets Manager.

**Log de accesos a secretos y alertas ante uso no autorizado.**

La **auditoría** es vital para la seguridad. Cada acceso a un secreto, ya sea exitoso o fallido, debe ser registrado. Estos registros, o logs de auditoría, son esenciales para el monitoreo de seguridad, la detección de anomalías y la respuesta a incidentes. La integración con sistemas de monitoreo y alerta permite una reacción rápida ante cualquier intento de acceso no autorizado o un patrón de uso sospechoso.

* **AWS Secrets Manager con CloudTrail y CloudWatch**: AWS CloudTrail registra todas las llamadas a la API de Secrets Manager. Estos eventos se pueden enviar a CloudWatch Logs, donde se pueden crear métricas y alarmas.

```json
# Ejemplo de filtro de métrica en CloudWatch Logs para intentos fallidos de acceso a secretos
{
  "filterPattern": "{ ($.eventName = GetSecretValue || $.eventName = DescribeSecret) && $.errorCode = AccessDenied }",
  "metricTransformations": [
    {
      "metricName": "SecretAccessDeniedCount",
      "metricNamespace": "SecretsManagerMetrics",
      "metricValue": "1"
    }
  ]
}
```

Se puede configurar una alarma de CloudWatch que se active cuando la métrica `SecretAccessDeniedCount` supere un umbral determinado, notificando al equipo de seguridad.

* **HashiCorp Vault con Audit Devices**: Vault tiene "audit devices" que registran todas las solicitudes y respuestas.

```bash
vault audit enable file file_path=/var/log/vault_audit.log
vault audit enable syslog
```
        
Estos logs se pueden enviar a un sistema de gestión de eventos e información de seguridad (SIEM) como Splunk o ELK Stack para su análisis y correlación con otros eventos de seguridad.

### 3.6.3 Control de Acceso Basado en Roles (RBAC) y Políticas

El control de acceso es fundamental para garantizar que solo las **entidades autorizadas** (usuarios o servicios) puedan interactuar con los *pipelines* ETL y sus datos subyacentes. Esto es crucial para la **seguridad, la integridad y la confidencialidad** de la información en entornos de Big Data.

##### Implementación de RBAC (Role-Based Access Control)

El **RBAC (Control de Acceso Basado en Roles)** es un método eficiente para gestionar permisos al agruparlos y asignarlos a **roles específicos** (por ejemplo, Analista de Datos, Ingeniero de Datos, Administrador de Infraestructura). Esto simplifica la administración de la seguridad y reduce el riesgo de errores.

**Definición de roles en Apache Airflow para limitar la ejecución de DAGs**:

En Airflow, los roles se utilizan para controlar qué usuarios pueden ver, modificar o ejecutar ciertos DAGs (Directed Acyclic Graphs). Esto es vital para asegurar que solo el personal calificado pueda desplegar y operar *pipelines* de producción.

Airflow proporciona una interfaz de usuario y una API para definir roles y asignar permisos a los usuarios. Los permisos pueden ser muy granulares, permitiendo controlar el acceso a DAGs específicos, a vistas de la interfaz de usuario, o a acciones como pausar, reanudar o activar *runs* de DAGs. Al definir un rol, se especifican las acciones que los usuarios con ese rol pueden realizar sobre los recursos de Airflow.

* Rol **Ingeniero de Datos**: Tiene permisos para crear, modificar y ejecutar DAGs en entornos de desarrollo y pruebas. Puede ver el estado de todos los DAGs de producción, pero no puede modificarlos ni activarlos manualmente en producción.

* Rol **Operaciones de Datos**: Tiene permisos para monitorear y activar *runs* de DAGs en producción, así como para gestionar el estado de los *tasks*. No puede modificar la definición de los DAGs.

* Rol **Analista de Datos**: Solo tiene permisos de visualización sobre el estado de los DAGs de producción relevantes para sus informes, sin capacidad de ejecución o modificación.

Para configurar esto en Airflow, se usa la interfaz de administración o, programáticamente, se pueden usar las APIs de Airflow. Por ejemplo, para un DAG llamado `data_ingestion_prod`, se puede configurar que solo el rol "Operaciones de Datos" tenga permiso de `can_dag_run` sobre él.

**Aplicación de RBAC en sistemas de archivos distribuidos (como HDFS) para restringir lectura/escritura**:

En sistemas de almacenamiento distribuido como HDFS (Hadoop Distributed File System), la gestión de permisos es crucial para proteger los datos en reposo. El RBAC permite controlar quién puede leer, escribir o ejecutar archivos y directorios.

HDFS utiliza un modelo de permisos similar al de sistemas de archivos POSIX, que incluye permisos de propietario, grupo y otros. Sin embargo, para entornos de Big Data, se suelen usar **ACLs (Access Control Lists)** para una gestión más granular. Las ACLs permiten asignar permisos específicos a usuarios y grupos individuales más allá de los permisos básicos de propietario y grupo.

Supongamos un directorio en HDFS donde se almacenan datos de clientes sensibles: `/user/raw_data/customer_info`.

* Asignación de permisos básicos:

```bash
hdfs dfs -chown data_engineer:data_team /user/raw_data/customer_info
hdfs dfs -chmod 750 /user/raw_data/customer_info
```

Esto da al usuario `data_engineer` (propietario) permisos de lectura, escritura y ejecución (7), al grupo `data_team` permisos de lectura y ejecución (5), y a otros ningún permiso (0).

* Uso de ACLs para RBAC más granular:

Si queremos que un usuario específico, `audit_user`, tenga solo permisos de lectura en ese directorio, sin ser parte del grupo `data_team`, podemos usar ACLs:

```bash
hdfs dfs -setfacl -m user:audit_user:r-x /user/raw_data/customer_info
```

Esto permite al `audit_user` leer y listar el contenido del directorio, pero no modificarlo. Por otro lado, si un rol "Científico de Datos" solo necesita leer datos agregados en `/user/processed_data/agg_metrics`, se le otorgaría acceso de lectura solo a ese directorio.

##### Políticas de Acceso y Segregación de Entornos

La seguridad se robustece significativamente al **segregar los entornos** (desarrollo, pruebas, producción) y establecer **políticas de acceso claras y estrictas** para cada uno, controlando cómo los datos y los *pipelines* fluyen entre ellos. Esto previene que cambios accidentales o maliciosos en desarrollo afecten la producción.

**Airflow con DAGs de producción ejecutables solo por usuarios con rol "operaciones"**:

Esta política garantiza que el despliegue y la ejecución de *pipelines* críticos en producción estén altamente controlados, reduciendo el riesgo de interrupciones o errores.

En un entorno de CI/CD para Airflow, los DAGs de producción suelen ser desplegados automáticamente desde un repositorio de código fuente (como Git) a un ambiente de producción de Airflow. Sin embargo, la activación manual o la modificación de estos DAGs en producción debe ser un privilegio restringido. Esto se logra configurando Airflow con roles específicos que tienen permisos explícitos para interactuar con DAGs de producción.

Se define un rol `produccion_airflow_operator` que tiene permisos para `can_dag_run`, `can_dag_edit`, y `can_dag_pause_unpause` *solo* para DAGs que residen en el *folder* de producción de Airflow (por ejemplo, `/opt/airflow/dags/prod_dags`). Los ingenieros de datos solo tienen un rol `desarrollo_airflow_engineer` que les permite operar en `/opt/airflow/dags/dev_dags` y `/opt/airflow/dags/test_dags`.

Cuando se despliega un nuevo DAG a producción, solo los usuarios con el rol `produccion_airflow_operator` pueden activar sus *runs* iniciales o reiniciar *runs* fallidos, siguiendo un protocolo estricto de **cambios controlados**.

**Configuración de entornos de Spark aislados por cluster o namespace**:

El aislamiento de entornos en Spark previene la interferencia entre cargas de trabajo de desarrollo, pruebas y producción, y asegura que los recursos computacionales y los datos se utilicen de manera segura y eficiente.

En plataformas en la nube o en clústeres locales, es una buena práctica configurar clústeres Spark o *namespaces* separados para cada entorno. Esto significa que un trabajo de desarrollo no puede acceder accidentalmente a los datos de producción o consumir sus recursos computacionales. Los permisos de acceso a los datos subyacentes (en S3, ADLS Gen2, GCS, HDFS, Snowflake, etc.) también se configuran para cada entorno.

* **Databricks Workspaces**: Se crean *workspaces* separados para desarrollo, *staging* y producción. Cada *workspace* tiene sus propios clústeres Spark, cuadernos, tablas y configuraciones de seguridad. Los usuarios solo tienen acceso a su *workspace* asignado. Un ingeniero de datos que desarrolla en el *workspace* de "Desarrollo" no puede ver ni acceder a los datos o *jobs* en el *workspace* de "Producción".

* **AWS EMR**: Se lanzan clústeres EMR dedicados para cada entorno. Por ejemplo, un clúster EMR "dev-spark-cluster" y un clúster "prod-spark-cluster". Los roles de IAM de AWS se utilizan para controlar qué clústeres pueden lanzar los usuarios y qué buckets de S3 pueden leer o escribir cada clúster.

```yaml
# Política IAM para el rol de producción de EMR
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::prod-data-lake/*",
                "arn:aws:s3:::prod-spark-logs/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetDatabase",
                "glue:GetDatabases"
            ],
            "Resource": "*" # Limitar esto a bases de datos de producción si es posible
        }
    ]
}
```
Esta política permitiría a un clúster Spark de producción acceder solo a los *buckets* S3 designados para producción y al catálogo de Glue Data Catalog relevante para producción.


### 3.6.4 Prácticas de auditoría y trazabilidad de operaciones

Una arquitectura de datos robusta y segura no solo se enfoca en la protección perimetral o el control de acceso; también requiere **mecanismos sofisticados para rastrear qué sucedió, quién lo hizo, cuándo y cómo los datos se transformaron a lo largo de su ciclo de vida**. Estas prácticas son cruciales no solo para la **seguridad y la detección de incidentes**, sino también para el **cumplimiento normativo, la depuración de errores y la gobernanza de datos**.

##### Auditoría de ejecuciones y cambios

La auditoría de ejecuciones y cambios implica el registro sistemático y centralizado de todas las **actividades operacionales, modificaciones de configuración y accesos a datos dentro de la plataforma Big Data**. Este registro detallado es fundamental para la **identificación temprana de comportamientos anómalos, la investigación forense de posibles brechas de seguridad, y la demostración de cumplimiento frente a regulaciones como GDPR, HIPAA o SOX**.

**Uso de Airflow Logs centralizados con integración a herramientas como ELK o CloudWatch.**
    
Apache Airflow es una herramienta ampliamente utilizada para orquestar flujos de trabajo de datos. Cada ejecución de una tarea (DAG Run) genera logs detallados que contienen información sobre el inicio, finalización, errores, salidas de los scripts y recursos utilizados. Para una auditoría efectiva, estos logs deben ser **centralizados y persistidos fuera del entorno efímero de Airflow workers**. 

Herramientas como **Elastic Stack (ELK: Elasticsearch, Logstash, Kibana)** o **Amazon CloudWatch (en AWS)** permiten recolectar, indexar, buscar y visualizar estos logs de manera eficiente. Esto facilita la creación de **dashboards de monitoreo**, la **configuración de alertas** para fallos o accesos inusuales, y la **generación de reportes de auditoría**.
    
Imagina que tienes un DAG de Airflow (`data_ingestion_dag`) que ingesta datos sensibles. Si un usuario no autorizado intenta modificar este DAG o si una ejecución falla repetidamente, necesitas ser notificado y tener un registro de estos eventos.
    
* **Configuración en Airflow**: Asegúrate de que Airflow esté configurado para enviar sus logs a un sistema de log centralizado. Para AWS, esto podría ser S3 y CloudWatch:
    
```python
# airflow.cfg o variables de entorno para Airflow
# [logging]
# remote_logging = True
# remote_base_log_folder = s3://your-airflow-logs-bucket/
# remote_log_conn_id = aws_default_conn
# S3_REMOTE_HANDLES = s3
```

Luego, puedes configurar CloudWatch Logs para ingerir logs desde S3 o directamente desde tus instancias EC2 donde corre Airflow.
        
* **Consulta en Kibana (ejemplo para ELK)**: Una vez que los logs estén en Elasticsearch, puedes buscar eventos específicos, por ejemplo, todas las ejecuciones fallidas de un DAG en particular o intentos de acceso a la interfaz de Airflow por usuarios específicos.
    
```
# Ejemplo de consulta en Kibana
"dag_id": "data_ingestion_dag" AND "status": "failed" AND "@timestamp": [now-24h TO now]
```
        
* **Alerta en CloudWatch (ejemplo para AWS)**: Puedes crear una métrica de log en CloudWatch que cuente los errores de Airflow y configure una alarma para notificarte.
    
```json
{
  "logGroupNames": ["/aws/containerinsights/your-cluster-name/application"],
  "metricFilterPatterns": [
    "{ $.logStreamName = \"/ecs/airflow-worker*\" && $.message like \"ERROR\" }"
  ],
  "metricTransformations": [
    {
      "metricName": "AirflowErrors",
      "metricNamespace": "AirflowCustomMetrics",
      "metricValue": "1"
    }
  ]
}
```
        
Esta configuración te permitiría ver un aumento en los errores de Airflow y actuar proactivamente.
    
**Activación de audit logs en Snowflake para rastrear consultas, cambios de roles o ingestas.**
    
Snowflake, como un Data Warehouse en la nube, ofrece capacidades de auditoría robustas a través de sus **vistas de Account Usage** y la **función `QUERY_HISTORY`**. Estas herramientas permiten a los administradores **rastrear cada consulta ejecutada, los usuarios que las realizaron, el tiempo de ejecución, la cantidad de datos procesados, los roles utilizados**, y también **cambios en la configuración de seguridad (como la creación o modificación de usuarios y roles) o actividades de ingesta de datos (uso de COPY INTO)**. La auditoría en Snowflake es fundamental para **garantizar la seguridad de los datos, identificar patrones de uso inusuales, optimizar el rendimiento y cumplir con las políticas de gobernanza de datos**.
    
Supongamos que necesitas saber quién accedió a una tabla crítica de clientes en los últimos 7 días o si alguien intentó modificar los permisos de un usuario específico.
    
* **Rastreo de consultas a una tabla específica**:
        
```sql
SELECT
    query_id,
    query_text,
    user_name,
    role_name,
    start_time,
    end_time,
    error_message
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    query_text ILIKE '%SELECT%FROM%customer_data%' -- Busca consultas que accedan a 'customer_data'
    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY
    start_time DESC;
```
        
* **Auditoría de cambios de rol o usuario**:
        
Snowflake registra eventos de seguridad en la vista `LOGIN_HISTORY` y `USERS` para logins fallidos. Para cambios en roles y permisos, aunque no hay una vista `ROLE_HISTORY` directa, estos eventos se registran en `QUERY_HISTORY` si se ejecutan comandos DDL.
    
```sql
SELECT
    query_id,
    query_text,
    user_name,
    start_time
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    query_type IN ('CREATE_ROLE', 'ALTER_ROLE', 'GRANT_ROLE', 'REVOKE_ROLE', 'CREATE_USER', 'ALTER_USER', 'DROP_USER')
    AND start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
ORDER BY
    start_time DESC;
```
        
* **Monitoreo de ingestas de datos (COPY INTO)**:
    
```sql
SELECT
    query_id,
    query_text,
    user_name,
    start_time,
    rows_inserted,
    files_scanned
FROM
    SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE
    table_name = 'YOUR_TARGET_TABLE' -- Reemplaza con tu tabla
    AND start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY
    start_time DESC;
```
        
Estas consultas permiten a los equipos de seguridad y operaciones mantener un registro inmutable de las actividades cruciales en Snowflake.

##### Lineage y trazabilidad de datos

El **linaje de datos (data lineage)** es la capacidad de **mapear y comprender el recorrido de los datos desde su origen inicial hasta su destino final**, incluyendo todas las transformaciones y movimientos intermedios. Es fundamental para la **gobernanza de datos, la calidad de los datos, la depuración, el análisis de impacto de cambios y el cumplimiento normativo**. Saber de dónde provienen los datos y cómo se transformaron es clave para **confiar en los resultados analíticos y asegurar la integridad de la información**.

**Integración de Apache Atlas con Spark para visualizar el lineage completo de columnas.**
    
Apache Atlas es una plataforma de gobernanza de datos y metadatos que proporciona un **catálogo de datos centralizado y capacidades de linaje**. Cuando se integra con motores de procesamiento de datos como Apache Spark, Atlas puede **interceptar y registrar las transformaciones de datos a nivel de columna**. Esto significa que puedes visualizar no solo cómo un conjunto de datos se deriva de otro, sino también cómo cada columna individual en un conjunto de datos de salida se construye a partir de columnas específicas en los conjuntos de datos de entrada. Esta granularidad es invaluable para rastrear el origen de un valor, entender el impacto de un cambio en una columna fuente, o identificar la causa raíz de un problema de calidad de datos.
    
Imagina un proceso Spark que lee datos de ventas de una tabla, los limpia, agrega información de clientes de otra tabla y luego los escribe en una tabla de reportes.
    
* **Proceso Spark (pseudocódigo)**:
    
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
    
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .enableHiveSupport() \
    .getOrCreate()
    
# Lectura de datos de ventas (origen 1)
sales_df = spark.read.table("raw_data.sales")
    
# Lectura de datos de clientes (origen 2)
customers_df = spark.read.table("raw_data.customers")
    
# Limpieza y transformación de datos de ventas
cleaned_sales_df = sales_df.filter(col("quantity") > 0) \
                           .withColumn("total_price", col("quantity") * col("unit_price"))
    
# Unir con datos de clientes
enriched_sales_df = cleaned_sales_df.join(customers_df, "customer_id", "inner") \
                                    .select(
                                        col("sale_id"),
                                        col("product_id"),
                                        col("total_price"),
                                        concat_ws(" ", col("first_name"), col("last_name")).alias("customer_full_name"),
                                        col("region")
                                    )
    
# Escribir el resultado en una tabla de reportes
enriched_sales_df.write.mode("overwrite").saveAsTable("reporting.daily_sales_summary")
    
spark.stop()
```
    
* **Visualización en Apache Atlas**: Una vez que este trabajo Spark se ejecuta y Apache Atlas está configurado para escuchar eventos de Spark (a través de hooks de Spark o un plugin), Atlas registrará automáticamente el linaje. En la interfaz de usuario de Atlas, podrías ver un gráfico de linaje como este:
        
`raw_data.sales (tabla)` --> `cleaned_sales_df (dataframe Spark)` --(Transformación: `total_price` de `quantity` y `unit_price`)--> `enriched_sales_df (dataframe Spark)` --(Unión con `raw_data.customers`)--> `reporting.daily_sales_summary (tabla)`
        
Y al profundizar, podrías ver el linaje a nivel de columna, por ejemplo:
        
* `reporting.daily_sales_summary.total_price` se deriva de `raw_data.sales.quantity` y `raw_data.sales.unit_price`.
* `reporting.daily_sales_summary.customer_full_name` se deriva de `raw_data.customers.first_name` y `raw_data.customers.last_name`.
        
**Aplicación de etiquetas (tags) a datasets sensibles en Data Catalogs para trazabilidad y control.**
    
Un **Catálogo de Datos (Data Catalog)** es una herramienta esencial para la gobernanza de datos que actúa como un **inventario centralizado de todos los activos de datos de una organización**. Permite descubrir, comprender y gestionar los datos. Una funcionalidad clave es la capacidad de aplicar etiquetas (tags) o clasificaciones a los datasets, tablas, o incluso columnas, especialmente aquellos que contienen información sensible (PII, datos financieros, etc.) o que son críticos para el negocio. Estas etiquetas no solo mejoran la **trazabilidad al indicar la naturaleza y sensibilidad de los datos**, sino que también pueden **automatizar la aplicación de políticas de seguridad y cumplimiento**. Por ejemplo, una etiqueta "PII" podría desencadenar automáticamente reglas de enmascaramiento o acceso restringido.
    
Supongamos que tienes un conjunto de datos en Google BigQuery que contiene información personal identificable de clientes.
    
* **Identificación y Etiquetado**: Usarías tu Data Catalog (por ejemplo, Google Data Catalog, Collibra, Amundsen, etc.) para buscar la tabla `project.dataset.customers`. Una vez localizada, podrías aplicar etiquetas (tags) a la tabla completa o a columnas específicas.
        
    **Ejemplo de tags a nivel de tabla**:
        
    * `Sensibilidad: Alto`
    * `Clasificación: PII`
    * `Regulación: GDPR`
    * `Propietario: Equipo de Datos de Clientes`
        
    **Ejemplo de tags a nivel de columna**:
        
    * Columna `email`: `Tipo: Contacto`, `Sensibilidad: PII-Directo`
    * Columna `credit_card_number`: `Tipo: Financiero`, `Sensibilidad: Muy Alto`, `Regulación: PCI-DSS`
        
* **Beneficios de la Trazabilidad con Tags**:
        
    * **Descubrimiento**: Cualquier analista que busque datos de clientes verá inmediatamente las etiquetas de sensibilidad, lo que le ayudará a decidir si puede usar esos datos y bajo qué condiciones.
    * **Cumplimiento**: Las herramientas de gobernanza pueden usar estas etiquetas para generar reportes sobre la ubicación de datos PII y demostrar el cumplimiento de GDPR.
    * **Automatización de Políticas**: En algunos catálogos, estas etiquetas pueden integrarse con sistemas de seguridad para que, por ejemplo, el acceso a datos etiquetados como "PII" requiera una aprobación adicional o se apliquen automáticamente funciones de enmascaramiento para usuarios no autorizados.
    * **Análisis de Impacto**: Si una columna etiquetada como "PII" se modifica o se elimina, el catálogo puede mostrar qué otros conjuntos de datos o reportes dependen de ella, ayudando a predecir el impacto antes de realizar el cambio.
        
Estas prácticas de auditoría y trazabilidad son los pilares para construir una arquitectura de datos no solo eficiente y escalable, sino también **segura, transparente y conforme a las regulaciones**.

### 3.6.5 Requerimientos normativos comunes: GDPR, HIPAA, etc.

Los **pipelines ETL (Extract, Transform, Load)** no son solo herramientas técnicas; también deben diseñarse y operarse en estricto cumplimiento con una serie de **regulaciones que protegen la privacidad y los derechos de los usuarios sobre sus datos personales**. 

La omisión o el manejo inadecuado de estas normativas puede acarrear multas significativas, daño reputacional y la pérdida de confianza de los clientes. Es crucial que los arquitectos de datos y desarrolladores de pipelines entiendan la importancia de la privacidad desde el diseño (`Privacy by Design`) y la seguridad desde el diseño (`Security by Design`).

##### Regulaciones aplicables al tratamiento de datos

La naturaleza global del Big Data significa que las organizaciones a menudo deben cumplir con un mosaico de regulaciones dependiendo de la ubicación de los datos, la residencia de los usuarios y el sector industrial. Algunas de las normativas más prominentes incluyen:

**Aplicación de anonimización en pipelines que tratan datos personales de ciudadanos europeos (GDPR).**

El **Reglamento General de Protección de Datos (GDPR)** de la Unión Europea es una de las normativas de privacidad de datos más estrictas a nivel mundial. Exige que los datos personales de los ciudadanos de la UE sean procesados de forma lícita, leal y transparente. Esto a menudo implica la **anonimización o seudonimización** de los datos para reducir el riesgo de identificación directa de los individuos.

* Un pipeline que procesa datos de compras en línea de clientes europeos debe anonimizar campos como `nombre`, `dirección` y `email` antes de almacenarlos en un data lake analítico. Esto podría implicar el uso de técnicas como el **hashing irreversible** para `email` o la **generalización** para `código_postal`.

```python
# Ejemplo de seudonimización con hashing (Python + PySpark)
from pyspark.sql.functions import sha2, concat_ws, lit

# Suponiendo un DataFrame 'df_clientes_europeos'
# con columnas 'nombre', 'apellido', 'email', 'direccion'

df_anonimizado = df_clientes_europeos.withColumn(
    "email_hash", sha2(df_clientes_europeos["email"], 256) # Hashing SHA-256 del email
).withColumn(
    "nombre_seudonimo", concat_ws("_", lit("cliente"), sha2(df_clientes_europeos["nombre"], 256))
).drop("email", "nombre", "apellido", "direccion") # Eliminar columnas de identificación directa

df_anonimizado.show()
```

**Encriptación obligatoria de historiales clínicos en pipelines hospitalarios (HIPAA).**

La **Ley de Portabilidad y Responsabilidad del Seguro Médico (HIPAA)** en Estados Unidos establece estándares nacionales para proteger la información de salud protegida (`PHI`). Cualquier pipeline que maneje datos de salud, como historiales médicos, resultados de laboratorio o información de seguros, debe asegurar que esta información esté **encriptada tanto en tránsito como en reposo**.

Un pipeline ETL que ingiere datos de sistemas EHR (Electronic Health Records) a un almacén de datos para análisis de investigación debe garantizar que los archivos de datos se encripten antes de ser subidos a un bucket de S3 o a Azure Blob Storage, y que la comunicación con la base de datos de origen utilice SSL/TLS.

```bash
# Ejemplo de encriptación de archivos en reposo (AWS S3)
# Al subir un archivo a S3, especificar encriptación del lado del servidor
aws s3 cp /ruta/a/historial_clinico.csv s3://mi-bucket-salud/data/ --sse AES256

# O si se usa un cliente de Python (boto3)
import boto3
s3 = boto3.client('s3')
bucket_name = 'mi-bucket-salud'
file_path = '/ruta/a/historial_clinico.csv'
object_name = 'data/historial_clinico.csv'

s3.upload_file(
    file_path,
    bucket_name,
    object_name,
    ExtraArgs={
        'ServerSideEncryption': 'AES256' # Encriptación del lado del servidor con AES-256
    }
)
```

**Inclusión de consentimiento informado como metadato procesado en flujos de datos personales.**

Muchas regulaciones, como GDPR y LGPD (Brasil), requieren que el **consentimiento del usuario** para el procesamiento de sus datos personales sea explícito, informado y revocable. Esto significa que los pipelines no solo deben procesar los datos, sino también la **información sobre el consentimiento** asociada a esos datos, lo que a menudo implica almacenar metadatos relacionados con la fecha de consentimiento, el tipo de consentimiento y la versión de la política de privacidad aceptada.

Un pipeline que recopila datos de comportamiento de usuario de una aplicación móvil debe adjuntar un `ID_consentimiento` y `fecha_consentimiento` a cada evento de datos, permitiendo así filtrar y borrar datos en caso de que un usuario revoque su consentimiento.

```json
# Ejemplo de un registro de evento de datos con metadatos de consentimiento
{
  "user_id": "usuario_abc123",
  "event_type": "pagina_vista",
  "page_url": "/productos/item123",
  "timestamp": "2025-06-10T10:30:00Z",
  "consent_metadata": {
    "consent_id": "cons_xyz789",
    "consent_date": "2025-01-15T09:00:00Z",
    "policy_version": "2.1",
    "data_processing_purposes": ["analitica", "marketing_personalizado"]
  }
}
```

##### Diseño de pipelines "compliance-ready"

Para cumplir con las normativas de forma proactiva y evitar problemas, es fundamental adoptar un enfoque de **diseño de pipelines "compliance-ready"** o "privacy-by-design". Esto implica integrar consideraciones de cumplimiento normativo en cada etapa del ciclo de vida del desarrollo del pipeline, minimizando así los riesgos legales y operativos.

**Creación de módulos reutilizables para anonimizar, enmascarar o eliminar datos bajo demanda.**

La estandarización es clave para la consistencia y la eficiencia en el cumplimiento. Desarrollar un **conjunto de funciones o microservicios reusables** para las operaciones comunes de protección de datos (anonimización, seudonimización, enmascaramiento, tokenización y borrado seguro) permite aplicarlas de manera uniforme en diferentes pipelines y fuentes de datos.

Un equipo de datos podría construir una librería interna en Spark para ofuscar PII (Información de Identificación Personal).

```python
# spark_data_masking_lib.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib

@udf(returnType=StringType())
def hash_email(email):
    if email:
        return hashlib.sha256(email.lower().encode('utf-8')).hexdigest()
    return None

@udf(returnType=StringType())
def mask_phone_number(phone):
    if phone and len(phone) > 4:
        return '*' * (len(phone) - 4) + phone[-4:]
    return phone

# En un pipeline de Spark:
# from spark_data_masking_lib import hash_email, mask_phone_number
# df_raw.withColumn("hashed_email", hash_email("email")) \
#       .withColumn("masked_phone", mask_phone_number("telefono"))
```

**Implementación de políticas de retención y borrado automatizado de datos personales.**

Las regulaciones suelen establecer límites sobre cuánto tiempo se pueden retener los datos personales. Los pipelines deben ser capaces de aplicar **políticas de retención de datos** que automaticen el borrado o la anonimización de datos que ya no son necesarios o para los que el consentimiento ha sido revocado. Esto podría implicar el uso de funcionalidades de **gestión del ciclo de vida (lifecycle management)** en sistemas de almacenamiento en la nube o tareas programadas.

Configurar reglas de ciclo de vida en un bucket de AWS S3 para que los objetos de más de 7 años sean eliminados automáticamente, o un trabajo de Airflow que ejecute un script de borrado lógico en una tabla de Snowflake para registros con antigüedad superior a la política de retención.

```python
# Ejemplo de tarea de Airflow para borrado de datos antiguos (pseudocódigo)
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='data_retention_policy_cleaner',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=7), # Ejecutar semanalmente
    catchup=False
) as dag:
    clean_old_customer_data = BashOperator(
        task_id='clean_customer_data_in_snowflake',
        bash_command="""
            snowflake_conn_string="user={{ var.value.snowflake_user }} password={{ var.value.snowflake_password }} account={{ var.value.snowflake_account }}"
            SNOWFLAKE_QUERY="DELETE FROM analytics_db.public.customer_data WHERE created_at < DATEADD(year, -7, CURRENT_DATE());"
            snowsql -c "$snowflake_conn_string" -q "$SNOWFLAKE_QUERY"
        """
    )
```

**Inclusión de revisiones legales en el ciclo de vida del pipeline (Data Governance Boards).**

El cumplimiento normativo no es solo una preocupación técnica, sino también legal y de gobernanza. Es fundamental establecer un **marco de gobernanza de datos** que incluya **revisiones legales y de cumplimiento** en las etapas clave del diseño, desarrollo y despliegue de los pipelines. Un `Data Governance Board` (Junta de Gobernanza de Datos) o un `Privacy Officer` debe aprobar cómo se procesan los datos personales, asegurando que las decisiones técnicas estén alineadas con las políticas corporativas y los requisitos legales.

Antes de lanzar un nuevo pipeline de ingesta de datos de clientes para un nuevo servicio, el diseño arquitectónico y el plan de procesamiento de datos son revisados por el equipo legal y el oficial de privacidad de datos para asegurar el cumplimiento con GDPR, CCPA y cualquier otra regulación relevante. Esto podría implicar una reunión formal donde los ingenieros de datos presenten el flujo de datos, las medidas de seguridad y anonimización implementadas, y las políticas de retención, recibiendo la aprobación formal del comité.


## Tarea

Desarrolla los siguientes ejercicios prácticos en tu entorno de laboratorio:

1. Configura una conexión segura TLS entre Apache Spark y una base de datos PostgreSQL y documenta los pasos seguidos.
2. Implementa el acceso a secretos mediante AWS Secrets Manager desde un DAG de Apache Airflow que conecta con un bucket de S3.
3. Diseña una política de control de acceso por roles (RBAC) para un pipeline ETL en Databricks, diferenciando roles de analista, ingeniero y auditor.
4. Audita un pipeline ejecutado en Airflow e identifica los registros clave de su trazabilidad y posibles fallas.
5. Rediseña un pipeline para que cumpla con GDPR incorporando enmascaramiento de datos personales y lógica de eliminación bajo solicitud del usuario.

