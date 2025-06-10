[← Volver al Inicio](index.md)

# Spark en modo `local`

El modo **local** es la configuración más sencilla y se ejecuta en una sola máquina sin distribución de trabajo. Es ideal para desarrollo, pruebas y pequeños conjuntos de datos, ya que no requiere un entorno distribuido. 

### Características

- No depende de un **gestor de clúster** como [YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) o [Kubernetes](https://kubernetes.io/).
- Puede usar uno o múltiples núcleos en la máquina local (`local[1]` para un núcleo, `local[*]` para todos los núcleos disponibles).
- Se ejecuta sin necesidad de configurar un nodo maestro (*driver*), ni nodos trabajadores (*worker*).
- Es útil para depuración y desarrollo rápido.

### Ejecución

1. Descarga la imagen de Docker:

```bash
docker pull quay.io/jupyter/pyspark-notebook
```

2. Crea un volumen con el nombre `work`, para preservar el trabajo realizado con Jupyter:

```bash
docker volume create work
```

3. Ejecuta el contenedor en modo interactivo, iniciará Jupyter Notebook y lo expondrá en el puerto 8888:

```bash
docker run -it --rm -p 8888:8888 -v work:/home/jovyan/ quay.io/jupyter/pyspark-notebook
```

4. Accede a Jupyter Notebook:

- Abre el link que muestra el LOG de docker al iniciar: `http://localhost:8888/...`

- Crea un nuevo notebook Python, e inicia una sesión en modo `local` con todos los núcleos disponibles (`local[*]`):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
print(spark)
```

---

# Spark en modo `cluster`

El modo **clúster** permite distribuir el procesamiento de datos entre múltiples máquinas. Es esencial cuando se trabaja con grandes volúmenes de datos que no pueden manejarse eficientemente en una sola máquina.

### Características

- Requiere un **gestor de clúster** como [Spark Standalone](https://spark.apache.org/docs/3.5.2/spark-standalone.html), [YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), [Mesos](https://mesos.apache.org/) o [Kubernetes](https://kubernetes.io/).
- Puede escalar horizontalmente agregando más nodos al clúster.
- Requiere un nodo maestro (*driver*) que coordina el trabajo, y nodos trabajadores (*worker*) que ejecutan las tareas.
- Ideal para **procesamiento distribuido** de Big Data.

### Ejecución

##### Opción 1: usando [Docker Compose](https://docs.docker.com/compose/) (recomendada)

1. Crea el archivo `docker-compose.yml`:

```yaml
version: '3'

services:
  spark-master:
    image: quay.io/jupyter/pyspark-notebook
    container_name: spark-master
    ports:
      - "7077:7077"  # puerto del nodo maestro
      - "8080:8080"  # interfaz web del nodo maestro
      - "8888:8888"  # interfaz web de Jupyter
    environment:
      - SPARK_MODE=master
    volumes:
      - ./data:/home/jovyan/data

  spark-worker-1:
    image: quay.io/jupyter/pyspark-notebook
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_MODE=worker
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1G

  spark-worker-2:
    image: quay.io/jupyter/pyspark-notebook
    container_name: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_MODE=worker
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1G
```

2. Ejecutar:

```python
docker-compose up -d
```

3. Acceder a la interfaz Web del nodo maestro: `http://localhost:8080`

4. Acceder a Jupyter Notebook: `http://localhost:8888`
   si solicita una contraseña, `jupyter`

5. Utilizando PySpark en el cuaderno:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("test-cluster") \
    .getOrCreate()
```

