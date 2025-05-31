[← Volver al Inicio](index.md)

# Instalación y Configuración de Docker y WSL2

Para trabajar cómodamente con [Spark](https://spark.apache.org/) y [Jupyter](https://jupyter.org/), es recomendable usar [Docker](https://www.docker.com/) en combinación con [WSL2](https://learn.microsoft.com/es-es/windows/wsl/install) (Windows Subsystem for Linux). Esto garantizará un entorno flexible y eficiente para el desarrollo.

**Docker** en Windows requiere virtualización por hardware habilitada en el BIOS, una versión superior a **Windows 10** `1607`+ o soporte para Hyper-V,

La forma más fácil para correr **Docker** en Windows, es configurarlo para utilizar **WSL2** (*Windows Subsystem para Linux*).

## 1. Instalación de WSL2

- Para Habilitar WSL2, abre una ventana de **PowerShell** como administrador y ejecuta:

```powershell
wsl --install
```

- Asegurate que WSL2 está ejecutando la última versión con el comando:

```powershell
wsl --update
```

- Abre una ventana de **Ubuntu** desde el menú de inicio, y configura usuario y contraseña.  

## 2. Instalación de Docker

**Docker** permitirá ejecutar **Spark** y **Jupyter** en contenedores sin complicaciones.

- Descarga **Docker Desktop** para Windows desde el [sitio web oficial](https://docs.docker.com/desktop/install/windows-install/) y sigue las instrucciones.

- Activa el uso de WSL2, abriendo el programa **Docker Desktop**, ir al menú **Settings**, pestaña **General** y activar **Use the WSL 2 based engine**, luego haz click en **Apply & Restart** y listo.

- Verifica la instalación con este comando:

```bash
docker --version
```

## 3. Ejecutando un contenedor de ejemplo

Ya tienes instalado **Docker**, es el momento de ejecutar tu primer contenedor, usando el siguiente comando: 

```bash
docker run hello-world
```

Ese contenedor solo sirve para probar que Docker funciona, hay más imágenes preconstruidas en [Docker Hub](https://hub.docker.com/) que puedes utilizar para ejecutar otros servicios.

[← Volver al Inicio](index.md)

