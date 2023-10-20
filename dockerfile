
# Utiliza una imagen base de Python
FROM python:3.8

# Copia el script Python a la carpeta /app/ dentro del contenedor
COPY api_clima.py /app/

# Instala las dependencias necesarias utilizando un archivo de requisitos (si lo tienes)
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt


RUN pip install apache-airflow

# Establece el directorio de trabajo
WORKDIR /app/
