FROM apache/airflow:2.8.1-python3.8

USER root

# Cài Java 17
RUN apt-get update && \
    apt-get install -y default-jdk curl gnupg2 apt-transport-https && \
    apt-get clean

# Cài ODBC Driver 18
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc -o /tmp/microsoft.asc && \
    gpg --batch --yes --dearmor -o /usr/share/keyrings/microsoft-prod.gpg /tmp/microsoft.asc && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > \
    /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install pyspark pyodbc