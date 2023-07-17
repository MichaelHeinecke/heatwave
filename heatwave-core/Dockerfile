# Adapted from https://levelup.gitconnected.com/how-to-run-spark-with-docker-c6287a11a437
FROM eclipse-temurin:17.0.7_7-jre

# VERSIONS
ENV SPARK_VERSION=3.4.1 \
HADOOP_VERSION=3 \
JAVA_VERSION=11

# DOWNLOAD SPARK AND INSTALL
RUN DOWNLOAD_URL_SPARK="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && wget --no-verbose -O apache-spark.tgz  "${DOWNLOAD_URL_SPARK}"\
    && mkdir -p /home/spark \
    && tar -xf apache-spark.tgz -C /home/spark --strip-components=1 \
    && rm apache-spark.tgz

# SET SPARK ENV VARIABLES
ENV SPARK_HOME="/home/spark"
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

# Change to "$NB_UID" so the image runs as a non root user by default
USER $NB_UID

RUN mkdir -p app/data
COPY target/heatwave-core-1.0-jar-with-dependencies.jar app/heatwave.jar

ENTRYPOINT ["java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "app/heatwave.jar"]
CMD ["path/to/mount/input/data"]
