FROM maven:3.9-eclipse-temurin-11 AS builder

COPY pom.xml /build/pom.xml
COPY src /build/src
COPY dev /build/dev
COPY README.md /build/README.md
WORKDIR /build
RUN mvn package -DskipTests -q

FROM eclipse-temurin:11-jdk-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Install Spark 3.5.5
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz && \
    tar xzf spark-3.5.5-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.5-bin-hadoop3 /opt/spark && \
    rm spark-3.5.5-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# Install Delta Lake JARs
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
      -O $SPARK_HOME/jars/delta-spark_2.12-3.2.1.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
      -O $SPARK_HOME/jars/delta-storage-3.2.1.jar

# Install Jupyter + Scala kernel
RUN pip3 install --no-cache-dir jupyter spylon-kernel && \
    python3 -m spylon_kernel install

# Copy Ariadne JAR (shaded by maven-shade-plugin)
# Use a two-step approach: copy all matching JARs, then remove javadoc/sources
COPY --from=builder /build/target/ariadne-spark35_2.12-*.jar /tmp/ariadne-jars/
RUN cp /tmp/ariadne-jars/ariadne-spark35_2.12-*.jar $SPARK_HOME/jars/ && \
    rm -f $SPARK_HOME/jars/*-javadoc.jar $SPARK_HOME/jars/*-sources.jar && \
    rm -rf /tmp/ariadne-jars

WORKDIR /home
EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--NotebookApp.token=''", "--notebook-dir=/home/notebooks"]
