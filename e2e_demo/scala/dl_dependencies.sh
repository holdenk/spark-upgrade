# We DL Spark2 but also slipstreamed spark
SPARK2_DETAILS="spark-2.4.8-bin-without-hadoop-scala-2.12"
CORE_SPARK2="spark-2.4.8-bin-hadoop2.7"
SPARK3_DETAILS="spark-3.3.1-bin-hadoop2"

echo "Downloading Spark 2 and 3"
if [ ! -f ${CORE_SPARK2}.tgz ]; then
  wget  https://archive.apache.org/dist/spark/spark-2.4.8/${CORE_SPARK2}.tgz &
fi
if [ ! -f hadoop-2.7.0.tar.gz ]; then
  wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz &
fi
if [ ! -f ${SPARK2_DETAILS}.tgz ]; then
  wget  https://archive.apache.org/dist/spark/spark-2.4.8/${SPARK2_DETAILS}.tgz &
fi
if [ ! -f ${SPARK3_DETAILS}.tgz ]; then
  wget https://archive.apache.org/dist/spark/spark-3.3.1/${SPARK3_DETAILS}.tgz &
fi
wait
echo "Unzipping downloaded files"
if [ ! -d ${SPARK3_DETAILS} ]; then
  tar -xf ${SPARK3_DETAILS}.tgz
fi
if [ ! -d ${SPARK2_DETAILS} ]; then
  tar -xf ${SPARK2_DETAILS}.tgz
fi
if [ ! -d ${CORE_SPARK2} ]; then
  tar -xf ${CORE_SPARK2}.tgz
fi
if [ ! -d hadoop-2.7.0 ]; then
  tar -xf hadoop-2.7.0.tar.gz
fi
echo "Fetching iceberg dependencies"
if [ ! -f iceberg-spark-runtime-3.3_2.12-1.1.0.jar ]; then
  wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar -O iceberg-spark-runtime-3.3_2.12-1.1.0.jar &
fi
if [ ! -f iceberg-spark-runtime-2.4-1.1.0.jar ]; then
  wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-2.4/1.1.0/iceberg-spark-runtime-2.4-1.1.0.jar -O iceberg-spark-runtime-2.4-1.1.0.jar &
fi
wait
cp iceberg-spark-runtime-3.3_2.12-1.1.0.jar ${SPARK3_DETAILS}/jars/
cp iceberg-spark-runtime-2.4-1.1.0.jar ${SPARK2_DETAILS}/jars/

# Bring over the hadoop parts we need, this is a bit of a hack but using hadoop-2.7.0 contents
# does not work well either.
cp -f ${CORE_SPARK2}/jars/apache*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/guice*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/http*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/proto*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/parquet-hadoop*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/snappy*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/hadoop*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/guava*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/commons*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/libthrift*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/slf4j*.jar ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/log4j* ${SPARK2_DETAILS}/jars/
cp -f ${CORE_SPARK2}/jars/hive-*.jar ${SPARK2_DETAILS}/jars/
# Bring over non-scala 2.11 jackson jars.
cp -f ${CORE_SPARK2}/jars/*jackson*.jar ${SPARK2_DETAILS}/jars/
rm ${SPARK2_DETAILS}/jars/*jackson*_2.11*.jar

spark_submit2="$(pwd)/${SPARK2_DETAILS}/bin/spark-submit"
spark_submit3="$(pwd)/${SPARK3_DETAILS}/bin/spark-submit"
spark_sql3="$(pwd)/${SPARK3_DETAILS}/bin/spark-sql"
