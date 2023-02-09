#!/bin/bash

set -ex

echo "Downloading Spark 2 and 3"
if [ ! -f spark-2.4.8-bin-hadoop2.7.tgz ]; then
  wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz &
fi
if [ ! -f spark-3.3.1-bin-hadoop2.tgz ]; then
  wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop2.tgz &
fi
wait
if [ ! -d spark-3.3.1-bin-hadoop2 ]; then
  tar -xvf spark-3.3.1-bin-hadoop2.tgz
fi
if [ ! -d spark-2.4.8-bin-hadoop2.7 ]; then
  tar -xvf spark-2.4.8-bin-hadoop2.7.tgz
fi
if [ ! -f iceberg-spark-runtime-3.3_2.12-1.1.0.jar ]; then
  wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar -O iceberg-spark-runtime-3.3_2.12-1.1.0.jar &
  wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-2.4/1.1.0/iceberg-spark-runtime-2.4-1.1.0.jar -O iceberg-spark-runtime-2.4-1.1.0.jar &
  wait
  cp iceberg-spark-runtime-3.3_2.12-1.1.0.jar spark-3.3.1-bin-hadoop2/jars/
  cp iceberg-spark-runtime-2.4-1.1.0.jar spark-2.4.8-bin-hadoop2.7/jars/
fi


echo "Making a copy of the demo project so we can have side-by-side migrated / non-migrated."
rm -rf sparkdemoproject-3
cp -af sparkdemoproject sparkdemoproject-3
echo "Build the current demo project"
cd sparkdemoproject
sbt clean compile test package
cd ..
cd sparkdemoproject-3
echo "Now we run the migration setup."
cat ../../../docs/scala/sbt.md
# Sketchy auto rewrite build.sbt
cp -af build.sbt build.sbt.bak
cat build.sbt.bak | \
  python -c 'import re,sys;print(re.sub(r"name :=\s*\"(.*?)\"", "name :=\"\1-3\"", sys.stdin.read()))' > build.sbt
cat >> build.sbt <<- EOM
scalafixDependencies in ThisBuild +=
  "com.holdenkarau" %% "spark-scalafix-rules" % "0.1.1-2.4.8"
EOM
mkdir -p project
cat >> project/plugins.sbt <<- EOM
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
EOM
read -p "Press enter to continue:" hifriends
echo "Great! Now we'll try and run the scala fix rules in your project! Yay!. This might fail if you have interesting build targets."
sbt scalafix
echo "Hells yes! Ok now take a close look at the output from above. THere may be linter rules for items we failed to migrate."
echo "You will also need to update dependency versions now."
echo "Please address those and then press enter."
read -p "Press enter to continue:" hifriends
sbt clean compile test package
echo "Lovely! Now we \"simulate\" publishing these jars to an S3 bucket (using local fs)"
cd ..
mkdir -p /tmp/spark-migration-jars
cp -af sparkdemoproject-*/target/scala-*/*.jar /tmp/spark-migration-jars
echo "Excellent news! All done. Now we just need to make sure we have the same pipeline. Let's magic it!"
cd ../../
cd pipelinecompare
echo "There is some trickery in our spark-submit2 v.s. spark-submit3 including the right iceberg version"
echo "Provided you have iceberg in your environment pre-insalled this should be equivelent to prod but... yeah."
python domagic.py --iceberg --spark-control-command spark-submit2 --spark-new-command spark-submit3 \
       --combined-pipeline "     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
    --conf spark.sql.catalog.spark_catalog.type=hive
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.local.type=hadoop
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse
    /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar"
