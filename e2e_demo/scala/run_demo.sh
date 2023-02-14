#!/bin/bash

echo "Hi Friend! If you have questions running this script please reach out on Slack :D"

set -ex

INITIAL_VERSION=${INITIAL_VERSION:-2.4.8}
TARGET_VERSION=${TARGET_VERSION:-3.3.1}

prompt () {
  if [ -z "$NO_PROMPT" ]; then
    read -p "Press enter to continue:" hifriends
  fi
}

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
fi
if [ ! -f iceberg-spark-runtime-2.4-1.1.0.jar ]; then
  wget https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-2.4/1.1.0/iceberg-spark-runtime-2.4-1.1.0.jar -O iceberg-spark-runtime-2.4-1.1.0.jar &
fi
wait
cp iceberg-spark-runtime-3.3_2.12-1.1.0.jar spark-3.3.1-bin-hadoop2/jars/
cp iceberg-spark-runtime-2.4-1.1.0.jar spark-2.4.8-bin-hadoop2.7/jars/

spark_submit2="$(pwd)/spark-2.4.8-bin-hadoop2.7/bin/spark-submit"
spark_submit3="$(pwd)/spark-3.3.1-bin-hadoop2/bin/spark-submit"

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
  python -c 'import re,sys;print(re.sub(r"name :=\s*\"(.*?)\"", "name :=\"\\1-3\"", sys.stdin.read()))' > build.sbt
cat >> build.sbt <<- EOM
scalafixDependencies in ThisBuild +=
  "com.holdenkarau" %% "spark-scalafix-rules-2.4.8" % "0.1.9"
semanticdbEnabled in ThisBuild := true
EOM
mkdir -p project
cat >> project/plugins.sbt <<- EOM
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
EOM
cp ../../../scalafix/.scalafix.conf ./
prompt
echo "Great! Now we'll try and run the scala fix rules in your project! Yay!. This might fail if you have interesting build targets."
sbt scalafix ||     read -p "Linter warnings were found please check then press enter" hifriends
echo "ScalaFix is done, you should probably review the changes (e.g. git diff)"
prompt
sbt clean compile test package
cp -af build.sbt build.sbt.bak.pre3
cat build.sbt.bak.pre3 | \
  python -c "import re,sys;print(sys.stdin.read().replace(\"${INITIAL_VERSION}\", \"${TARGET_VERSION}\"))" > build.sbt
echo "You will also need to update dependency versions now (e.g. Spark to 3.3 and libs)"
echo "Please address those and then press enter."
prompt
sbt clean compile test package
echo "Lovely! Now we \"simulate\" publishing these jars to an S3 bucket (using local fs)"
cd ..
mkdir -p /tmp/spark-migration-jars
cp -af sparkdemoproject*/target/scala-*/*.jar /tmp/spark-migration-jars
echo "Excellent news! All done. Now we just need to make sure we have the same pipeline. Let's magic it!"
cd ../../
cd pipelinecompare
echo "There is some trickery in our spark-submit2 v.s. spark-submit3 including the right iceberg version"
echo "Provided you have iceberg in your environment pre-insalled this should be equivelent to prod but... yeah."
python domagic.py --iceberg --spark-control-command ${spark_submit2} --spark-new-command ${spark_submit3} \
       --new-jar-suffix "-3" \
       --combined-pipeline "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --class com.holdenkarau.sparkDemoProject.CountingLocalApp \
    /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar"
