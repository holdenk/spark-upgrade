#!/bin/bash

echo "Hi Friend! If you have questions running this script please reach out on Slack :D"

set -ex


prompt () {
  if [ -z "$NO_PROMPT" ]; then
    echo $'\n\n\n'
    read -p "$1. Press enter to continue:" hifriends
  fi
}

bash ./cleanup.sh
cd ./sparkdemoproject && gradle clean && cd ..

########################################################################
# Define variables
########################################################################

INITIAL_VERSION=${INITIAL_VERSION:-2.4.8}
TARGET_VERSION=${TARGET_VERSION:-3.3.1}
SCALAFIX_RULES_VERSION=${SCALAFIX_RULES_VERSION:-0.1.13}
outputTable="local.newest_farttable"

SPARK2_DETAILS="spark-2.4.8-bin-without-hadoop-scala-2.12"
CORE_SPARK2="spark-2.4.8-bin-hadoop2.7"
SPARK3_DETAILS="spark-3.3.1-bin-hadoop2"

spark_submit2="$(pwd)/${SPARK2_DETAILS}/bin/spark-submit"
spark_submit3="$(pwd)/${SPARK3_DETAILS}/bin/spark-submit"
spark_sql3="$(pwd)/${SPARK3_DETAILS}/bin/spark-sql"

prompt "Env setup done. Next we'll download dependencies."
########################################################################
# Downloading dependencies
########################################################################

source dl_dependencies.sh

prompt "Dependencies fetched. Will proceed to setup now."
########################################################################
# Run scalafix in a cloned dir
########################################################################
echo "Making a copy of the demo project so we can have side-by-side migrated / non-migrated."
rm -rf sparkdemoproject-3
cp -af sparkdemoproject sparkdemoproject-3
echo "Build the current demo project"
cd sparkdemoproject
gradle clean test jar
cd ..
cd sparkdemoproject-3

echo "Now we run the migration setup."
cat ../../../docs/scala/gradle.md

cp build.gradle build.gradle.bak
cp gradle.properties gradle.properties.bak
cp settings.gradle settings.gradle.bak
cat settings.gradle.bak | \
  python ../update_gradle_settings.py > settings.gradle

cat build.gradle.bak | \
    python ../update_gradle_build.py  > build.gradle


#Copy scalafix
cp ../../../scalafix/.scalafix.conf ./

prompt "Setup for scalafix complete"

echo "Great! Now we'll try and run the scala fix rules in your project! Yay!. This might fail if you have interesting build targets."
gradle scalafix #|| (echo "Linter warnings were found"; prompt)

echo "ScalaFix is done, you should probably review the changes (e.g. git diff)"

prompt "Scalafix run complete"

echo "You will also need to update dependency versions now (e.g. Spark to 3.3 and libs)"
echo "Please address those and then press enter."

prompt "Build file setup done. Next, we will build a jar"
gradle jar

prompt "Jar has been built. Check build/libs for the jar"

echo "Lovely! Now we \"simulate\" publishing these jars to an S3 bucket (using local fs)"
cd ..
mkdir -p /tmp/spark-migration-jars
cp -af sparkdemoproject*/build/libs/*.jar /tmp/spark-migration-jars
echo "Excellent news! All done. Now we just need to make sure we have the same pipeline. Let's magic it!"
cd ../../

prompt "Jars published. Check /tmp/spark-migration-jars."


## AT THIS POINT WE HAVE TWO JARS, ONE FROM e2e_demo/scala/sparkdemoproject/build/libs/sparkdemoproject-2.4.8-0.0.1.jar
## OTHER FROM e2e_demo/scala/sparkdemoproject-3/build/libs/sparkdemoproject-3.3.1-0.0.1.jar

## Both of those are in /tmp/spark-migration-jars

########################################################################
# Pipeline comparison
########################################################################
#Build the iceberg spark upgrade plugin
cd iceberg-spark-upgrade-wap-plugin
sbt clean compile test package
cd ..

echo "Iceberg spark plugin built. Next we will run a pipeline comparison"
prompt "Creating temp iceberg table next"

#Go into the pipeline compare dir
cd pipelinecompare
echo "There is some trickery in our spark-submit2 v.s. spark-submit3 including the right iceberg version"
echo "Provided you have iceberg in your environment pre-insalled this should be equivelent to prod but... yeah."
# Exepected to pass
${spark_sql3}     --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
   -e "CREATE TABLE IF NOT EXISTS ${outputTable} (word string, count long) USING iceberg TBLPROPERTIES('write.wap.enabled' = 'true')"


python domagic.py --iceberg --spark-control-command ${spark_submit2} --spark-new-command ${spark_submit3} \
       --spark-command ${spark_submit3} \
       --new-jar-suffix "-3" \
       --warehouse-config " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    " \
       --combined-pipeline " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --class com.holdenkarau.sparkDemoProject.CountingLocalApp \
    /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar utils.py ${outputTable}"
echo "Pipeline migration passed! Yay!"
echo "Press enter to see how it can fail (e.g. using /var/log/syslog which gets extra records as we go)"
echo "In that case the user would need to configure a tolerance value for difference."

prompt "This next job should fail"
# Expected to fail because syslog changes between runs.
(python domagic.py --iceberg --spark-control-command ${spark_submit2} --spark-new-command ${spark_submit3} \
       --spark-command ${spark_submit3} \
       --new-jar-suffix "-3" \
       --warehouse-config " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    " \
       --combined-pipeline " \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --class com.holdenkarau.sparkDemoProject.CountingLocalApp \
    /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar /var/log/syslog local.old_farttable" && exit 1) || echo "Failed as expected."
