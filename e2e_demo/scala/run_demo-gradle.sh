
#!/bin/bash

echo "Hi Friend! If you have questions running this script please reach out on Slack :D"

set -ex

prompt () {
  if [ -z "$NO_PROMPT" ]; then
    read -p "Press enter to continue:" hifriends
  fi
}

########################################################################
# Define variables
########################################################################

INITIAL_VERSION=${INITIAL_VERSION:-2.4.8}
TARGET_VERSION=${TARGET_VERSION:-3.3.1}
SCALAFIX_RULES_VERSION=${SCALAFIX_RULES_VERSION:-0.1.9}
outputTable="local.newest_farttable"


# We DL Spark2 but also slipstreamed spark
SPARK2_DETAILS="spark-2.4.8-bin-without-hadoop-scala-2.12"
CORE_SPARK2="spark-2.4.8-bin-hadoop2.7"
SPARK3_DETAILS="spark-3.3.1-bin-hadoop2"

spark_submit2="$(pwd)/${SPARK2_DETAILS}/bin/spark-submit"
spark_submit3="$(pwd)/${SPARK3_DETAILS}/bin/spark-submit"
spark_sql3="$(pwd)/${SPARK3_DETAILS}/bin/spark-sql"
prompt

########################################################################
# Downloading dependencies
########################################################################

#SKIPPING THIS PART FOR NOW. ASSUMING THIS IS RUN AFTER THE OG DEMO
bash ./fetch_dependencies.sh $CORE_SPARK2 $SPARK2_DETAILS $SPARK3_DETAILS

prompt
########################################################################
# Run scalafix in a cloned dir
########################################################################
echo "Making a copy of the demo project so we can have side-by-side migrated / non-migrated."
rm -rf sparkdemoproject-3
cp -af sparkdemoproject sparkdemoproject-3
echo "Build the current demo project"
cd sparkdemoproject
gradle jar
cd ..
cd sparkdemoproject-3
echo "Now we run the migration setup."

#backup the 
# TODO : Make some script to edit in the scalafix dependencies
mv build.gradle build.gradle.bak
mv build.gradle.scalafix build.gradle

#Copy scalafix
cp ../../../scalafix/.scalafix.conf ./

prompt
echo "Great! Now we'll try and run the scala fix rules in your project! Yay!. This might fail if you have interesting build targets."
gradle scalafix || (echo "Linter warnings were found"; prompt)

echo "ScalaFix is done, you should probably review the changes (e.g. git diff)"
prompt
# We don't run compile test because some changes are not back compat (see value/key change).
cp -af settings.gradle settings.gradle.scalafix.bak.pre3
cat settings.gradle | \
  python -c "import re,sys;print(sys.stdin.read().replace(\"${INITIAL_VERSION}\", \"${TARGET_VERSION}\"))" > settings.gradle
echo "You will also need to update dependency versions now (e.g. Spark to 3.3 and libs)"
echo "Please address those and then press enter."
prompt
gradle jar

echo "Lovely! Now we \"simulate\" publishing these jars to an S3 bucket (using local fs)"
cd ..
mkdir -p /tmp/spark-migration-jars
cp -af sparkdemoproject*/build/libs/*.jar /tmp/spark-migration-jars
echo "Excellent news! All done. Now we just need to make sure we have the same pipeline. Let's magic it!"
cd ../../

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
   -e "CREATE TABLE ${outputTable} (word string, count long) USING iceberg TBLPROPERTIES('write.wap.enabled' = 'true')"


# TODO : Call the domagic script. Not entirely sure about why the original script calls /tmp/spark-migration-jars/sparkdemoproject_2.12-0.0.1.jar for the domagic?
# Maybe I've messed up the jar name on creation, mine is called sparkdemoproject-2.4.8-0.0.1.jar






