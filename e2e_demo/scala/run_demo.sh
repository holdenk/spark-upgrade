#!/bin/bash

echo "Hi Friend! If you have questions running this script please reach out on Slack :D"

set -ex

prompt () {
  if [ -z "$NO_PROMPT" ]; then
    read -p "Press enter to continue:" hifriends
  fi
}

./cleanup.sh
cd ./sparkdemoproject && sbt clean && cd ..

########################################################################
# Setting variables
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

########################################################################
# Downloading dependencies
########################################################################

source dl_dependencies.sh

########################################################################
# Run scalafix in a cloned dir
########################################################################
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
# Adding the scalafix dependency to the Spark 3 copy of the project
cp -af build.sbt build.sbt.bak
cat build.sbt.bak | \
  python -c 'import re,sys;print(re.sub(r"name :=\s*\"(.*?)\"", "name :=\"\\1-3\"", sys.stdin.read()))' > build.sbt
cat >> build.sbt <<- EOM
scalafixDependencies in ThisBuild +=
  "com.holdenkarau" %% "spark-scalafix-rules-2.4.8" % "${SCALAFIX_RULES_VERSION}"
semanticdbEnabled in ThisBuild := true
EOM
mkdir -p project
cat >> project/plugins.sbt <<- EOM
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
EOM
cp ../../../scalafix/.scalafix.conf ./
prompt
echo "Great! Now we'll try and run the scala fix rules in your project! Yay!. This might fail if you have interesting build targets."
sbt scalafix
echo "Huzzah running the warning check..."
cp ../../../scalafix/.scalafix-warn.conf ./.scalafix.conf
sbt scalafix ||     (echo "Linter warnings were found"; prompt)
echo "ScalaFix is done, you should probably review the changes (e.g. git diff)"
prompt
# We don't run compile test because some changes are not back compat (see value/key change).
# sbt clean compile test package
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

#Build the iceperg spark upgrade plugin
cd iceberg-spark-upgrade-wap-plugin
sbt clean compile test package

#Go into the pipeline compare dir
cd ..
cd pipelinecompare
echo "There is some trickery in our spark-submit2 v.s. spark-submit3 including the right iceberg version"
echo "Provided you have iceberg in your environment pre-insalled this should be equivelent to prod but... yeah."
# Exepected to pass.
# Note: here we insert some data into our test table so that we have a common ancestor and use CDC view.
${spark_sql3}     --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
   -e "CREATE TABLE IF NOT EXISTS ${outputTable} (word string, count long) USING iceberg TBLPROPERTIES('write.wap.enabled' = 'true'); INSERT INTO ${outputTable} VALUES ('baked potato', 42)"
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
prompt
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
