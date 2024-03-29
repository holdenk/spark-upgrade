package fix

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GroupByKeyRenameColumnQQ {
  case class City(countryName: String, cityName: String)
  def inSource(spark: SparkSession): Unit = {
    import spark.implicits._

    val ds = List("Person 1", "Person 2", "User 1", "User2", "Test").toDS()
    // Don't change the RDD one.
    val sc = SparkContext.getOrCreate()
    val rdd = sc.parallelize(List((1,2))).groupByKey().map(x => "value")

    // Do change the inidrect ds ones
    val ds11 =
      ds.groupByKey(c => c.substring(0, 3)).count().select(col("key"))
    val df: DataFrame = null
    var words1: Dataset[Row] = null
    def keyMe(a: Row): String = {
      "1"
    }
    val stopArray = array(lit("hi"))
    val splitPattern = ""
    words1.groupByKey(keyMe).count().select(col("key").as("word"), col("count(1)")).orderBy("count(1)")
    val words = df.select(explode(split(lower(col("value")), splitPattern)).as("words")).filter(
      not(array_contains(stopArray, col("words"))))
    words.groupByKey(keyMe).count().select(col("key").as("word"), col("count(1)")).orderBy("count(1)")

    val ds10 = List("Person 1", "Person 2", "User 1", "User 3", "test")
      .toDS()
      .groupByKey(i => i.substring(0, 3))
      .count()
      .select(col("key"))

    val ds1 = List("Person 1", "Person 2", "User 1", "User 3", "test")
      .toDS()
      .groupByKey(i => i.substring(0, 3))
      .count()
      .select('key)

    val ds2 = List("Person 1", "Person 2", "User 1", "User 3", "test")
      .toDS()
      .groupByKey(i => i.substring(0, 3))
      .count()
      .withColumn("key", upper(col("key")))

    val ds3 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .withColumnRenamed("key", "newName")

    val ds5 =
      List("Paerson 1", "Person 2", "User 1", "User 2", "test", "gggg")
        .toDS()
        .groupByKey(l => l.substring(0, 3))
        .count()
        .withColumn("newNameCol", upper(col("key")))

    val ds00 = List("Person 1", "Person 2", "User 1", "User 3", "test")
      .toDS()
      .select(col("value"))

    val c = List("Person 1", "Person 2", "User 1", "User 3", "test")
      .toDS()
      .withColumnRenamed("value", "newColName")
      .count()

    val s = "value"
    val value = 1
  }
  def inSource2(spark: SparkSession): Unit = {
    import spark.implicits._

    val source = Seq(
      City("USA", "Seatle"),
      City("Canada", "Toronto"),
      City("Ukraine", "Kyev"),
      City("Ukraine", "Ternopil"),
      City("Canada", "Vancouver"),
      City("Germany", "Köln")
    )

    val df = source.toDF().groupBy(col("countryName")).count
    val ds = source.toDS().groupBy(col("countryName")).count
    val res = source.toDF().as[City].groupBy(col("countryName")).count
    val res1 = res
      .select(col("countryName").alias("value"))
      .as[String]
      .groupByKey(l => l.substring(0, 3))
      .count()
    val res2 = res
      .select(col("countryName").alias("newValue"))
      .as[String]
      .groupByKey(l => l.substring(0, 3))
      .count()
      .select('key)
  }
  def inSource3(spark: SparkSession): Unit = {
    import spark.implicits._

    val source = Seq(
      City("USA", "Seatle"),
      City("Canada", "Toronto"),
      City("Ukraine", "Kyev"),
      City("Ukraine", "Ternopil"),
      City("Canada", "Vancouver"),
      City("Germany", "Köln")
    )
    val res = source.toDF().as[City].groupBy(col("countryName")).count
    val res1 = res
      .select(col("countryName").alias("value"))
      .as[String]
      .groupByKey(l => l.substring(0, 3))
      .count()

    val ds = List("Person 1", "Person 2", "User 1", "User 2").toDS()

    val res2 = res1.union(ds.groupByKey(l => l.substring(0, 3)).count)

    val r = res2.select('value, col("count(1)"))
  }
}
