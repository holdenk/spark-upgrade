package fix

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import slick.driver.H2Driver.api._

object UnionRewriteQQ {
  def inSource1(spark: SparkSession): Unit = {
    import spark.implicits._
    val df1 = Seq("Person 1", "Person 2").toDF("col1")
    val df2 = Seq("User 1", "User 2").toDF("col1")
    val res = df1.union(df2)
  }

  def inSource2(spark: SparkSession): Unit = {
    import spark.implicits._
    val ds1 = Seq("Person 1", "Person 2").toDS()
    val ds2 = Seq("User 1", "User 2").toDS()
    val r = ds1.union(ds2)
  }

  def inSource3(spark: SparkSession): Unit = {
    import spark.implicits._
    val dss = Seq(
      Seq("Person 1", "Person 2").toDS(),
      Seq("User 1", "User 2").toDS(),
      Seq("Person 31", "Person 32").toDS(),
      Seq("User 41", "User 42").toDS()
    )
    val res = dss.reduce(_ union _)
  }

  def inSource4(spark: SparkSession): Unit = {
    import spark.implicits._
    val dss = List(
      Seq("Person 1", "Person 2").toDF(),
      Seq("User 1", "User 2").toDF(),
      Seq("Person 31", "Person 32").toDF(),
      Seq("User 41", "User 42").toDF()
    )
    val res = dss.reduce(_ union _)
  }

  def inSource5(spark: SparkSession): Unit = {
    import spark.implicits._
    val ds1 = Seq("Person 1", "Person 2").toDS()
    val ds2 = Seq("User 1", "User 2").toDS()
    val ds3 = Seq("City 1", "City 2").toDS()
    val r = ds1.union(ds2).union(ds3)
  }

  def inSource6(ds1: Dataset[String], ds2: Dataset[String]): Unit = {
    val res = ds1.union(ds2)
  }

  def inSource7(
      ds1: Dataset[String],
      ds2: Dataset[String],
      ds3: Dataset[String]
  ): Unit = {
    val res1 = ds1.union(ds2).union(ds3)
    val res2 = ds1.union(ds3)
  }

  def inSource8(
      df1: DataFrame,
      df2: DataFrame,
      df3: DataFrame,
      df4: DataFrame
  ): Unit = {
    val res1 = df1.union(df2).union(df3)
    val res2 = df1.union(df3)
    val res3 = df1.union(df2).union(df3).union(df4)
  }

  def inSourceSlickUnionAll(): Unit = {
    case class Coffee(name: String, price: Double)
    class Coffees(tag: Tag) extends Table[(String, Double)](tag, "COFFEES") {
      def name = column[String]("COF_NAME")

      def price = column[Double]("PRICE")

      def * = (name, price)
    }

    val coffees = TableQuery[Coffees]

    val q1 = coffees.filter(_.price < 8.0)
    val q2 = coffees.filter(_.price > 9.0)

    val unionQuery = q1 union q2
    val unionAllQuery = q1 unionAll q2
    val unionAllQuery1 = q1 ++ q2
  }
}
