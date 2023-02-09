/*
rule=GroupByKeyWarn
 */
package fix
import org.apache.spark.sql.{SparkSession, Dataset}

class GroupByKeyWarn {
  def inSource(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val ds1 = List( // assert: GroupByKeyWarn
      "Person 1",
      "Person 2",
      "User 1",
      "User 2",
      "User 3",
      "Test",
      "Test Test"
    ).toDS()
      .groupByKey(l => l.substring(0, 3)) // assert: GroupByKeyWarn
      .count()

    val noChange: Dataset[String] = List("1").toDS()
    val noChangeMore = List("1").toDS().count()

    // Make sure we trigger not just on toDS
    val someChange = noChange.groupByKey(l => // assert: GroupByKeyWarn
          l.substring(0, 3).toUpperCase()
        )
        .count()

    val ds2: Dataset[(String, Long)] =
      List("Test 1", "Test 2", "user 1", "Person 1", "Person 2") // assert: GroupByKeyWarn
        .toDS()
        .groupByKey(l => // assert: GroupByKeyWarn
          l.substring(0, 3).toUpperCase()
        )
        .count()

    val ds3 =
      List(1, 2, 3, 4, 5, 6) // assert: GroupByKeyWarn
        .toDS()
        .groupByKey(l => l > 3) // assert: GroupByKeyWarn
        .count()

    val ds4 =
      List(Array(19, 12), Array(1, 2, 3, 4, 5, 6), Array(678, 99, 88)) // assert: GroupByKeyWarn
        .toDS()
        .groupByKey(l => l.length >= 3) // assert: GroupByKeyWarn
        .count()

    val ds5 = Seq("test 1", "test 2", "test 3").toDS()

    val ds6 = Seq("test 1", "test 2", "test 3").toDS().groupBy("value").count()

    val ds7 = Seq("test 1", "test 2", "test 3").toDF().groupBy("value").count()
  }

}
