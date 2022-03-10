import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Spark_assignment_2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Spark_assignment_2")

  val torrentLogData = sc.textFile("src/main/resources/ghtorrent-logs.txt")
  def parseLine1(line1: String): String ={
    val fields = line1.split(",")
    fields(0)
  }
  def parseLine2(line2: String): String = {
    val fields = line2.split(",")
    fields(2)

  }

  def numberOfLinesInRDD(): Long = {
    val rddLines1 = torrentLogData.map(parseLine1)
    val noOfLines = rddLines1.collect()
    var countingOfLines = 0
    for (i <- noOfLines)
      countingOfLines = countingOfLines + 1
    println(countingOfLines)
    rddLines1.count()

  }

  def countingNumberOfWarningMessages(): Unit = {
    val rddLines2 = torrentLogData.map(parseLine1)
    val warningCount = rddLines2.filter(x => x == "WARN")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)

  }

  def countingApiRepositiries(): Unit = {
    val rddlines3 = torrentLogData.flatMap(x => x.split(" "))
      .filter(x => x == "api_client.rb:")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)

  }

  def countingClientWithMostHttpRequest(): Unit = {
    val rddlines4 = torrentLogData.flatMap(x => x.split(","))
      .filter(x => x.contains("URL:") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).foreach(println)

  }

  def countingNumberOfFailedHttp(): Unit = {
    val rddlines5 = torrentLogData.flatMap(x => x.split(","))
      .filter(x => x.contains("Failed ") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).foreach(println)


  }

  def countingMostActiveHourOfDay() = {
    val rddLines6 = torrentLogData.flatMap(x => x.split(","))
      .filter(x => x.contains("+00"))
      .map(x => (x.substring(0, 11),((x.substring(12, 14)), 1)))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .foreach(println)

  }

  def countingMostActiveRepository() = {
    val requiredRddLines = torrentLogData.flatMap(x => x.split(","))
      .filter(x => x.contains("ghtorrent.rb: Repo") && x.contains("exists"))
      .map(x => (x.substring(x.indexOf("Repo")+5, x.indexOf("exists")-1),1))
      .reduceByKey((x, y) => x + y).sortBy(x => x._2, false).take(20)
      .foreach(println)

  }
  println("1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
  println("Counting the number of Lines the RDD file Contains" +"\n" + numberOfLinesInRDD)
  println("1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
  println("2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222")
  println("Counting the number of WARNING Messages" +"\n" + countingNumberOfWarningMessages)
  println("222222222222222222222222222222222222222222222222222222222222222222222222222222222222222")
  println("333333333333333333333333333333333333333333333333333333333333333333333333333333333333333")
  println("Counting the number of API Repositories" +"\n" + countingApiRepositiries)
  println("3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333")
  println("4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444")
  println("Counting the Clients with Most HTTP Request" +"\n" + countingClientWithMostHttpRequest)
  println("4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444")
  println("5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555")
  println("Counting the number of FAILED HTTP Request" +"\n" + countingNumberOfFailedHttp)
  println("5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555")
  println("6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666")
  println("Counting the Most Active Hours of the Day" +"\n" + countingMostActiveHourOfDay)
  println("6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666")
  println("7777777777777777777777777777777777777777777777777777777777777777777777777777777777777777")
  println("Counting the Most Active Repositories" +"\n" + countingMostActiveRepository)
  println("7777777777777777777777777777777777777777777777777777777777777777777777777777777777777777")

}
