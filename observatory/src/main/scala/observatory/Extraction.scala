package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.time.LocalDate
import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  val appName = "Functional Programming in Scala - Capstone"
  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master("local")
    .getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  // Define case classes
  // These need to be outside of the method where createDataFrame is called:
  // https://intellipaat.com/community/18751/scala-spark-app-with-no-typetag-available-error-in-def-main-style-app
  case class Station(stnIdentifier: String, wbanIdentifier: String, latitude: Double, longitude: Double)

  case class TemperatureMeasurement(stnIdentifier: String, wbanIdentifier: String, month: Int, day: Int, temperatureFahrenheit: Temperature)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    // TODO: Fix logical bug
    // Read the files
    val stationBuffer = Source.fromInputStream(getClass.getResourceAsStream(stationsFile), "utf-8")
    val temperatureBuffer = Source.fromInputStream(getClass.getResourceAsStream(temperaturesFile), "utf-8")
    // Stream over the lines and create a list of objects
    var stationSeq: Seq[Station] = Seq()
    for (elem <- stationBuffer.getLines().toStream) {
      val parts: Array[String] = elem.split(",")
      if (parts.length > 3) // ignore stations with no known location
        stationSeq = stationSeq :+ Station(parts(0), parts(1), parts(2).toDouble, parts(3).toDouble)
    }
    var temperatureSeq: Seq[TemperatureMeasurement] = Seq()
    for (elem <- temperatureBuffer.getLines().toStream) {
      val parts: Array[String] = elem.split(",")
      temperatureSeq = temperatureSeq :+ TemperatureMeasurement(parts(0), parts(1), parts(2).toInt, parts(3).toInt, parts(4).toDouble)
    }
    // Create DataFrames
    val stations: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(stationSeq))
    val temperatures: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(temperatureSeq))
    // Register DataFrames as TempViews
    stations.createOrReplaceTempView("stations")
    temperatures.createOrReplaceTempView("temperatures")
    // Execute SQL query to transform data in a more convenient format
    spark.sql(
      """
        |select t.month, t.day, s.latitude, s.longitude, ((t.temperatureFahrenheit - 32) / 1.8) as Temperature_C
        |from stations as s inner join temperatures as t
        |on s.stnIdentifier = t.stnIdentifier and s.wbanIdentifier = t.wbanIdentifier""".stripMargin)
      .collect()
      .map {
        case Row(month: String, day: String, latitude: String, longitude: String, temperature: Double) =>
          (LocalDate.of(year, month.toInt, day.toInt), Location(lat = latitude.toDouble, lon = longitude.toDouble), temperature)
        case Row(month: Int, day: Int, latitude: Double, longitude: Double, temperature: Double) =>
          (LocalDate.of(year, month, day), Location(lat = latitude, lon = longitude), temperature)
      }
      .toIterable
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {

    def sumTemperatures(groupedIterable: Iterable[(LocalDate, Location, Temperature)]): Double = {
      groupedIterable.map(_._3).sum
    }

    val recordsRDD: RDD[(LocalDate, Location, Temperature)] = spark.sparkContext.parallelize(records.toSeq)
    recordsRDD.groupBy(_._2)
      .aggregateByKey(zeroValue = (0d, 0))(seqOp = (acc, groupedIterable) => (acc._1 + sumTemperatures(groupedIterable), acc._2 + groupedIterable.size), combOp = (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(x => x._1 / x._2)
      .collect()
      .toIterable
  }

}
