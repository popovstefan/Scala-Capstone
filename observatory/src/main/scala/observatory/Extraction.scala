package observatory

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    // Define column names
    val stationColumnNames = List("STN_Identifier", "WBAN_Identifier", "Latitude", "Longitude")
    val temperatureColumnNames = List("STN_Identifier", "WBAN_Identifier", "Month", "Day", "Temperature_F")
    // Read the files
    val stations: DataFrame = spark.read.format("csv")
      .option("header", "false")
      .load(stationsFile)
      .toDF(stationColumnNames: _*)
    val temperatures: DataFrame = spark.read.format("csv")
      .option("header", "false")
      .load(temperaturesFile)
      .toDF(temperatureColumnNames: _*)
    // Note on the "list: _*" syntax: https://stackoverflow.com/a/15034725/15052008
    // Register DataFrames as TempViews
    stations.createOrReplaceTempView("stations")
    temperatures.createOrReplaceTempView("temperatures")
    // Execute SQL query to transform data in a more convenient format
    spark.sql(
      """
        |select t.Month, t.Day, s.Latitude, s.Longitude, ((t.Temperature_F - 32) / 1.8) as Temperature_C
        |from stations as s inner join temperatures as t
        |on s.STN_Identifier = t.STN_Identifier and s.WBAN_Identifier = t.WBAN_Identifier
        |where s.Latitude is not null and s.Longitude is not null""".stripMargin)
      .collect()
      .map {
        case Row(month: String, day: String, latitude: String, longitude: String, temperature: Double) =>
          (LocalDate.of(year, month.toInt, day.toInt), Location(lat = latitude.toDouble, lon = longitude.toDouble), temperature)
      }
      .toSeq
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
      .toSeq
  }

}
