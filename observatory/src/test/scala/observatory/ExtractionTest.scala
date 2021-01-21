package observatory

import java.time.LocalDate

import org.junit.Test

trait ExtractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _
  private val extractionObject = Extraction

  // Implement tests for the methods of the `Extraction` object
  @Test def `test locateTemperatures with example from the instructions`(): Unit = {
    val actual = extractionObject.locateTemperatures(year = 2015,
      stationsFile = "/home/stefan/Projects/Coursera/Functional Programming in Scala Capstone/ScalaCapstone/observatory/src/main/resources/stations.csv",
      temperaturesFile = "/home/stefan/Projects/Coursera/Functional Programming in Scala Capstone/ScalaCapstone/observatory/src/main/resources/2015.csv")
    val expected = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    )
    expected.foreach { expectedElement =>
      assert(actual.count(actualElement => actualElement._1.equals(expectedElement._1)
        && actualElement._2.equals(expectedElement._2)
        && actualElement._3.equals(expectedElement._3)) == 1, message = "Actual does not contain expected element: " + expectedElement)
    }
  }

  @Test def `test locationYearlyAverageRecords with example from the instructions`(): Unit = {
    val actual = extractionObject.locationYearlyAverageRecords(Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.0)
    ))
    val expected = Seq(
      (Location(37.35, -78.433), 27.3),
      (Location(37.358, -78.438), 1.0)
    )
    expected.foreach { expectedElement =>
      assert(actual.count(actualElement => actualElement._1.equals(expectedElement._1)
        && actualElement._2.equals(expectedElement._2)) == 1, message = "Actual does not contain expected element: " + expectedElement)
    }
  }

}
