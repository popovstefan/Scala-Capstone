package observatory

import com.sksamuel.scrimage.Image

import scala.annotation.tailrec
import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    def greatCircleDistance(loc1: Location, loc2: Location): Double = {

      def isAntipodal(_loc1: Location, _loc2: Location): Boolean = {
        if ((-1 * _loc2.lat).equals(_loc1.lat) && (_loc2.lon.equals(_loc1.lon + 180) || _loc2.lon.equals(_loc1.lon - 180)))
          true
        else
          false
      }

      if (loc1.equals(loc2))
        0
      else if (isAntipodal(loc1, loc2) || isAntipodal(loc1, loc2))
        Pi
      else {
        acos(sin(loc1.lat) * sin(loc2.lat) + cos(loc1.lat) * cos(loc2.lat) * cos(loc1.lon - loc2.lon))
      }
    }

    temperatures.map(it => 1 / pow(greatCircleDistance(it._1, location), 2) * it._2).sum / temperatures.size
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    @tailrec
    def findBelow(sortedIterable: Iterable[(Temperature, Color)]): (Temperature, Color) = {
      if (sortedIterable.size < 2)
        sortedIterable.head
      else if (sortedIterable.tail.head._1 <= value)
        sortedIterable.head
      else
        findBelow(sortedIterable.tail)
    }

    @tailrec
    def findAbove(sortedIterable: Iterable[(Temperature, Color)]): (Temperature, Color) = {
      if (sortedIterable.size < 2)
        sortedIterable.head
      else if (sortedIterable.head._1 >= value)
        findAbove(sortedIterable.tail)
      else
        sortedIterable.tail.head
    }

    // Sort points
    val sorted: Iterable[(Temperature, Color)] = points.toSeq.sortBy(_._1)(Ordering.Double.reverse)
    // Find the first below and above the point we are trying to interpolate
    val firstBelow: (Temperature, Color) = findBelow(sorted)
    val firstAbove: (Temperature, Color) = findAbove(sorted)
    // Calculate the ratio of the distances between the points above and below and our "value" point
    val ratio = (firstAbove._1 - firstBelow._1) / (firstAbove._1 - value)
    // Use this ratio to interpolate the R, G and B value
    val red = firstBelow._2.red + (firstAbove._2.red - firstBelow._2.red) * ratio
    val green = firstBelow._2.green + (firstAbove._2.green - firstBelow._2.green) * ratio
    val blue = firstBelow._2.blue + (firstAbove._2.blue - firstBelow._2.blue) * ratio
    // Return a Color object with the interpolated values
    Color(red.toInt, green.toInt, blue.toInt)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

