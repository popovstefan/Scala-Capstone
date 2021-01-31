package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math._

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val P = 3 // power parameter

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    def distance(l1: Location, l2: Location): Double = {
      // Define earth radius variable
      val earthRadiusKM = 6372.8
      // Convert degrees to radians
      val phi1 = math.toRadians(l1.lat)
      val phi2 = math.toRadians(l2.lat)
      val lambda1 = math.toRadians(l1.lon)
      val lambda2 = math.toRadians(l2.lon)
      // Apply formula
      val greatCircleDistance = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda1 - lambda2)))
      // Return result
      earthRadiusKM * greatCircleDistance
    }

    def aggregateByWeightedInverseDistancing(distanceTemperaturePairs: Iterable[(Double, Temperature)], power: Int): Double = {
      val (weightedSum, inverseWeightedSum) = distanceTemperaturePairs.aggregate((0.0, 0.0))(
        { // sequential operation
          case ((ws, iws), (distance, temp)) =>
            val w = 1 / pow(distance, power)
            (w * temp + ws, w + iws)
        }, { // combination operation
          case ((wsA, iwsA), (wsB, iwsB)) => (wsA + wsB, iwsA + iwsB)
        }
      )
      weightedSum / inverseWeightedSum
    }


    val distanceTemperaturePairs: Iterable[(Double, Temperature)] = temperatures.map {
      case (otherLocation, temperature) => (distance(otherLocation, location), temperature)
    }
    val nearbyTemperatures = distanceTemperaturePairs.filter(_._1 < 1)
    if (nearbyTemperatures.isEmpty)
      aggregateByWeightedInverseDistancing(distanceTemperaturePairs, power = P)
    else
      nearbyTemperatures.map(_._2).min
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    def inner(sortedPointsArray: Array[(Temperature, Color)]): Color = {
      for (i <- 0 until sortedPointsArray.length - 1) {
        (sortedPointsArray(i), sortedPointsArray(i + 1)) match {
          case ((t1, Color(r1, g1, b1)), (t2, Color(r2, g2, b2))) =>
            if (t1 > value)
              return Color(r1, g1, b1)
            else if (t2 > value) {
              // Calculate the ratio between the distances of {t1} and {t2} with {value}
              val ratio = (value - t1) / (t2 - t1)
              // Calculate and round R, G, and B components
              val r = math.round(r1 + (r2 - r1) * ratio)
              val g = math.round(g1 + (g2 - g1) * ratio)
              val b = math.round(b1 + (b2 - b1) * ratio)
              return Color(r.toInt, g.toInt, b.toInt)
            }
        }
      }
      // If value is not within the list, return the maximum (i.e. last) color value
      sortedPointsArray.last._2
    }

    inner(points.toList.sortBy(_._1)(Ordering.Double).toArray)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def pixelToLocation(x: Int, y: Int) = Location(lat = 90 - y, lon = x - 180)

    // Initialize image properties
    val alpha = 255
    val imageWidth = 360
    val imageHeight = 180
    // Initialize pixel array
    val imageBufferValues = new Array[Pixel](imageHeight * imageWidth)
    // Iterate over each pixel and interpolate it's value
    for (y <- 0 until imageHeight)
      for (x <- 0 until imageWidth) {
        // Predict temperature at the pixel
        val temperatureAtPixel: Temperature = predictTemperature(temperatures, pixelToLocation(x, y))
        // Interpolate the color based on the predicted temperature
        val colorAtPixel: Color = interpolateColor(colors, temperatureAtPixel)
        // Apply the interpolated color at the pixel
        imageBufferValues(y * imageWidth + x) = Pixel.apply(colorAtPixel.red, colorAtPixel.green, colorAtPixel.blue, alpha)
      }
    Image(imageWidth, imageHeight, imageBufferValues)
  }

}

