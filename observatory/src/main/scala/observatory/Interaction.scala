package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColor, predictTemperature}


/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    // The constant factor in both x and y formulae
    val constant: Double = (256 / (2 * math.Pi)) * math.pow(2, tile.zoom)
    // Calculate longitude, in radians
    val lambda: Double = (tile.x - constant * math.Pi) / constant
    // Calculate latitude in multiple steps, in radians
    // Step 1. Multiply constant and transfer to other side, leaving only the ln and tan functions on the one side
    val lnOfTan: Double = (constant * math.Pi - tile.y) / constant
    // Step 2. Remove the ln by applying it's inverse function; given y = ln(x), x = y^e
    val tan: Double = math.exp(lnOfTan)
    // Step 3. Remove the tan by applying it's inverse function; given y = tan(x), x = atan(y)
    val tanArgument: Double = math.atan(tan)
    // Step 4. Get phi value from the tan argument
    val phi: Double = 2 * tanArgument - (math.Pi / 2)
    // Return Location object, with the radian values converted to degrees
    Location(lat = math.toDegrees(phi), lon = math.toDegrees(lambda))
    /*
    // Simpler calculation, taken from:
    // https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Implementations
    val n = math.pow(2, tile.zoom)
    val longitude = tile.x / n * 360d - 180
    val latitude = math.toDegrees(
      math.atan(math.sinh(math.Pi * (1 - 2 * tile.y / n)))
    )
    Location(lat = latitude, lon = longitude)
    */
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    // Initialize variables
    val imageWidth: Int = 256
    val imageHeight: Int = 256
    val alpha: Int = 127 // transparency value, recommended by the instructors
    val offsetX: Int = tile.x * imageWidth
    val offsetY: Int = tile.y * imageHeight
    // Create coordinate pairs
    val coordinates = for {
      i <- 0 until imageWidth
      j <- 0 until imageHeight
    } yield (i, j)
    // Initialize pixel array in parallel
    val pixels: Array[Pixel] = coordinates.par.map {
      case (x, y) => Tile(x = x + offsetX, y = y + offsetY, zoom = 8 + tile.zoom)
    }.map(tileLocation)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(color => Pixel(color.red, color.green, color.blue, alpha))
      .toArray
    // Return result
    Image(imageWidth, imageHeight, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Year, Data)],
                           generateImage: (Year, Tile, Data) => Unit
                         ): Unit = {
    for {
      zoom <- 0 until 4
      x <- 0 until math.pow(2, zoom).toInt
      y <- 0 until math.pow(2, zoom).toInt
      yearData <- yearlyData
    } yield generateImage(yearData._1, Tile(x = x, y = y, zoom = zoom), yearData._2)
  }

}
