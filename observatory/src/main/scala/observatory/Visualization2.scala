package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
                             point: CellPoint,
                             d00: Temperature,
                             d01: Temperature,
                             d10: Temperature,
                             d11: Temperature
                           ): Temperature = {
    val x = point.x
    val nx = 1 - x
    val y = point.y
    val ny = 1 - y
    d00 * nx * ny + d10 * x * ny + d01 * nx * y + d11 * x * y
  }

  def createGridLocationsAndInterpolate(grid: GridLocation => Temperature, location: Location): Temperature = {
    val lat = location.lat.toInt
    val lon = location.lon.toInt
    // Create GridLocations
    val p00: GridLocation = GridLocation(lat, lon)
    val p01: GridLocation = GridLocation(lat + 1, lon)
    val p10: GridLocation = GridLocation(lat, lon + 1)
    val p11: GridLocation = GridLocation(lat + 1, lon + 1)
    val point: CellPoint = CellPoint(x = location.lon - lon, y = location.lat - lat)
    // Interpolate
    bilinearInterpolation(point, grid(p00), grid(p01), grid(p10), grid(p11))
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
                     grid: GridLocation => Temperature,
                     colors: Iterable[(Temperature, Color)],
                     tile: Tile
                   ): Image = {
    // Initialize variables
    val power = 8
    val imageWidth: Int = math.pow(2, power).toInt
    val imageHeight: Int = math.pow(2, power).toInt
    val alpha = 256
    val offsetX: Int = tile.x * imageWidth
    val offsetY: Int = tile.y * imageHeight
    // Create coordinate pairs
    val coordinates: Seq[(Int, Int)] = for {
      i <- 0 until imageWidth
      j <- 0 until imageHeight
    } yield (i, j)
    // Initialize pixel array in parallel
    val pixels: Array[Pixel] = coordinates.par
      .map {
        case (x, y) => Tile(x = x + offsetX, y = y + offsetY, zoom = power + tile.zoom)
      }
      .map(Interaction.tileLocation)
      .map(createGridLocationsAndInterpolate(grid, _))
      .map(Visualization.interpolateColor(colors, _))
      .map(color => Pixel(color.red, color.green, color.blue, alpha))
      .toArray
    // Return result
    Image(imageWidth, imageHeight, pixels)
  }

}
