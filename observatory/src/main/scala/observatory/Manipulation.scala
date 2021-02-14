package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {

  class Grid {
    private var temperatures: Array[Temperature] = new Array[Temperature](_length = 360 * 180)

    private def at(lat: Int, lon: Int): Int = (lat + 89) * 360 + (lon + 180)

    def set(lat: Int, lon: Int, temperature: Temperature): Unit = temperatures(at(lat, lon)) = temperature

    def get(lat: Int, lon: Int): Temperature = temperatures(at(lat, lon))

    def memoize(temps: Iterable[(Location, Temperature)]): Unit = for {
      lat <- Range(90, -90, -1)
      lon <- -180 until 180
    } set(lat, lon, Visualization.predictTemperature(temps, Location(lat, lon)))

    // Operator overloading, for easier temperature manipulation
    def +=(that: Grid): Grid = {
      temperatures.indices.foreach(index => this.temperatures(index) += that.temperatures(index))
      this
    }

    def -=(that: GridLocation => Temperature): Grid = {
      for {
        lat <- Range(90, -90, -1)
        lon <- -180 until 180
      } set(lat, lon, get(lat, lon) - that(GridLocation(lat, lon)))
      this
    }

    def /=(denominator: Double): Grid = {
      temperatures = temperatures.map(_ / denominator)
      this
    }
  }

  def getMemoizedInstance(temps: Iterable[(Location, Temperature)]): Grid = {
    val instance = new Grid()
    instance.memoize(temps)
    instance
  }

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val instance: Grid = getMemoizedInstance(temperatures)
    (gridLocation: GridLocation) => instance.get(gridLocation.lat, gridLocation.lon)
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val instance: Grid = temperaturess.par
      .map(getMemoizedInstance) // from each inner iterable, make a grid
      .reduce((a: Grid, b: Grid) => a += b) // sum up all grids (all years)
    instance /= temperaturess.size // calculate average
    (gridLocation: GridLocation) => instance.get(gridLocation.lat, gridLocation.lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val instance: Grid = getMemoizedInstance(temperatures)
    instance -= normals
    (gridLocation: GridLocation) => instance.get(gridLocation.lat, gridLocation.lon)
  }


}

