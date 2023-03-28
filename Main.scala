

import edu.ucr.cs.bdlab.beast._

val features = sparkContext.shapefile("NE_ports.zip")

val startTimeMillis = System.currentTimeMillis()
var outputName: String = "NE_ports"
var minLevel = 0
var maxLevel = 3
var allImageTiles: Unit = VectorTilePlotter.plotMultipleTiles(features, 1024, 2, parsedCLO.options
  , outputName, minLevel, maxLevel)
val endTimeMillis = System.currentTimeMillis()
val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
print(s"Running Time " + durationSeconds)
