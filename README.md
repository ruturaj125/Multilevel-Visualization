# Multilevel_Visualization_on_Vector_Data
This file contains the code for Multilevel Visualization on vector tiles in Beast.
This project aims to apply multilevel visualization on vector tiles and generate multiple tiles on multiple levels simultaneously.

Main.scala
Main file here shows how to use PlotMultipleTiles Function.

VectorTilePlotter.scala
PlotMultipleTiles() generates tiles from levels provided in input. 
createImageTilesHybrid() is the implementaion of multilevel visualization on vector data.

To run the code:
Add these methods mentioned above in the VectorTilePlotter class in the Beast Visualization.
Call PlotMultipleTiles() as shown in Main.scala

After MVT files generated those can be viewd in browser using ports.html



