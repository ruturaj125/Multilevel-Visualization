/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.cg.{Reprojector, SpatialDataTypes}
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EmptyGeometry, EnvelopeNDLite, Feature, GeometryReader, IFeature}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin._
import edu.ucr.cs.bdlab.beast.io.SpatialWriter
import edu.ucr.cs.bdlab.davinci.MultilevelPlot.{ConcatenateImageTiles, TileHeight, TileWidth, createImageTilesHybrid, saveImageTiles, saveImageTilesCompact}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.operation.transform.{AffineTransform2D, ConcatenatedTransform}
import org.locationtech.jts.geom.{Envelope, TopologyException}
import org.opengis.referencing.operation.MathTransform
import org.geotools.referencing.crs.DefaultGeographicCRS

import java.awt.geom.AffineTransform
import java.io.FileOutputStream

/**
 * Functions for plotting a geometry layer as a vector image in the MVT format
 */
object VectorTilePlotter extends Logging {

  /** The maximum angular latitude of the web mercator projection in degrees */
  private val WebMercatorMaxLatitude: Double = 2 * Math.atan(Math.exp(Math.PI)) * 180 / Math.PI - 90

  /** The MBR that defines the range of the entire Web Mercator space */
  private val WebMercatorMBR: EnvelopeNDLite = new EnvelopeNDLite(2, -20037508.34, 20037508.34,
      20037508.34, -20037508.34)

  /** The pyramid structure for the web mercator */
  private val WebMercatorPyramid: SubPyramid = new SubPyramid(WebMercatorMBR, 0, 19)

  /**
   * Plots the given set of features as a vector tile according to Mapbox specifications.
   * @param features the set of features to plot
   * @param resolution the resolution of the image in pixels
   * @param tileID the ID of the tile to plot
   * @param buffer additional pixels around the tile to plot from all directions (default is zero)
   * @param opts additional options to customize the plotting
   * @return a vector tile that contains all the given features
   */
  def plotSingleTile(features: SpatialDataTypes.SpatialRDD, resolution: Int,
                     tileID: Long, buffer: Int = 0, opts: BeastOptions = new BeastOptions()): VectorTile.Tile = {
    val sparkConf = features.sparkContext.getConf
    // 1. Calculate the MBR of the region of interest based on the tile index in the WebMercator CRS
    val tileMBRMercator: Envelope = new Envelope
    TileIndex.getMBR(WebMercatorMBR, tileID, tileMBRMercator)

    // Set up a transformation that transforms geometries to the image space
    // 1. Convert to web mercator
    val sourceSRID = features.first().getGeometry.getSRID
    val sourceToWebMercator: MathTransform =
      Reprojector.findTransformationInfo(sourceSRID, 3857, sparkConf).mathTransform
    // 2. Convert from web mercator to image space
    val webMercatorToImageTile: AffineTransform = new AffineTransform()
    webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, -resolution / tileMBRMercator.getHeight)
    webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMaxY)
    val dataToImage = ConcatenatedTransform.create(sourceToWebMercator,
      new AffineTransform2D(webMercatorToImageTile))
    val imageToData = dataToImage.inverse()

    // Get the extents of the input region that we want to process
    val sourceExtents = Array[Double](-buffer, -buffer, resolution + buffer, resolution + buffer)
    imageToData.transform(sourceExtents, 0, sourceExtents, 0, 2)
    val sourceExtentsEnvelope = GeometryReader.DefaultGeometryFactory.toGeometry(
      new Envelope(sourceExtents(0), sourceExtents(2), sourceExtents(1), sourceExtents(3))
    )
    val featuresOfInterest = features.rangeQuery(sourceExtentsEnvelope)
      // Clip records to the extents of the image
      .map(f => Feature.create(f, f.getGeometry.intersection(sourceExtentsEnvelope)))
      // Keep only features with non-empty geometries
      .filter(!_.getGeometry.isEmpty)
      // Convert to image space
      .map(f => Feature.create(f, JTS.transform(f.getGeometry, dataToImage)))

    // Aggregate all the features into a single tile
    val emptyTile = new IntermediateTile(resolution)
    val finalTile: VectorTile.Tile = featuresOfInterest.aggregate(emptyTile)(
      (tile, feature) => tile.addFeature(feature),
      (tile1, tile2) => tile1.merge(tile2)
    ).vectorTile

    finalTile
  }

  def plotMultipleTiles(features: SpatialDataTypes.SpatialRDD, resolution: Int,
                        buffer: Int = 0, opts: BeastOptions = new BeastOptions(),
                        outputFile: String, minLevel: Int = 0, maxLevel: Int = 3): Unit = {
    // Create Pyramid Partitioner
    // Partition the data into tiles
    // Aggregate with id
    // write to disc

    var featuresToPlot: SpatialRDD = features
    val sc = featuresToPlot.context
    featuresToPlot = VisualizationHelper.toWebMercator(featuresToPlot)

    // Create Pyramid Partitioner
    val fullPartitioner = new PyramidPartitioner(new SubPyramid(WebMercatorMBR, minLevel, maxLevel))

    // Partition the data into tiles
    var allImageTiles: Seq[RDD[(Long, VectorTile.Tile)]] = Seq()
    val tileWidth: Int = 256
    val tileHeight: Int = 256
    allImageTiles = allImageTiles :+ createImageTilesHybrid(featuresToPlot,
      tileWidth, tileHeight, resolution, fullPartitioner, opts)

    // Write all image tiles
    var tiles = sc.union(allImageTiles)

    tiles.foreach(t => {
      val tile = t._2
      val tileIndex = new TileIndex()
      TileIndex.decode(t._1, tileIndex)
      val output = new FileOutputStream(outputFile + s"New-${tileIndex.z}-${tileIndex.x}-${tileIndex.y}.mvt")
      tile.writeTo(output)
      output.close()
    })
  }

  private[davinci] def createImageTilesHybrid(features: SpatialRDD, tileWidth: Int,
                                              tileHeight: Int, resolution: Int, partitioner: PyramidPartitioner,
                                              opts: BeastOptions):RDD[(Long, VectorTile.Tile)]  = {
    val sparkConf = features.sparkContext.getConf
    val partitionerBroadcast = features.sparkContext.broadcast(partitioner)
    features.context.setJobDescription(s"Used hybrid partitioner $partitioner")

    // Assign each feature to all the overlapping tiles and group by tile ID
    val featuresAssignedToTiles: RDD[(Long, IFeature)] = features.flatMap(feature => {
      val mbr = new EnvelopeNDLite(2, -20037508.34, 20037508.34,
        20037508.34, -20037508.34)
      mbr.merge(feature.getGeometry)
      val par = partitionerBroadcast.value
      val matchedTiles = par.overlapPartitions(mbr)
        // assign it to all matching tiles
      matchedTiles.map(pid => (pid, feature))
    })

    // initialized empty intermediate tile
    val emptyTile = new IntermediateTile(resolution)

    val sourceSRID = features.first().getGeometry.getSRID

    // aggregate by key
    featuresAssignedToTiles.map({ case (tileId, feature) => (tileId, (tileId, feature)) })
      .aggregateByKey(emptyTile)(
        (intermediateTile, feature) => {

          val tileMBRMercator: Envelope = new Envelope
          val tileId: Long = feature._1
          TileIndex.getMBR(WebMercatorMBR, tileId, tileMBRMercator)

          // Convert to web mercator
          val sourceToWebMercator: MathTransform =
            Reprojector.findTransformationInfo(sourceSRID, 3857, sparkConf).mathTransform

          // Convert from web mercator to image space
          val webMercatorToImageTile: AffineTransform = new AffineTransform()
          webMercatorToImageTile.scale(resolution / tileMBRMercator.getWidth, -resolution / tileMBRMercator.getHeight)
          webMercatorToImageTile.translate(-tileMBRMercator.getMinX, -tileMBRMercator.getMaxY)
          val dataToImage = ConcatenatedTransform.create(sourceToWebMercator,
            new AffineTransform2D(webMercatorToImageTile))
          val imageToData = dataToImage.inverse()

          // covert to image space
          val f : IFeature = feature._2
          // plot
          intermediateTile.addFeature(Feature.create(f, JTS.transform(f.getGeometry, dataToImage)))

        },
        (intermediateTile1, intermediateTile2) => {
          intermediateTile1.merge(intermediateTile2)
        }
      ).map(cf => (cf._1.longValue(), cf._2.vectorTile))
  }
}
