import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = args(0)
    // val outputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      //var validOperation = true

      val crimesDF = sparkSession.read.format("csv")

        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputFile)

      crimesDF.selectExpr("*","ST_CreatePoint(x,y) as geometry")
      val renamedDF = crimesDF.withColumnRenamed("ID","ID")
        .withColumnRenamed("Case Number", "CaseNumber")
        // .withColumnRenamed("Date", "Date")
        // .withColumnRenamed("Block", "Block")
        // .withColumnRenamed("IUCR", "IUCR")
        .withColumnRenamed("Primary Type", "PrimaryType")
        // .withColumnRenamed("Description", "Description")
        .withColumnRenamed("Location Description", "LocationDescription")
        // .withColumnRenamed("Arrest", "Arrest")
        .withColumnRenamed("Community Area", "CommunityArea")
        .withColumnRenamed("FBI Code", "FBICode")
        .withColumnRenamed("X Coordinate", "XCoordinate")
        .withColumnRenamed("Y Coordinate", "YCoordinate")
        .withColumnRenamed("Updated On", "UpdatedOn")
        .withColumnRenamed("X Coordinate", "ZIPCode")

      crimesDF.selectExpr("*","ST_CreatePoint(x,y) as geometry").toSpatialRDD
      crimesDF.show()
      crimesDF.printSchema()

      /*
      //val countiesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_county.zip")
      val zipCodesRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
      val zipCodesDFRDD: RDD[(IFeature, IFeature)] = tweetsRDD.spatialJoin(zipCodesRDD)
      val DFzip: DataFrame = zipCodesDFRDD.map({ case (data, zip) => Feature.append(data, zip.getAs[String]("ZCTA5CE10"), "ZipCode") })
        .toDataFrame(sparkSession)
      val DFzipGeometry=DFzip.drop("geometry")
      DFzipGeometry.printSchema()
      DFzipGeometry.show()
      //val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")

       */
    } finally {
      sparkSession.stop()
    }
  }
}