package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  
  pickupInfo.createOrReplaceTempView("pickupInfo")  

  // YOU NEED TO CHANGE THIS PART
  val cells = spark.sql("select x,y,z,count(*) as num_rides from pickupInfo where x>=" + minX + " and x<=" + maxX + " and y>=" + minY + " and y<=" + maxY + " and z>=" +minZ+ " and z<=" + maxZ + " group by x,y,z").persist()
  cells.createOrReplaceTempView("cells")

  val stats = spark.sql("select sum(num_rides) as tot, sum(num_rides * num_rides) as sqr from cells").persist()
  val totalrides = stats.first().getLong(0).toDouble
  val sumofsqrs = stats.first().getLong(1).toDouble
  val mean = (totalrides/numCells)
  val std_deviation = Math.sqrt((sumofsqrs/numCells)-(mean*mean))

  val neighbors = spark.sql("select A.x as x, A.y as y, A.z as z, count(*) as numofneighbors, sum(B.num_rides) as sigma from cells as A inner join cells as B on ((abs(A.x - B.x) <= 1 and abs(A.y - B.y) <= 1 and abs(A.z - B.z) <= 1)) group by A.x,A.y,A.z ").persist()
  neighbors.createOrReplaceTempView("neighbors")
  spark.udf.register("CalcGOstat",(mean: Double, std_deviation: Double, numofneighbors: Int, sigma: Int, numCells: Int)=>((HotcellUtils.CalcGOstat(mean, std_deviation, numofneighbors, sigma, numCells))))

  val scores = spark.sql("select x,y,z,CalcGOstat(" + mean + "," + std_deviation + ",numofneighbors,sigma," + numCells+") as score from neighbors")
  scores.createOrReplaceTempView("scores")

  val hotcells = spark.sql("select x,y,z from scores order by score desc limit 50")
  return hotcells
  // return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
