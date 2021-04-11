package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def ST_Contains(queryRectange: String, pointString: String): Boolean = {

    // Parse the pontString by comma into an array of x and y coordinate of type double
    val pointDouble = pointString.split(",").map(_.toDouble)

    // Store x and y coordinates of point into separate variables
    val x_point = pointDouble(0)
    val y_point = pointDouble(1)

    // Parse queryRectangle by comma into an array of type double
    val rectangle = queryRectange.split(",").map(_.toDouble)

    // Find which point is the lower left (x1,y1) and which is the upper right (x2,y2) and store them
    val x1_rectangle = Math.min(rectangle(0), rectangle(2))
    val y1_rectangle = Math.min(rectangle(1),rectangle(3))
    val x2_rectangle = Math.max(rectangle(0), rectangle(2))
    val y2_rectangle = Math.max(rectangle(1),rectangle(3))

    // check if the rectangle contains the point.
    (x_point >= x1_rectangle) && (x_point <= x2_rectangle) && (y_point >= y1_rectangle) && (y_point <= y2_rectangle)
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    // Parse the pontString1 by comma into an array of x and y coordinate of type double
    val pointDouble1 = pointString1.split(",").map(_.toDouble)

    // Store x and y coordinates of pointDouble1 into separate variables
    val x_point1 = pointDouble1(0)
    val y_point1 = pointDouble1(1)

    // Parse the pontString1 by comma into an array of x and y coordinate of type double
    val pointDouble2 = pointString2.split(",").map(_.toDouble)

    // Store x and y coordinates of pointDouble2 into separate variables
    val x_point2 = pointDouble2(0)
    val y_point2 = pointDouble2(1)

    // Find the Euclidean distance between two points sqrt((x_point1 - x_point2)^2 + (y_point1 - y_point2)^2)
    val calculated_dist = Math.sqrt( Math.pow((x_point1-x_point2),2) + Math.pow((y_point1-y_point2),2) )

    // Check if the calculated distance between two points is within the queried distance
    calculated_dist <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
