package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def ST_Contains(queryRectange: String, pointString: String): Boolean = {

    // Parse Point Query and Convert to Double
    val point = pointString.split(",").map(_.toDouble)

    // Store x and y coordinates of point
    val point_x = point(0)
    val point_y = point(1)

    // Parse queryRectangle and Convert to Double
    val rectangle = queryRectange.split(",").map(_.toDouble)

    // Store x and y coordinates of rectangle
    val rect_x1 = Math.min(rectangle(0), rectangle(2))
    val rect_y1 = Math.min(rectangle(1),rectangle(3))
    val rect_x2 = Math.max(rectangle(0), rectangle(2))
    val rect_y2 = Math.max(rectangle(1),rectangle(3))

    // Check if point is within the rectangle
    return (point_x >= rect_x1 && point_x <= rect_x2 && point_y >= rect_y1 && point_y <= rect_y2)
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    // Parse point1 and Convert to Double
    val point1 = pointString1.split(",").map(_.toDouble)

    // Store x and y coordinates ofpoint1
    val p1_x = point1(0)
    val p1_y = point1(1)

    // Parse point2 and Convert to Double
    val point2 = pointString2.split(",").map(_.toDouble)

    // Store x and y coordinates of point2
    val p2_x = point2(0)
    val p2_y = point2(1)

    // ((p1_x - p2_x)^2 + (p1_y - p2_y)^2)^0.5
    val e_dist = Math.sqrt( Math.pow((p1_x-p2_x),2) + Math.pow((p1_y-p2_y),2) )

    // Return true or false if distance is greater than or less than euclidian distance
    return e_dist <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
