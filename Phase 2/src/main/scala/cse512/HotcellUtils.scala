package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
    val coordinateStep = 0.01

    def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
    {
        // Configuration variable:
        // Coordinate step is the size of each cell on x and y
        var result = 0
        coordinateOffset match
        {
            case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
            case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
            // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
            case 2 => {
                val timestamp = HotcellUtils.timestampParser(inputString)
                result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
            }
        }
        return result
    }

    def timestampParser (timestampString: String): Timestamp =
    {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val parsedDate = dateFormat.parse(timestampString)
        val timeStamp = new Timestamp(parsedDate.getTime)
        return timeStamp
    }

    def dayOfYear (timestamp: Timestamp): Int =
    {
        val calendar = Calendar.getInstance
        calendar.setTimeInMillis(timestamp.getTime)
        return calendar.get(Calendar.DAY_OF_YEAR)
    }

    def dayOfMonth (timestamp: Timestamp): Int =
    {
        val calendar = Calendar.getInstance
        calendar.setTimeInMillis(timestamp.getTime)
        return calendar.get(Calendar.DAY_OF_MONTH)
    }

    // YOU NEED TO CHANGE THIS PART
    /*Returns aggregate weight of each cell i wrt its adjacent cells j
      min_x - min value of X
      min_y - min value of Y
      min_z - min value of Z
      max_x - max value of X
      max_y - max value of Y
      max_z - max value of Z
      x - x coordinate
      y - y coordinate
      z - z coordinate */
    def computeAdjWeight( min_x: Int, min_y: Int, min_z: Int, max_x: Int, max_y: Int, max_z: Int, x: Int, y: Int, x: Int): Int = {
        var weight = 0

        // if cell is on X-boundary
        if (x == min_x || y == max_x) {
            weight = 1
        }
        // if cell is on X-boundary and Y-boundary
        if (y == min_y || y == max_y) {
            weight = 2
        }
        // if cell is on X-boundary, Y-boundary, and Z-boundary
        if (z == min_z || z == max_z) {
            weight = 3
        }

        if(weight == 1){
            return 18
        }
        else if(weight == 2){
            return 12
        }
        else if(weight == 3){
            return 8
        }
        else{
            return 27
        }
    }

    /*Returns Getis-Ord score
      num_cells - total number of cells in the cube
      x - x coordinate
      y - y coordinate
      z - z coordinate
      agg_weight - aggregate weight of adjacent hotcells
      cell_number - Hotcell number for given x and y coordinate
      mean - mean number of points per cell
      std_dev - standard deviation */
    def GScore(num_cells: Int, x: Int, y: Int, z: Int, agg_weight: Int, cell_number: Int , mean: Double, std_dev: Double): Double = {
        var num_of_cells: Double = num_cells.toDouble
        var w: Double = agg_weight.toDouble
        var h: Double = cell_number.toDouble
        (h - (mean * w)) / (std_dev * math.sqrt((( w * num_of_cells) - (w * w)) / (num_of_cells - 1.0)))
    }
}