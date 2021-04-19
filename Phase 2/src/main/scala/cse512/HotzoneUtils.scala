package cse512

object HotzoneUtils {

    def ST_Contains(queryRectange: String, pointString: String): Boolean = {

        // Parse Point Query and Convert to Double data type
        val point_query = pointString.split(",").map(_.toDouble)

        // Store x and y coordinates of the point
        val point_x = point_query(0)
        val point_y = point_query(1)

        // Parse queryRectangle and Convert to Double
        val rectangle_query = queryRectange.split(",").map(_.toDouble)

        // Store x and y coordinates of rectangle
        val rectangle_x1 = Math.min(rectangle_query(0), rectangle_query(2))
        val rectangle_y1 = Math.min(rectangle_query(1),rectangle_query(3))
        val rectangle_x2 = Math.max(rectangle_query(0), rectangle_query(2))
        val rectangle_y2 = Math.max(rectangle_query(1),rectangle_query(3))

        // Check if point is within the rectangle
        return (point_x >= rectangle_x1 && point_x <= rectangle_x2 && point_y >= rectangle_y1 && point_y <= rectangle_y2)
    }
}
