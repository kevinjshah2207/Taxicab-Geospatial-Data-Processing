package cse512

object HotzoneUtils {

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
}
