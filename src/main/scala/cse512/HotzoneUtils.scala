package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // queryRectangle format  -74.189999,40.671001,-74.153071,40.707982
    // pointString format      -74.001580000000004,40.719382000000003
  	val coor_rect = queryRectangle.split(",")
	val point_coor = pointString.split(",")

	val p_x: Double = point_coor(0).trim.toDouble
	val p_y: Double = point_coor(1).trim.toDouble
	val r_x1: Double = coor_rect(0).trim.toDouble
	val r_y1: Double = coor_rect(1).trim.toDouble
	val r_x2: Double = coor_rect(2).trim.toDouble
	val r_y2: Double = coor_rect(3).trim.toDouble
   // we use the min and max value to prevent order in the definition of our rectangle
	if ((p_x >= math.min(r_x1,r_x2)) && (p_x <= math.max(r_x1,r_x2)) && (p_y >= math.min(r_y1,r_y2)) && (p_y <=math.max(r_y1,r_y2) )) {
		return true
	}
	return false
  }


}
