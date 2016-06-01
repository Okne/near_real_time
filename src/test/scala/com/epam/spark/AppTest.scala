package com.epam.spark


object AppTest {

  def main(args: Array[String]): Unit = {
    val obj = App.UNKNOWN_CITY_OBJ
    println(ccToMap(obj))
  }

  def ccToMap(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
  }
}
