package com.example.gradle.spark

import org.apache.spark.sql.SparkSession

case class Customer(id: Int, name: String, age: Int, gender: String)

object SparkExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._

    val customers = List(
      Customer(1, "James", 21, "M"),
      Customer(2, "Liz", 25, "F"),
      Customer(3, "John", 31, "M"),
      Customer(4, "Jennifer", 45, "F"),
      Customer(5, "Robert", 41, "M"),
      Customer(6, "Sandra", 45, "F")
    )

    val dataFrame = sparkSession.sparkContext.parallelize(customers).toDF()
    dataFrame.show()
  }
}
