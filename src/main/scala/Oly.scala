import org.apache.spark.{SparkConf, SparkContext}

object Oly {
  def getInt(value: String): Int = {
    try {
      return value.toInt
    } catch {
      case _: Exception => return 0
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OlyData").setMaster("local[1]");
    val sc = new SparkContext(conf)
    //    var rdd = sc.textFile("/Users/mahadev/Documents/sparkScala/datasets1/dummy.txt")
    //    val countryVsGoldCount = rdd.filter(line => {
    //      line.split(",")(14).contains("Gold")
    //    }).map(line => (line.split(",")(11), 1)).reduceByKey((a, v) => a + v)
    //    countryVsGoldCount.foreach(println(_))

    //    val indiaGoldCount = rdd.filter(line => {
    //      val splitLine = line.split(",")
    //      splitLine(14).contains("Gold") && splitLine(6).contains("India")
    //    }).count()
    //    println("India Gold Count : ", indiaGoldCount)
    //Find Athlete whose has height more than average

    //    val values = rdd.map(line => {
    //      val value = line.split(",")(4)
    //      getInt(value)
    //    })
    //
    //    val validValuesCount = values.filter(_ != 0).count()
    //    var averageHeight = values.fold(0)(_ + _) / validValuesCount
    //
    //    var athletsithHightGreaterThanAverage = rdd.filter(line => {
    //      val value = line.split(",")(4)
    //      val intValue = getInt(value)
    //      intValue > averageHeight
    //    }).map(line => line.split(",")(1))
    //    athletsithHightGreaterThanAverage.foreach(println(_))
    val rdd1 = sc.textFile("/Users/mahadev/Documents/sparkScala/Datasets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv")
    val filtered = rdd1.filter(line => line.contains("some"))

    val count = filtered.count();
  }
}