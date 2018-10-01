import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Demo").setMaster("local[1]"))
    val textFile = sc.textFile("/Users/mahadev/Documents/SampleDemo/data/demo.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => {
        println(word)
        (word, 1)
      })
      .reduceByKey(_ + _)
    counts.saveAsTextFile("/Users/mahadev/Documents/SampleDemo/data/out")
  }
}
