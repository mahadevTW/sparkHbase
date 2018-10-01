package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.immutable

object HBaseAtomicity {


  val outputTable: String = "position"

  def getHBaseConfiguration(): Configuration = {
    val conf = HBaseConfiguration.create()
    System.setProperty("user.name", "hdfs")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    conf.set("hbase.master", "127.0.0.1:60000")
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "127.0.0.1")
    conf.setInt("hbase.zookeeper.property.clientPort", 2181)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    return conf
  }


  def writeToHbase(line: Row, tableName: String): Unit = {
    var conf: Configuration = getHBaseConfiguration()
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val table = new HTable(conf, tableName)
    val put: Put = new Put((line.get(0).toString + line.get(2).toString).getBytes)
    for (i <- 0 to line.length - 1) {
      put.addColumn("position".getBytes, line.schema.fieldNames(i).toString.getBytes, line.get(i).toString.getBytes)
    }
    table.put(put)
    table.flushCommits()
    table.close()

  }

  def writeListToHbase(list: List[IndexedSeq[(ImmutableBytesWritable, KeyValue)]]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(list)
  }

  def getRddByKey(rdd: RDD[Position], key: String) = {
    rdd.filter(x => x.AcctKey == key)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.executor.memory", "8g").appName("Test").master("local[1]").getOrCreate()
    import spark.implicits._
    val posnDf = spark.read.option("header", "true").schema(getPosnSchema()).csv("/Users/mahadev/work/city/ApacheSpark_Scala_Learning/Datasets/position.csv").as[Position]
    val partitionedPosn: Dataset[Position] = posnDf.repartition($"AcctKey")
    val rdd: RDD[Position] = partitionedPosn.rdd
    val groupAcctKey: RDD[(String, Iterable[Position])] = rdd.groupBy(line => line.AcctKey)
    val keys = groupAcctKey.keys.distinct.collect
    for (key <- keys) { // Get an RDD by filtering the original RDD by key
      val rddByKey: RDD[Position] = getRddByKey(rdd, key)
      rddByKey.collect()
      val rddToWrite: RDD[(ImmutableBytesWritable, KeyValue)] = rddByKey.flatMap(x => {
        val columnNames = List("account_number", "amount", "pp_code", "position").sorted
        val dataValues = List(x.AcctKey, x.Amount, x.PpCode, x.Posn)
        for (i <- 1 to 3) yield {
          val kv: KeyValue = new KeyValue((x.AcctKey.toString).getBytes, "position".getBytes, columnNames(i).getBytes, dataValues(i).getBytes)
          (new ImmutableBytesWritable(x.toString.getBytes), kv)
        }
      })
      rddToWrite.saveAsNewAPIHadoopFile("/Users/mahadev/work/city/ApacheSpark_Scala_Learning/CitiSpike/output", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], getHBaseConfiguration())
      val bulkLoader = new LoadIncrementalHFiles(getHBaseConfiguration())
      val table = new HTable(getHBaseConfiguration(), "position")
      println("Start Time is 2: " + System.currentTimeMillis())
      bulkLoader.doBulkLoad(new Path("/Users/mahadev/work/city/ApacheSpark_Scala_Learning/CitiSpike/output"), table)
      println("Finish Time is : " + System.currentTimeMillis())
    }
  }

  def getPosnSchema(): StructType = {
    StructType(
      Array(
        StructField("AcctKey", StringType),
        StructField("PpCode", StringType),
        StructField("Posn", StringType),
        StructField("Amount", StringType)
      )
    )

  }
}

case class Transaction(AcctKey: String, PpCode: String, TxnType: String, Amount: String)

case class Position(AcctKey: String, PpCode: String, Posn: String, Amount: String)




