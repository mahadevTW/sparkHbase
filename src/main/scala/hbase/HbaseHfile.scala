package hbase

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext

object HbaseHfile {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "HbaseWrite")
    val conf = InsertDataHbase.getHbaseConf
    val tableName = "hao"
    val table = new HTable(conf, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(job, table)
    // Generate 10 sample data:
    val num = sc.parallelize(1 to 10)
    val rdd = num.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), "value_xxx".getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
    // Directly bulk load to Hbase/MapRDB tables.
    rdd.saveAsNewAPIHadoopFile("/tmp/xxxx19", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())
    println("********************* done with writing..");
  }
}
