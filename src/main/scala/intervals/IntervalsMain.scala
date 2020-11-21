package intervals

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}

object IntervalsMain extends App {

  def ipStringToLong: String => Long = (ip: String) =>
    ip.split("\\.").zipWithIndex.foldLeft(0.longValue) { case (acc, (n, i)) => acc | (n.toLong << ((3 - i) * 8)) }

  val ipStringToLongUdf = udf(ipStringToLong)

  def longToIpString: Long => String = (ip: Long) =>
    ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF)

  val longToIpStringUDF = udf(longToIpString)

  val spark: SparkSession = SparkSession.builder()
    .appName("DisjointIntervals")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    "197.203.0.0, 197.206.9.255",
    "197.204.0.0, 197.204.0.24",
    "201.233.7.160, 201.233.7.168",
    "201.233.7.164, 201.233.7.168",
    "201.233.7.167, 201.233.7.167",
    "203.133.0.0, 203.133.255.255"
  ).map(_.split(", ")).map(r => (r.head, r.last))

  val dataDF = data.toDF("start", "end")

  val rangeUdf = udf((start: Long, end: Long) => start to end)
  val ipCountsDF = dataDF
    .select(ipStringToLongUdf('start).as("start"), ipStringToLongUdf('end).as("end"))
    .select(explode(rangeUdf('start, 'end)).as("ip"))
    .groupBy('ip)
    .agg(count("*").as("count"))
    .filter('count === 1)

  val window = Window.orderBy('ip)
  val disjointRangesDF = ipCountsDF
    .withColumn("prev", lag('ip, 1).over(window))
    .withColumn("is_consecutive", coalesce('ip === 'prev + 1, lit(false)))
    .withColumn("group_nb", sum(when('is_consecutive, lit(0)).otherwise(lit(1))).over(window))
    .groupBy('group_nb).agg(min('ip).as("start"), max('ip).as("end"))
    .select(longToIpStringUDF('start).as("start"), longToIpStringUDF('end).as("end"))

  disjointRangesDF.show()
}
