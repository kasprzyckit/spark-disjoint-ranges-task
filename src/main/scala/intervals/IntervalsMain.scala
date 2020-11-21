package intervals

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}
import org.postgresql

object IntervalsMain extends App {
  val classes = Seq(
    getClass,
    classOf[postgresql.Driver]
  )
  val jars = classes.map(_.getProtectionDomain.getCodeSource.getLocation.getPath())
  val conf = new SparkConf().setJars(jars)
  val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .appName("DisjointIntervals")
    .getOrCreate()

  import spark.implicits._

  val dataDF = new PostgresDataLoader(spark).load_data()

  def ipStringToLong: String => Long = (ip: String) =>
    ip.split("\\.").zipWithIndex.foldLeft(0.longValue) { case (acc, (n, i)) => acc | (n.toLong << ((3 - i) * 8)) }

  val ipStringToLongUdf = udf(ipStringToLong)

  def longToIpString: Long => String = (ip: Long) =>
    ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF)

  val longToIpStringUDF = udf(longToIpString)

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
