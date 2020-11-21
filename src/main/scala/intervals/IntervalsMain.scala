package intervals

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object IntervalsMain extends App {

//  def intersections(inters: List[(Long, Long)]): Array[(Long, Long)] = {
//    def intersectionsRev(intervals: List[(Long, Long)], c: (Long, Long)): List[(Long, Long)] = intervals match {
//      case Nil => Nil
//      case (x@(a, _)) :: tail if a >= c._2 => intersectionsRev(tail, x)
//      case (_, b) :: tail if b <= c._1 => intersectionsRev(tail, c)
//      case (a, b) :: tail =>
//        val a2 = if (a <= c._1) c._1 else a
//        val (b1, b2) = if (b <= c._2) (b, c._2) else (c._2, b)
//        (a2, b1) :: intersectionsRev(tail, (b1, b2))
//    }
//
//    if (inters.nonEmpty) intersectionsRev(inters.tail, inters.head).toArray else Array.empty
//  }
//
//  def binarySearch(list: Array[(Long, Long)], el: Long): Int = {
//    @tailrec
//    def search(start: Int, end: Int): Int = {
//      val pos = ((end - start) / 2) + start
//      if ((end - start) <= 1) pos
//      else if (el >= list(pos)._2) search(pos, end)
//      else search(start, pos)
//    }
//
//    search(0, list.length)
//  }
//
//  def disjointIntervals(inters: Array[(Long, Long)], c: (Long, Long)): List[(Long, Long)] = {
//    val ind = binarySearch(inters, c._1)
//    val ints = (ind until inters.length).view.map(inters).takeWhile(_._1 < c._2).filter(_._2 > c._1)
//    if (ints.isEmpty) List(c) else {
//      val first = ints.head._1
//      val last = ints.last._2
//      val intsScan = ints.scanLeft(((0.longValue, 0.longValue), c._1))((a, b) => ((a._2, b._1), b._2)).drop(2).map(_._1).toList
//      if (c._1 < first && c._2 > last) ((c._1, first) :: intsScan) :+ (last, c._2)
//      else if (c._1 < first) (c._1, first) :: intsScan
//      else if (c._2 > last) intsScan :+ (last, c._2)
//      else intsScan
//    }
//  }
//
//  def ipStringToLong: String => Long = (ip: String) =>
//    ip.split("\\.").zipWithIndex.foldLeft(0.longValue) { case (acc, (n, i)) => acc | (n.toLong << ((3 - i) * 8)) }
//
//  val ipStringToLongUdf = udf(ipStringToLong)
//
//  def longToIpString: Long => String = (ip: Long) =>
//    ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF)
//
//  val longToIpStringUDF = udf(longToIpString)
//
//  val spark: SparkSession = SparkSession.builder()
//    .appName("DisjointIntervals")
//    .getOrCreate()
//
//  import spark.implicits._
//
////  val data = Seq(
////    "197.203.0.0, 197.206.9.255",
////    "197.204.0.0, 197.204.0.24",
////    "201.233.7.160, 201.233.7.168",
////    "201.233.7.164, 201.233.7.168",
////    "201.233.7.167, 201.233.7.167",
////    "203.133.0.0, 203.133.255.255"
////  ).map(_.split(", ")).map(r => (r.head, r.last))
////
////  val dataDF = data.toDF("start", "end")
////  val intsDF = dataDF
////    .withColumn("start", ipStringToLongUdf('start))
////    .withColumn("end", ipStringToLongUdf('end) + 1)
//////  val inters = intersections(intsDF.collect().map(r => (r.getAs[Long](0), r.getAs[Long](1))).toList)
//////  val disjointIntsDF = intsDF.flatMap(r => disjointIntervals(inters, (r.getAs[Long](0), r.getAs[Long](1))))
////  val disjointDF = intsDF
////    .withColumn("start", longToIpStringUDF('start))
////    .withColumn("end", longToIpStringUDF('end) - 1)
////
//  import java.time.LocalDate
//  import java.time.format.DateTimeFormatter
//  def enumerateDays(start: LocalDate, end: LocalDate) = {
//    Iterator.iterate(start)(d => d.plusDays(1L))
//      .takeWhile(d => !d.isAfter(end))
//      .toSeq
//  }
//  val udf_enumerateDays = udf( (start:String, end:String) => enumerateDays(LocalDate.parse(start), LocalDate.parse(end)).map(_.toString))
//  val df = Seq(
//    ("Mike","2018-09-01","2018-09-10"), // range 1
//    ("Mike","2018-09-05","2018-09-05"), // range 1
//    ("Mike","2018-09-12","2018-09-12"), // range 1
//    ("Mike","2018-09-11","2018-09-11"), // range 1
//    ("Mike","2018-09-25","2018-09-29"), // range 4
//    ("Mike","2018-09-21","2018-09-23"), // range 4
//    ("Mike","2018-09-24","2018-09-24"), // range 4
//    ("Mike","2018-09-14","2018-09-16"), // range 2
//    ("Mike","2018-09-15","2018-09-17"), // range 2
//    ("Mike","2018-09-05","2018-09-05"), // range 1
//    ("Mike","2018-09-19","2018-09-19"), // range 3
//    ("Mike","2018-09-19","2018-09-19"), // range 3
//    ("Mike","2018-08-19","2018-08-20"), // range 5
//    ("Mike","2018-10-01","2018-10-20"), // range 6
//    ("Mike","2018-10-10","2018-10-30")  // range 6
//  ).toDF("name", "start", "end")
//
//  df.select($"name", explode(udf_enumerateDays($"start",$"end")).as("day"))
//    .distinct
//    .withColumn("day_prev", lag($"day",1).over(Window.partitionBy($"name").orderBy($"day")))
//    .withColumn("is_consecutive", coalesce(datediff($"day",$"day_prev"),lit(0))<=1)
//    .withColumn("group_nb", sum(when($"is_consecutive",lit(0)).otherwise(lit(1))).over(Window.partitionBy($"name").orderBy($"day")))
//    .groupBy($"name",$"group_nb").agg(min($"day").as("start"), max($"day").as("end"))
//    .drop($"group_nb")
//    .orderBy($"name",$"start")
//    .show

  val spark: SparkSession = SparkSession.builder()
//    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  df.show()

  //Group By on single column
  df.groupBy("department").count().show(false)
  df.groupBy("department").avg("salary").show(false)
  df.groupBy("department").sum("salary").show(false)
  df.groupBy("department").min("salary").show(false)
  df.groupBy("department").max("salary").show(false)
  df.groupBy("department").mean("salary").show(false)

  //GroupBy on multiple columns
  df.groupBy("department","state")
    .sum("salary","bonus")
    .show(false)
  df.groupBy("department","state")
    .avg("salary","bonus")
    .show(false)
  df.groupBy("department","state")
    .max("salary","bonus")
    .show(false)
  df.groupBy("department","state")
    .min("salary","bonus")
    .show(false)
  df.groupBy("department","state")
    .mean("salary","bonus")
    .show(false)

  //Running Filter
  df.groupBy("department","state")
    .sum("salary","bonus")
    .show(false)

  //using agg function
  df.groupBy("department")
    .agg(
      sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary"),
      sum("bonus").as("sum_bonus"),
      max("bonus").as("max_bonus"))
    .show(false)

  df.groupBy("department")
    .agg(
      sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary"),
      sum("bonus").as("sum_bonus"),
      stddev("bonus").as("stddev_bonus"))
    .where(col("sum_bonus") > 50000)
    .show(false)
}
