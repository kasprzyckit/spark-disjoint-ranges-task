package intervals

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}

import scala.annotation.tailrec

object IntervalsMain extends App {

  def intersections(inters: List[(Long, Long)]): Array[(Long, Long)] = {
    def intersectionsRev(intervals: List[(Long, Long)], c: (Long, Long)): List[(Long, Long)] = intervals match {
      case Nil => Nil
      case (x@(a, _)) :: tail if a >= c._2 => intersectionsRev(tail, x)
      case (_, b) :: tail if b <= c._1 => intersectionsRev(tail, c)
      case (a, b) :: tail =>
        val a2 = if (a <= c._1) c._1 else a
        val (b1, b2) = if (b <= c._2) (b, c._2) else (c._2, b)
        (a2, b1) :: intersectionsRev(tail, (b1, b2))
    }

    if (inters.nonEmpty) intersectionsRev(inters.tail, inters.head).toArray else Array.empty
  }

  def binarySearch(list: Array[(Long, Long)], el: Long): Int = {
    @tailrec
    def search(start: Int, end: Int): Int = {
      val pos = ((end - start) / 2) + start
      if ((end - start) <= 1) pos
      else if (el >= list(pos)._2) search(pos, end)
      else search(start, pos)
    }

    search(0, list.length)
  }

  def disjointIntervals(inters: Array[(Long, Long)])(c: (Long, Long)): List[(Long, Long)] = {
    val ind = binarySearch(inters, c._1)
    val ints = (ind until inters.length).view.map(inters).takeWhile(_._1 < c._2).filter(_._2 > c._1)
    if (ints.isEmpty) List(c) else {
      val first = ints.head._1
      val last = ints.last._2
      val intsScan = ints.scanLeft(((0.longValue, 0.longValue), c._1))((a, b) => ((a._2, b._1), b._2)).drop(2).map(_._1).toList
      if (c._1 < first && c._2 > last) ((c._1, first) :: intsScan) :+ (last, c._2)
      else if (c._1 < first) (c._1, first) :: intsScan
      else if (c._2 > last) intsScan :+ (last, c._2)
      else intsScan
    }
  }

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

    val intsDF = dataDF
      .withColumn("start", ipStringToLongUdf('start))
      .withColumn("end", ipStringToLongUdf('end) + 1)
    intsDF.show()
    val inters = intersections(intsDF.collect().map(r => (r.getAs[Long](0), r.getAs[Long](1))).toList)
    val disjointIntervalsUdf = udf((start: Long, end: Long) => (disjointIntervals(inters)(start, end)).toSeq.map(_.toString))
      val disjointIntsDF = intsDF.flatMap(r => disjointIntervals(inters)(r.getAs[Long](0), r.getAs[Long](1)))
    //  df.select($"name", explode(udf_enumerateDays($"start",$"end")).as("day"))
//    val disjointIntsDF = intsDF.select(explode(disjointIntervalsUdf($"start",$"end")).as("ip"))
//    disjointIntsDF.show()
    val disjointDF = disjointIntsDF
      .withColumn("start", longToIpStringUDF('start))
      .withColumn("end", longToIpStringUDF('end) - 1)
    disjointDF.show()
}
