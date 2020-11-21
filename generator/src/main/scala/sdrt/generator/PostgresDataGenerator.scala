package sdrt.generator

import java.util.concurrent.TimeUnit

import slick.jdbc.PostgresProfile.api._

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

object PostgresDataGenerator {

  def ipStringToLong: String => Long = (ip: String) =>
    ip.split("\\.").zipWithIndex.foldLeft(0.longValue) { case (acc, (n, i)) => acc | (n.toLong << ((3 - i) * 8)) }

  def longToIpString: Long => String = (ip: Long) =>
    ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF)

  case class IpRange(start: String, end: String)

  class IpRangesTable(tag: Tag) extends Table[(String, String)](tag, "ip_ranges") {
    def start = column[String]("start")

    def end = column[String]("end")

    def * = (start, end)
  }

  val ipRanges = TableQuery[IpRangesTable]
  val maxIpLong: Long = ipStringToLong("255.255.255.255")

  def generateData(count: Int): Unit = {
    val data = (1 to count)
      .map(_ => Random.nextInt(100000))
      .map(i => {
        val st = Random.nextLong() % (maxIpLong - i)
        (longToIpString(st), longToIpString(st + i))
      })

    val db = Database.forURL("jdbc:postgresql://postgres:5432/?user=postgres&password=1234")

    implicit val ex: ExecutionContext = global
    val future = for {
      _ <- db.run(ipRanges.schema.createIfNotExists.transactionally)
      count <- db.run((ipRanges ++= data).transactionally)
    } yield count
    Await.result(future, Duration(100, TimeUnit.SECONDS))
      .foreach(i => println(s"Inserted $i rows"))

    db.close()
  }
}
