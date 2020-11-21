package intervals

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class PostgresDataLoader(spark: SparkSession) {

  import spark.implicits._

  private lazy val exampleDF = Seq(
    "197.203.0.0, 197.206.9.255",
    "197.204.0.0, 197.204.0.24",
    "201.233.7.160, 201.233.7.168",
    "201.233.7.164, 201.233.7.168",
    "201.233.7.167, 201.233.7.167",
    "203.133.0.0, 203.133.255.255"
  ).map(_.split(", "))
    .map(r => (r.head, r.last))
    .toDF("start", "end")

  def load_data(): DataFrame = {
    import spark.sqlContext

    val postgresDF = Try(sqlContext.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://postgres:5432/")
      .option("dbtable", "IP_RANGES")
      .option("user", "postgres")
      .option("password", "1234")
      .load())

    postgresDF.getOrElse(exampleDF)
  }
}
