package sdrt.generator

import scala.util.Try

object GeneratorMain extends App {

  sys.env.get("IP_RANGES_COUNT")
    .flatMap(n => Try(n.toInt).toOption)
    .foreach(n => {
      println(s"Generating $n rows of data.")
      PostgresDataGenerator.generateData(n)
    })
}
