package sdrt.intervals

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}

import scala.util.Try

class ElasticsearchDataSaver {

  def saveResult(ranges: Seq[(String, String)]): Unit = {
    Try(ElasticClient(JavaClient(ElasticProperties("http://elasticsearch:9200")))).foreach { client =>
      client.execute {
        createIndex("dijoint").mapping(
          properties(
            textField("start"),
            textField("end")
          )
        )
      }.await
      val inserts = ranges.map { case (start, end) =>
        indexInto("disjoint").source(s"""{"start": "$start", "end": "$end"}""")
      }
      client.execute {
        bulk(inserts: _*).refresh(RefreshPolicy.WaitFor)
      }.await
      client.close()
    }
  }
}
