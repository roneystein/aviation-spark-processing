/**
  * Created by roney on 09/02/16.
  */

/**
  * 2008-01-03 Thursday WN WN 588 HOU LIT 1325 1 1435 16.00 1.00
  *
  * Fields:
  * 0  date (yyyy-MM-dd)
  * 1  Weekday
  * 2  Carrier ID
  * 3  Carrier
  * 4  Flight
  * 5  Origin
  * 6  Destination
  * 7  Departure time
  * 8  Departure delayed (true/false)
  * 9  Arrival time
  * 10 Arrival delay in minutes (>0)
  * 11 Arrival delayed (true/false)
  */

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.streaming._

object eachTop10Air {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("eachTop10Air").
      set("spark.cassandra.connection.host", "127.0.0.1")

    val kafkaTopics = Set("on-time")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint-eachTop10Air")
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "largest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    // Get the origin, destination and departure delayed flag
    val records = lines.map[((String, String), (Int, Int))](
      x => ((x.split(" ")(5), x.split(" ")(6)), (x.split(" ")(11).toFloat.toInt, 1)))
    val recordsSum = records.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    val mappingFunc = (word: (String, String), one: Option[(Int, Int)], state: State[(Int, Int)]) => {
      val read = one.getOrElse((0,0))
      val saved = state.getOption.getOrElse((0,0))
      val sum = (read._1 + saved._1, read._2 + saved._2)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = recordsSum.mapWithState(
      StateSpec.function(mappingFunc))

    // Rank per origin : ERRADO: ordenar assim não vai dar o resultado.
    // objetivo: origem-destino: top 10 destinos
    // ordenar sem mudar a chave, top 10 com (origen, destino).
    // se possivel jogar no banco somente esse top10, fazendo a posicao a primary key
    // caso contrario armazenamos tudo e fazemos o top 10 na busca

    // só tem utilidade para ver o print
    stateDstream.stateSnapshots().
      map[((String, String), Float)]( x => (x._1, (x._2._1 / x._2._2.toFloat) * 100)).
      transform(rdd => rdd.sortBy(x => x._2)).print(100)

    // para armazenar no DB não vamos usar o snapshot
    stateDstream.map[((String, String), Float)]( x => (x._1, (x._2._1 / x._2._2.toFloat) * 100)).
      transform(rdd => rdd.sortBy(x => x._2)).
      saveToCassandra()

    ssc.start()
    ssc.awaitTermination()
  }
}
