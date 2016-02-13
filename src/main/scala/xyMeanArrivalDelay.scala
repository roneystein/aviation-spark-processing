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
  *
  * For each source-destination pair X-Y, calculate the mean arrival delay in minutes
  */

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object xyMeanArrivalDelay {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("xyMeanArrivalDelay").
      set("spark.cassandra.connection.host", "127.0.0.1")

    val kafkaTopics = Set("on-time")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint-xyMeanArrivalDelay")
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "largest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    // Get the origin, destination, departure delayed flag and "1"
    val records = lines.map[((String, String), (Int, Int))](
      x => ((x.split(" ")(5), x.split(" ")(6)), (x.split(" ")(10).toFloat.toInt, 1)))
    val recordsSum = records.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    // Stores the sum
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

    stateDstream.stateSnapshots().
      map[(String, String, Float)]( x => (x._1._1, x._1._2, x._2._1 / x._2._2.toFloat )).
      saveToCassandra("capstone2", "xymeandelay", SomeColumns(
        "origin", "destination", "mean_delay")
      )

    ssc.start()
    ssc.awaitTermination()
  }
}

