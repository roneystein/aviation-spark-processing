/**
  * Created by roney on 09/02/16.
  * Ranks the week day by average arrival delay in minutes
  */

/**
  * 2008-01-03 Thursday WN WN 588 HOU LIT 1325 1 1435 16.00 1.00
  *
  * Fields:
  * date (yyyy-MM-dd)
  * Weekday
  * Carrier ID
  * Carrier
  * Flight
  * Origin
  * Destination
  * Departure time
  * Departure delayed (true/false)
  * Arrival time
  * Arrival delay in minutes (>0)
  * Arrival delayed (true/false)
  */

import kafka.serializer.StringDecoder
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object RankWeekday {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RankWeekday")
    val kafkaTopics = Set("on-time")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://namenode:8020/checkpoint-rankweekday")
    val kafkaParams = Map("metadata.broker.list" -> "kafka1:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> "capstone",
      "zookeeper.connect" -> "kafka1:2181")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    // Just get the weekday, the arrival delayed flag and "1" to count the records
    val records = lines.map[(String, (Int, Int))](x => (x.split(" ")(1), (x.split(" ")(11).toFloat.toInt, 1)))
    val recordsSum = records.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    val mappingFunc = (word: String, one: Option[(Int, Int)], state: State[(Int, Int)]) => {
      val read = one.getOrElse((0,0))
      val saved = state.getOption.getOrElse((0,0))
      val sum = (read._1 + saved._1, read._2 + saved._2)
      val output = (word, sum)
      state.update(sum)
      output
    }

    recordsSum.mapWithState(StateSpec.function(mappingFunc)).
      stateSnapshots().map[(String, Float)]( x => (x._1, (x._2._1 / x._2._2.toFloat) * 100)).
      transform(rdd => rdd.sortBy(x => x._2)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
