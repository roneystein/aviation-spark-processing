/**
  * Created by roney on 09/02/16.
  *
  * For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance
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
  */

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object xyTop10Carriers {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("xyTop10Carriers").
      set("spark.cassandra.connection.host", "datanode1").
      set("spark.cassandra.connection.keep_alive_ms", "12000")

    val kafkaTopics = Set("on-time")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://namenode:8020/checkpoint-xyTop10Carriers")
    val kafkaParams = Map("metadata.broker.list" -> "kafka1:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> "capstone",
      "zookeeper.connect" -> "kafka1:2181")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    // Get the origin, destination, arrival delayed flag and "1"
    val records = lines.map[((String, String, String), (Int, Int))](
      x => ((x.split(" ")(5), x.split(" ")(6), x.split(" ")(3)), (x.split(" ")(11).toFloat.toInt, 1)))
    // Reduce by key summing delayed flights and count
    val recordsSum = records.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

    // Stores the sum of delayed flights and total per origin-destination-carrier
    val mappingFunc = (word: (String, String, String), one: Option[(Int, Int)], state: State[(Int, Int)]) => {
      val read = one.getOrElse((0,0))
      val saved = state.getOption.getOrElse((0,0))
      val sum = (read._1 + saved._1, read._2 + saved._2)
      val output = (word, sum)
      state.update(sum)
      output
    }
    val stateDstream = recordsSum.mapWithState(
      StateSpec.function(mappingFunc))

    // Gets the entire state, calculates the percentage of delayed flights
    // sorts, rank the top 10 and save to Cassandra
    stateDstream.stateSnapshots().
      map[((String, String), (String, Int))]( x => ((x._1._1, x._1._2), (x._1._3, ((x._2._1 / x._2._2.toFloat)*100).toInt ))).
      groupByKey().flatMapValues( x => {
      val top10 = x.toList.sortBy(_._2).take(10)
      val top10list = (1 to top10.size).toList
      top10 zip top10list
      }).map[(String, String, String, Int, Int)](x => (x._1._1, x._1._2, x._2._1._1, x._2._1._2, x._2._2)).
      saveToCassandra("capstone2", "xytop10carriers", SomeColumns(
          "origin", "destination", "carrier", "percentage_delayed", "rank"))

    ssc.start()
    ssc.awaitTermination()
  }
}

