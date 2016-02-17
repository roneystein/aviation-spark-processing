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
  * Group 3 question 2
  */

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object xyz {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setAppName("xyz").
      set("spark.cassandra.connection.host", "datanode1").
      set("spark.cassandra.connection.keep_alive_ms", "12000")

    val kafkaTopics = Set("on-time")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://namenode:8020/checkpoint-xyz")
    val kafkaParams = Map("metadata.broker.list" -> "kafka1:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> "capstone",
      "zookeeper.connect" -> "kafka1:2181")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    // (flight date, origin, destination, AM/PM, flight, departure time, arrival delay in minutes)
    val records = lines.map[((DateTime, String, String, String), (String, String, Int))](
      x => ((DateTime.parse(x.split(" ")(0), DateTimeFormat.forPattern("yyyy-MM-dd")),
        x.split(" ")(5),
        x.split(" ")(6),
        if (x.split(" ")(7).toInt < 1200) "AM" else "PM"
        ), (
        x.split(" ")(3) + " " + x.split(" ")(4),
        x.split(" ")(7),
        x.split(" ")(10).toFloat.toInt ) ) )

    // Reduces and gets only the least delayed flight
    val flights = records.reduceByKey( (x, y) => if (x._3 > y._3) y else x )

    // Stores the flights but keep only the least delayed
    val mappingFunc = (word: (DateTime, String, String, String), one: Option[(String, String, Int)], state: State[(String, String, Int)]) => {
        val read = one.getOrElse(("", "", Int.MaxValue))
        val saved = state.getOption.getOrElse(("", "", Int.MaxValue))
        val best = if (read._3 > saved._3) saved else read
        state.update(best)
        (word._1.toString("dd/MM/yyyy"), word._4, word._2, word._3, best._1, best._2, best._3)
    }

    val flightsDstream = flights.mapWithState(StateSpec.function(mappingFunc))
    flightsDstream.saveToCassandra("capstone2", "xyz", SomeColumns(
          "departure_date", "period", "origin", "destination", "flight", "departure_time", "delay" ))

    ssc.start()
    ssc.awaitTermination()
  }
}

