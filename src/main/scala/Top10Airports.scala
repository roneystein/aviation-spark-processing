/**
  * Created by roney on 09/02/16.
  */

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Top10Airports {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Top10Airports")
    val kafkaTopics = Set("origin-destination")
    val numThreads = 1
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "largest")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics).
      map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1)).reduceByKey(_ + _)

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc).timeout(Minutes(60)))
    val stateSnapshotStream = stateDstream.stateSnapshots()

    //stateDstream.transform(rdd => rdd.sortBy(x => -x._2)).print()
    //println("snapshot:")
    stateSnapshotStream.transform(rdd => rdd.sortBy(x => -x._2)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
