package example

import `val`.KafkaProducerVal
import mould2.FastConsumer

object wordCountConsumer2 {
  def main(args: Array[String]): Unit = {
    FastConsumer("local[*]",
      "master:2181,slave:2181",
      "master:9092,,slave:9092",
      "wordCount")
      .planwithWindow[String,String,(String,Int)](
      ds=>ds.mapPartitions(_.map(_._2->1)).reduceByKey(_+_),
      windowValue=>println(windowValue),
      "windowTest"
    )
      .planWithKafkaCache[String,String](
      ds=>ds.mapPartitions(_.map(_._2->1)).reduceByKey(_+_).map(_.toString()),
      "cacheTest",
      Map[String,Object](
        KafkaProducerVal.BOOTSTRAP_SERVERS->"master:9092,slave:9092",
        KafkaProducerVal.ACKS->"all",
        KafkaProducerVal.RETRIES->"0",
        KafkaProducerVal.BATCH_SIZE->"16384",
        KafkaProducerVal.LINGER_MS->"1",
        KafkaProducerVal.KEY_SERIALIZER->"org.apache.kafka.common.serialization.StringSerializer",
        KafkaProducerVal.VALUE_SERIALIZER->"org.apache.kafka.common.serialization.StringSerializer"
      ),
      "mound2Test"
    )
      .start
  }
}
