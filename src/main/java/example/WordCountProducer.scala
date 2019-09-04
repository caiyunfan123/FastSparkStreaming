package example

import java.util.{Properties, Random}

import util.KafkaSink

object WordCountProducer {
  val str = Array[String](
    "I am CYF",
    "what is my name",
    "who am I",
    "I choose Hadoop"
  )
  def main(args: Array[String]): Unit = {
    val rand = new Random(System.currentTimeMillis())
    val prop = new Properties()
    prop.put("bootstrap.servers","master:9092,slave:9092")
    prop.put("acks", "all")
    prop.put("retries", "0")
    prop.put("batch.size", "16384")
    prop.put("linger.ms", "1")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = KafkaSink[String,String](prop)
    while(true){
      kafkaProducer.send("wordCount",str(rand.nextInt(4)))
      println("--send--")
      Thread.sleep(1000)
    }
  }
}
