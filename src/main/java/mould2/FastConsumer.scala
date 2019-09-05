package mould2

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import util.KafkaSink

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable


//三个坑点：1.闭包问题（如果对象不闭包，spark不通过）2.序列化问题（可以用自定义对象序列化）3.偏移量需要+1
case class FastConsumer(master:String, zkHostPort:String, kafkaHostPort:String, groupName:String="default")(implicit interverl:Int=2, globe_kafkaOffsetPath:String="/kafka/offsets",otherKafkaConf:Map[String,String]=null){

  //使用窗口且不缓存中间结果的方案，F为最终窗口接收的参数类型
  def planwithWindow[F](consumerTopics:String*)(DSplan:DStream[ConsumerRecord[Any,Any]]=>DStream[F],planName:String="default")(windowFun:F=>Unit,lengthBeilv:Int=1,slideBeilv:Int=1):this.type={
    val directStream=createDirectStream(ssc,kafkaParams,consumerTopics.toSet,groupName,planName)
    val acc=this.acc
    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(), rdd.partition(), rdd.offset())
        rdd
      }))
    ).cache().window(Duration(interverl*lengthBeilv*1000),Duration(interverl*slideBeilv*1000))
      .foreachRDD(rdd=>{
        rdd.foreachPartition(_.foreach(value=>windowFun(value)))
        if(!acc.isZero) {
          saveOffsets(acc.value.groupBy(a => (a._1, a._2)).mapValues(_.maxBy(_._3)).values.toArray, planName)
          acc.reset()
        }
      })
    this
  }

  //缓存中间结果到kafka的plan
  def planWithKafkaCache(consumerTopics:String*)(DSplan:DStream[ConsumerRecord[Any,Any]]=>DStream[String], planName:String="default")(kafkaProducerConf:Map[String,Object], sinkTopic:String):this.type={
    val kafkaCli=sparkContext.broadcast[KafkaSink[String,String]](KafkaSink(kafkaProducerConf))
    val directStream=createDirectStream(ssc,kafkaParams,consumerTopics.toSet,groupName,planName)
    val acc=this.acc
    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(), rdd.partition(), rdd.offset())
        rdd
      }))
    ).foreachRDD(rdd=>{
      //沉潜批处理结果到kafka再保存
      rdd.foreach(value=>{kafkaCli.value.send(sinkTopic,value);println("缓存成功")})
      if(!acc.isZero) {
        saveOffsets(acc.value.groupBy(a => (a._1, a._2)).mapValues(_.maxBy(_._3)).values.toArray, planName)
        acc.reset()
      }
    })
    this
  }

  //启动sparkStreaming
  def start={
    ssc.start()
    ssc.awaitTermination()
  }
  private val sparkContext=SparkSession.builder().appName(this.getClass.getSimpleName)
    .master(master).getOrCreate().sparkContext
  private var kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->kafkaHostPort,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> groupName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )
  if(otherKafkaConf!=null) kafkaParams++=otherKafkaConf

  private val zkclient=CuratorFrameworkFactory.builder()
    .connectString(zkHostPort)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build()
  zkclient.start()

  private val acc=sparkContext.collectionAccumulator[(String,Int,Long)]
  private val ssc=new StreamingContext(sparkContext,Seconds(interverl))

  //保存偏移量
  private def saveOffsets(offsetRange: Array[(String,Int,Long)],planName:String)={
    offsetRange.foreach(o=>{
      val zkPath = globe_kafkaOffsetPath+"/"+o._1+"/"+groupName+"/"+planName+"/"+o._2
      if (zkclient.checkExists().forPath(zkPath) == null)
        zkclient.create().creatingParentsIfNeeded().forPath(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o._1 +", planName:"+ planName +", Partition:" + o._2 + ", Offset:" + (o._3+1))
      zkclient.setData().forPath(zkPath,(o._3+1).toString.getBytes())
    })
    this
  }

  //读取偏移量
  private def readOffsets(planName:String,consumerTopics:Set[String])= {
    val offsets=mutable.Map[TopicPartition, Long]()
    consumerTopics.foreach(kafkaTopic=>{
      val zkTopicPath = globe_kafkaOffsetPath+"/"+kafkaTopic+"/"+groupName+"/"+planName
      // 检查路径是否存在，不存在就创建
      if (zkclient.checkExists().forPath(zkTopicPath) == null)
        zkclient.create().creatingParentsIfNeeded().forPath(zkTopicPath)

      // 获取分区名（相当于获取kafka/offsets/$groupId/$topic目录下的所有目录名(也就是上面保存的o.partition)）
      val i = zkclient.getChildren().forPath(zkTopicPath).iterator()
      while(i.hasNext) {
        val partitionId = i.next()
        val topicPartition = new TopicPartition(kafkaTopic, Integer.parseInt(partitionId))
        val offset = java.lang.Long.valueOf(new String(zkclient.getData().forPath(zkTopicPath+"/"+partitionId))).toLong
        offsets += ((topicPartition, offset))
      }
    })
    offsets.toMap
  }

  //创建连接
  private def createDirectStream(ssc:StreamingContext, kafkaParams:Map[String, Object], topics:Set[String], groupName:String, planName:String)={
    val offsets=readOffsets(planName,topics)
    if(offsets.isEmpty)
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topics,kafkaParams))
    else
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topics,kafkaParams,offsets))
  }
}