package mould2

import java.util.concurrent.TimeUnit

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
case class FastConsumer(master:String, zkHostPort:String, kafkaHostPort:String, groupName:String="default", interval:Int=2)(implicit globe_kafkaOffsetPath:String="/kafka/offsets", otherKafkaConf:Map[String,String]=null) extends Serializable{

  /**
    * 使用窗口且不缓存中间结果的计划
    * @param consumerTopics 一个或多个主题
    * @param DSplan         数据批处理逻辑，传入数据为带有topic信息的rdd原始数据的DStream
    * @param planName       计划名
    * @param windowWithAutoSave  唯一会发生偏移量保存的窗口（你可以有多个窗口计划，但只有该窗口封装了保存偏移量的操作）
    * @param lengthBeilv    该窗口的长度倍率
    * @param slideBeilv     该窗口的滑动时间
    * @tparam F             该窗口接收的参数类型
    * @return               this.type
    */
  def planbyWindow[F](consumerTopics:String*)(DSplan:DStream[ConsumerRecord[Any,Any]]=>DStream[F], planName:String="default")(windowWithAutoSave:F=>Unit, lengthBeilv:Int=1, slideBeilv:Int=1):this.type={
    val directStream=createDirectStream(ssc,kafkaParams,consumerTopics.toSet,groupName,planName)
    //这个累加器，理论上应该是一个闭包一个才对
    val acc=sparkContext.collectionAccumulator[(String,String,Int,Long)]

    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(),planName,rdd.partition(),rdd.offset())
        rdd
      }))
    ).cache().window(Duration(interval*lengthBeilv*1000),Duration(interval*slideBeilv*1000))
      .foreachRDD(rdd=>{
        rdd.foreachPartition(_.foreach(value=>windowWithAutoSave(value)))
        saveOffsets(acc.value.groupBy(a => (a._1, a._2,a._3)).mapValues(_.maxBy(_._4)).values.toArray)
        acc.reset()
      })
    this
  }

  //参数：消费的主题、消费的计划和计划名、沉潜kafka的参数
  /**
    * 缓存中间结果到kafka的计划
    * @param consumerTopics
    * @param DSplan
    * @param planName
    * @param kafkaProducerConf 沉潜kafka的参数
    * @param sinkTopic         沉潜指定的主题
    * @param waitTime_Seconds  指定等待kafka返回的时长（秒），该时间大于0时会启动等待机制，超过等待时间则取消任务并将失败的中间结果保存到指定位置，默认值为0
    * @return
    */
  def planbyKafkaCache(consumerTopics:String*)(DSplan:DStream[ConsumerRecord[Any,Any]]=>DStream[String], planName:String="default")(kafkaProducerConf:Map[String,Object], sinkTopic:String, waitTime_Seconds:Int=0):this.type={
    val kafkaCli=sparkContext.broadcast[KafkaSink[String,String]](KafkaSink(kafkaProducerConf))
    val directStream=createDirectStream(ssc,kafkaParams,consumerTopics.toSet,groupName,planName)
    val acc=sparkContext.collectionAccumulator[(String,String,Int,Long)]

    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(),planName,rdd.partition(),rdd.offset())
        rdd
      }))
    ).foreachRDD(rdd=> {
      //沉潜批处理结果到kafka再保存
      rdd.foreachPartition(_.foreach(value => {
        val result = kafkaCli.value.send(sinkTopic, value)
        if (waitTime_Seconds > 0 && result.get(waitTime_Seconds, TimeUnit.SECONDS) == null) {
          if (result.cancel(true) || result.isCancelled)
            println("数据发送失败，内容为：" + value)
        }
        println("缓存完毕")
      }))
      saveOffsets(acc.value.groupBy(a => (a._1, a._2, a._3)).mapValues(_.maxBy(_._4)).values.toArray)
      acc.reset()
    })
//    //启动最终结果的消费者，执行最终业务
//    if(newInterval!=0)
//      FastConsumer(master,zkHostPort,kafkaHostPort,groupName)(newInterval,globe_kafkaOffsetPath,otherKafkaConf)
//        .foreach[String](sinkTopic)(planName)(lastWork).startCatch
    this
  }
//
  /**
    * 纯foreach操作，可供kafkaCatch缓存后再读入时使用
    * @param consumerTopics
    * @param planName
    * @param function
    * @tparam V   传入foreach方法的类型
    * @return
    */
  def foreach[V](consumerTopics:String*)(planName:String,function:V=>Unit):this.type ={
    val directStream=createDirectStream(ssc,kafkaParams,consumerTopics.toSet,groupName,planName)
    val acc=sparkContext.collectionAccumulator[(String,String,Int,Long)]

    directStream.foreachRDD(rdd=>{
      rdd.foreachPartition(_.foreach(a =>{
        function(a.value().asInstanceOf[V])
        acc.add(a.topic(),planName,a.partition(),a.offset())
      }))
      saveOffsets(acc.value.groupBy(a => (a._1, a._2,a._3)).mapValues(_.maxBy(_._4)).values.toArray)
      acc.reset()
    })
    this
  }

  //启动sparkStreaming
  def start={
    ssc.start()
    ssc.awaitTermination()
  }
  val sparkContext=SparkSession.builder().appName(this.getClass.getSimpleName)
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

  private val ssc=new StreamingContext(sparkContext,Seconds(interval))

  //保存偏移量
  private def saveOffsets(offsetRange: Array[(String,String,Int,Long)]):Unit={
    offsetRange.foreach(o=>{
      val zkPath = globe_kafkaOffsetPath+"/"+o._1+"/"+groupName+"/"+o._2+"/"+o._3
      if (zkclient.checkExists().forPath(zkPath) == null)
        zkclient.create().creatingParentsIfNeeded().forPath(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o._1 +", planName:"+ o._2 +", Partition:" + o._3 + ", Offset:" + (o._4+1))
      zkclient.setData().forPath(zkPath,(o._4+1).toString.getBytes())
    })
  }

//  private def saveOffsets(acc:CollectionAccumulator[(String,String,Int,Long)]):Unit={
//    val list = acc.value
//    val offsets = list.groupBy(a => (a._1, a._2)).mapValues(_.maxBy(_._3)).values.toArray
//    offsets.foreach(acc.add)
//    saveOffsets(offsets)
//  }

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