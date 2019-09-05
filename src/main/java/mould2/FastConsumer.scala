package mould2

import `val`.ConsumerVal
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
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
case class FastConsumer(master:String, zkHostPort:String, kafkaHostPort:String, kafkaTopic:String, groupId:String="10000")(implicit conf:Map[String,String]=Map[String,String](), otherKafkaConf:Map[String,String]=null){

  //使用窗口且不缓存中间结果的方案，K为从kafka获取的key,V为value，F为最终窗口接收的参数类型
  def planwithWindow[K,V,F](DSplan:DStream[(K,V)]=>DStream[F],windowFun:F=>Unit,planName:String)={
    val directStream=createDirectStream(ssc,kafkaParams,Set(kafkaTopic),groupId,planName)
    val acc=this.acc
    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(), rdd.partition(), rdd.offset())
        (rdd.key().asInstanceOf[K],rdd.value().asInstanceOf[V])
      }))
    ).cache().window(Duration(INTERVERL*WINDOWLENGTH_BEILV*1000),Duration(INTERVERL*WINDOWSLIDE_BEILV*1000))
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
  def planWithKafkaCache[K,V](DSplan:DStream[(K,V)]=>DStream[String],planName:String,KafkaProducerConfig:Map[String,Object],topicName:String)={
    val kafkaCli=spark.broadcast[KafkaSink[String,String]](KafkaSink(KafkaProducerConfig))
    val directStream=createDirectStream(ssc,kafkaParams,Set(kafkaTopic),groupId,planName)
    val acc=this.acc
    DSplan(
      directStream.mapPartitions(_.map(rdd=>{
        acc.add(rdd.topic(), rdd.partition(), rdd.offset())
        (rdd.key().asInstanceOf[K],rdd.value().asInstanceOf[V])
      }))
    ).foreachRDD(rdd=>{
      //沉潜批处理结果到kafka再保存
      rdd.foreach(value=>{kafkaCli.value.send(topicName,value);println("缓存成功")})
      if(!acc.isZero) {
        saveOffsets(acc.value.groupBy(a => (a._1, a._2)).mapValues(_.maxBy(_._3)).values.toArray, planName)
        acc.reset()
      }
    })
    this
  }
  //对外接口，启动sparkStreaming
  def start={
    ssc.start()
    ssc.awaitTermination()
  }
  private val sparkContext=SparkSession.builder().appName(this.getClass.getSimpleName)
    .master(master).getOrCreate().sparkContext
  private val INTERVERL=conf.getOrElse(ConsumerVal.INTERVERL,"2").toInt
  private val WINDOWLENGTH_BEILV=conf.getOrElse(ConsumerVal.WINDOWLENGTH_BEILV,"5").toInt
  private val WINDOWSLIDE_BEILV=conf.getOrElse(ConsumerVal.WINDOWSLIDE_BEILV,"5").toInt
  private val Globe_kafkaOffsetPath = conf.getOrElse(ConsumerVal.GLOBE_OFFSET_PATH,"/kafka/offsets")
  private var kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->kafkaHostPort,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> conf.getOrElse(ConsumerVal.AUTO_OFFSET_RESET_CONFIG,"latest"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )
  if(otherKafkaConf!=null) kafkaParams++=otherKafkaConf

  private val zkclient=CuratorFrameworkFactory.builder()
    .connectString(zkHostPort)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build()
  zkclient.start()

  private val spark=sparkContext
  private val acc=spark.collectionAccumulator[(String,Int,Long)]
  private val ssc=new StreamingContext(spark,Seconds(INTERVERL))

  //保存偏移量
  private def saveOffsets(offsetRange: Array[(String,Int,Long)],planName:String)={
    offsetRange.foreach(o=>{
      val zkPath = s"${Globe_kafkaOffsetPath}/${groupId}/${o._1}/${planName}/${o._2}"
      if (zkclient.checkExists().forPath(zkPath) == null)
        zkclient.create().creatingParentsIfNeeded().forPath(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o._1 +", planName:"+ planName +", Partition:" + o._2 + ", Offset:" + (o._3+1))
      zkclient.setData().forPath(zkPath,(o._3+1).toString.getBytes())
    })
    this
  }

  //读取偏移量
  private def readOffsets(planName:String)= {
    val zkTopicPath = Globe_kafkaOffsetPath+"/"+groupId+"/"+kafkaTopic+"/"+planName

    // 检查路径是否存在，不存在就创建
    if (zkclient.checkExists().forPath(zkTopicPath) == null)
      zkclient.create().creatingParentsIfNeeded().forPath(zkTopicPath)

    // 获取分区名（相当于获取kafka/offsets/$groupId/$topic目录下的所有目录名(也就是上面保存的o.partition)）
    val i = zkclient.getChildren().forPath(zkTopicPath).iterator()
    val offsets=mutable.Map[TopicPartition, Long]()
    while(i.hasNext) {
      val partitionId = i.next()
      val topicPartition = new TopicPartition(kafkaTopic, Integer.parseInt(partitionId))
      val offset = java.lang.Long.valueOf(new String(zkclient.getData().forPath(s"$zkTopicPath/${partitionId}"))).toLong
      offsets += ((topicPartition, offset))
    }
    offsets.toMap
  }

  //创建连接
  def createDirectStream(ssc:StreamingContext, kafkaParams:Map[String, Object], topic:Set[String], groupName:String,planName:String)={
    val offsets=readOffsets(planName)
    if(offsets.isEmpty)
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topic,kafkaParams))
    else
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topic,kafkaParams,offsets))
  }
}