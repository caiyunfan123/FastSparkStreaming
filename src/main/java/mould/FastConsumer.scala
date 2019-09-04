package mould

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable


//三个坑点：1.闭包问题（如果对象不闭包，spark不通过）2.序列化问题（可以用自定义对象序列化）3.偏移量需要+1
case class FastConsumer(master:String, zkHostPort:String, kafkaHostPort:String, kafkaTopic:String, groupId:String="10000", conf:Map[String,String]=Map[String,String](), otherKafkaConf:Map[String,String]=null) extends Serializable {
  private val sparkContext=SparkSession.builder().appName(this.getClass.getSimpleName)
    .master(master).getOrCreate().sparkContext
  private val INTERVERL=conf.getOrElse(MyConsumerVal.INTERVERL,"2").toInt
  private val WINDOWLENGTH_BEILV=conf.getOrElse(MyConsumerVal.WINDOWLENGTH_BEILV,"5").toInt
  private val WINDOWSLIDE_BEILV=conf.getOrElse(MyConsumerVal.WINDOWSLIDE_BEILV,"5").toInt
  private val Globe_kafkaOffsetPath = conf.getOrElse(MyConsumerVal.GLOBE_OFFSET_PATH,"/kafka/offsets")
  private var kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->kafkaHostPort,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> conf.getOrElse(MyConsumerVal.AUTO_OFFSET_RESET_CONFIG,"latest"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )
  if(otherKafkaConf!=null) kafkaParams++=otherKafkaConf

  private val zkclient=CuratorFrameworkFactory.builder()
    .connectString(zkHostPort)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build()

  private val spark=sparkContext
  private val acc=spark.collectionAccumulator[Array[(String,Int,Long)]]
  private val ssc=new StreamingContext(spark,Seconds(INTERVERL))
  private val directStream=createDirectStream(ssc,kafkaParams,Set(kafkaTopic),groupId)


  //对外接口，封装了维护偏移量的过程
  //惨啊，DsStream指定了所有的类型，因此这里不能用泛型代劳了
    def plan[K,V](plan:Plan[K,V]):this.type={
    //注意闭包
    val directStream=this.directStream
    val acc=this.acc
    val mapPlan=directStream.mapPartitions(a=>a.map(rdd=> {
      //每个分区获取偏移量
      val offsets = Array[(String,Int,Long)]((rdd.topic(), rdd.partition(), rdd.offset()))
      val result=plan.mapMethod(rdd.value().asInstanceOf[String])
      result._1->(result._2,offsets)
    }))

    val reduceByKeyPlan=mapPlan.reduceByKey((v1,v2)=>
      (plan.reduceByKeyMethod(v1._1,v2._1),v1._2++v2._2)
    )//计算完成后将偏移量发送给driver端
      .mapPartitions(a=>a.map(rdd=>{
        acc.add(rdd._2._2)
        (rdd._1, rdd._2._1)
      })).cache()

    reduceByKeyPlan.window(Duration(INTERVERL*WINDOWLENGTH_BEILV*1000),Duration(INTERVERL*WINDOWSLIDE_BEILV*1000))
        .foreachRDD(
          rdd=> {
            rdd.foreach(a =>plan.windowMehthod(a._1, a._2))
            //每次窗口统一保存偏移量
            if(!acc.isZero) {
              val arr2 = mutable.ArrayBuffer[(String, Int, Long)]()
              val iterator = acc.value.iterator()
              while (iterator.hasNext)
                arr2 ++= iterator.next()
              val offsets = arr2.groupBy(a => (a._1, a._2)).mapValues(a => {
                a.map(b => b._3).sorted(Ordering[Long].reverse)(0)
              }).toArray.map(a => (a._1._1, a._1._2, a._2))
              saveOffsets(offsets)
              acc.reset()
            }
          }
        )
    this
  }
  //对外接口，启动sparkStreaming
  def start={
    ssc.start()
    ssc.awaitTermination()
  }

  //保存偏移量
  private def saveOffsets(offsetRange: Array[(String,Int,Long)])={
    val zkclient=this.zkclient
    offsetRange.foreach(o=>{
      val zkPath = s"${Globe_kafkaOffsetPath}/${groupId}/${o._1}/${o._2}"
      if (zkclient.checkExists().forPath(zkPath) == null)
        zkclient.create().creatingParentsIfNeeded().forPath(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o._1 +", Partition:" + o._2 + ", Offset:" + (o._3+1))
      zkclient.setData().forPath(zkPath,(o._3+1).toString.getBytes())
    })
    this
  }

  //读取偏移量
  private def readOffsets= {
    //注意闭包
    val zkclient=this.zkclient
    zkclient.start()
    val zkTopicPath = Globe_kafkaOffsetPath+"/"+groupId+"/"+kafkaTopic

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
  private def createDirectStream(ssc:StreamingContext, kafkaParams:Map[String, Object], topic:Set[String], groupName:String)={
    val offsets=readOffsets
    if(offsets.isEmpty)
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topic,kafkaParams))
    else
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[Any,Any](topic,kafkaParams,offsets))
  }
}