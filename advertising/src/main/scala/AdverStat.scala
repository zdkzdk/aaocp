import java.util.Date

import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {
    /*
    创建上下文，搭建页面代码环境
    构建Spark上下文
    创建Spark客户端
    创建kafka的连接配置
    创建kafka消费者配置
    设置检查点目录
     */
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./streaming_checkpoint")
    // 获取Kafka配置
    val broker_list = ConfigurationManager.config.getString("kafka.broker.list")
    val topics = ConfigurationManager.config.getString("kafka.topics")

    /*
     kafka消费者配置
     创建kafka消费者需要几个配置
     */
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    /*
    创建DStream，准备数据源
       LocationStrategies：根据给定的主题和集群地址创建consumer
       LocationStrategies.PreferConsistent：均匀的在所有Executor之间分配分区
                          PreferBroker：kafka和Executor在一个节点
                         preferFixed：自己手动指定
       ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
       ConsumerStrategies.Subscribe：订阅一系列主题

       在kafka和sparkStr整合时，消费者是sparkStr的Executor，因为driver会控制kafka直接向Executor发送消息

       createDirectStream[String,String] 的泛型是消费的消息的kv类型
     */
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))
    /*
    DStream是多个RDD组成，RDD由message组成，message (k,v)  v就是发过来的封装了5个属性的字符串
     */
    var adRealTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())


    /*
     根据动态黑名单进行数据过滤 (userid, timestamp province city userid adid)
     */
    val filteredAdRealTimeLogDStream = filterByBlacklist(spark, adRealTimeValueDStream)
    //filteredAdRealTimeLogDStream.foreachRDD(rdd => rdd.foreach(println))


    /*
    需求一
    生成动态黑名单
      黑名单的数据是从DStream中动态获取然后存到mysql中的，是根据日期不断变化的，driver每获取一次RDD，就会更新一次
      统计 天_userid_ad 的点击次数，保存在ad_user_click_count表中
        把adRealTimeValueDStream的数据拼成 (天_userid_ad,1L)的形式
        countByKey
        封装到Bean中存到mysql   把(天_userid_ad,1L)存到mysql是为了获取userId时从mysql中查，
            存到mysql持久化可以释放内存，也可以记录每天的点击数
        根据countByKey的RDD filter conut > 100的，得到>100 的userId，然后存到mysql中
   */
    generateDynamicBlacklist(adRealTimeValueDStream)
    /*
    需求二
    省市每个广告的点击总数
     */
    val adRealTimeStatDStream = filteredAdRealTimeLogDStream.map {
      case (userId:Long, log:String) =>
        val logSplited = log.split(" ")

        val timestamp = logSplited(0)
        val date = new Date(timestamp.toLong)
        val datekey = DateUtils.formatDateKey(date)

        val province = logSplited(1)
        val city = logSplited(2)
        val adid = logSplited(4).toLong

        val key = datekey + "_" + province + "_" + city + "_" + adid

        (key, 1L)
    }.updateStateByKey[Long] {
      (values:Seq[Long], oldCount:Option[Long]) =>{
        var newCount = 0L
        if (oldCount.isDefined) newCount = oldCount.get
        newCount += values.sum
        Some(newCount)}
    }
    adRealTimeStatDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>

            //批量保存到数据库
            val adStats = ArrayBuffer[AdStat]()

            for(item <- items){
              val keySplited = item._1.split("_")
              val date = keySplited(0)
              val province = keySplited(1)
              val city = keySplited(2)
              val adid = keySplited(3).toLong

              val clickCount = item._2
              adStats += AdStat(date,province,city,adid,clickCount)
            }
            AdStatDAO.updateBatch(adStats.toArray)

        }
    }
    // 子需求三：实时统计每天每个省份top3热门广告
    calculateProvinceTop3Ad(spark,adRealTimeStatDStream)

    // 子需求四：实时统计每天每个广告在最近1小时的滑动窗口内的每分钟的点击量，点击趋势
    calculateAdClickCountByWindow(adRealTimeValueDStream)

    ssc.start()
    ssc.awaitTermination()
  }

  def filterByBlacklist(spark: SparkSession, adRealTimeValueDStream: DStream[String]): DStream[(Long, String)] = {
    // 刚刚接受到原始的用户点击行为日志之后
    // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
    // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）

    val filteredAdRealTimeLogDStream = adRealTimeValueDStream.transform { consumerRecordRDD =>

      // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
      val adBlacklists = AdBlacklistDAO.findAll()

      val blacklistRDD = spark.sparkContext.makeRDD(adBlacklists.map(item => (item.userid, true)))

      // 将原始数据rdd映射成<userid, tuple2<string, string>>
      val mappedRDD = consumerRecordRDD.map(consumerRecord => {
        val userid = consumerRecord.split(" ")(3).toLong
        (userid, consumerRecord)
      })

      // 将原始日志数据rdd，与黑名单rdd，进行左外连接
      // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
      // 用inner join，内连接，会导致数据丢失
      val joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD)

      val filteredRDD = joinedRDD.filter { case (userid, (log, black)) =>
        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
        if (black.isDefined && black.get) false else true
      }

      filteredRDD.map { case (userid, (log, black)) => (userid, log) }
    }

    filteredAdRealTimeLogDStream
  }

  def generateDynamicBlacklist(adRealTimeValueDStream: DStream[String]) = {
    val dailyUserAdClickCountDStream = adRealTimeValueDStream.map {
      case value =>
        val valueSplit = value.split(" ")
        val date = DateUtils.formatDateKey(new Date(valueSplit(0).toLong))
        val userId = valueSplit(3).toLong
        val adId = valueSplit(4).toLong
        (date + "_" + userId + "_" + adId, 1L)
    }.reduceByKey(_ + _)

    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    dailyUserAdClickCountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        // 对每个分区的数据就去获取一次连接对象
        // 每次都是从连接池中获取，而不是每次都创建
        // 写数据库操作，性能已经提到最高了

        val adUserClickCounts = ArrayBuffer[AdUserClickCount]()
        for (item <- items) {
          val keySplited = item._1.split("_")
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          // yyyy-MM-dd
          val userid = keySplited(1).toLong
          val adid = keySplited(2).toLong
          val clickCount = item._2

          //批量插入
          adUserClickCounts += AdUserClickCount(date, userid, adid, clickCount)
        }
        AdUserClickCountDAO.updateBatch(adUserClickCounts.toArray)
      }
    }
    // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
    // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
    // 从mysql中查询
    // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
    // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
    val blacklistDStream = dailyUserAdClickCountDStream.filter { case (key, count) =>
      val keySplited = key.split("_")

      // yyyyMMdd -> yyyy-MM-dd
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong

      // 从mysql中查询指定日期指定用户对指定广告的点击量
      val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

      // 判断，如果点击量大于等于10
      // 那么就拉入黑名单，返回true
      if (clickCount >= 10) true else false
    }
    //转成元素只有userid的然后去重然后使用foreachPartition存到mysql中
    blacklistDStream.map(item => item._1.split("_")(1).toLong).transform(rdd => rdd.distinct).foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        var adBlacklists = Set[Long]()

        for (item <- items)
          adBlacklists += item.toLong

        AdBlacklistDAO.insertBatch(adBlacklists.toArray)
      }
    }
  }
  /**
    * 业务功能三：计算每天各省份的top3热门广告
    * @param adRealTimeStatDStream
    */
  def calculateProvinceTop3Ad(spark:SparkSession, adRealTimeStatDStream:DStream[(String, Long)]) {

    // 每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量
    val rowsDStream = adRealTimeStatDStream.transform{ rdd =>

      // <yyyyMMdd_province_city_adid, clickCount>
      // <yyyyMMdd_province_adid, clickCount>

      // 计算出每天各省份各广告的点击量
      val mappedRDD = rdd.map{ case (keyString, count) =>

        val keySplited = keyString.split("_")
        val date = keySplited(0)
        val province = keySplited(1)
        val adid = keySplited(3).toLong
        val clickCount = count

        val key = date + "_" + province + "_" + adid
        (key, clickCount)
      }

      val dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey( _ + _ )

      // 将dailyAdClickCountByProvinceRDD转换为DataFrame
      // 注册为一张临时表
      // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
      val rowsRDD = dailyAdClickCountByProvinceRDD.map{ case (keyString, count) =>

        val keySplited = keyString.split("_")
        val datekey = keySplited(0)
        val province = keySplited(1)
        val adid = keySplited(2).toLong
        val clickCount = count

        val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))

        (date, province, adid, clickCount)

      }

      import spark.implicits._
      val dailyAdClickCountByProvinceDF = rowsRDD.toDF("date","province","ad_id","click_count")

      // 将dailyAdClickCountByProvinceDF，注册成一张临时表
      dailyAdClickCountByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov")

      // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
      val provinceTop3AdDF = spark.sql(
        "SELECT "
          + "date,"
          + "province,"
          + "ad_id,"
          + "click_count "
          + "FROM ( "
          + "SELECT "
          + "date,"
          + "province,"
          + "ad_id,"
          + "click_count,"
          + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
          + "FROM tmp_daily_ad_click_count_by_prov "
          + ") t "
          + "WHERE rank<=3"
      )

      provinceTop3AdDF.rdd
    }

    // 每次都是刷新出来各个省份最热门的top3广告，将其中的数据批量更新到MySQL中
    rowsDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>

        // 插入数据库
        val adProvinceTop3s = ArrayBuffer[AdProvinceTop3]()

        for (item <- items){
          val date = item.getString(0)
          val province = item.getString(1)
          val adid = item.getLong(2)
          val clickCount = item.getLong(3)
          adProvinceTop3s += AdProvinceTop3(date,province,adid,clickCount)
        }
        AdProvinceTop3DAO.updateBatch(adProvinceTop3s.toArray)

      }
    }
  }
  /**
    * 业务功能四：计算最近1小时滑动窗口内的广告点击趋势
    * @param adRealTimeValueDStream
    */
  def calculateAdClickCountByWindow(adRealTimeValueDStream:DStream[String]) {

    // 映射成<yyyyMMddHHMM_adid,1L>格式
    val pairDStream = adRealTimeValueDStream.map{ case consumerRecord  =>
      val logSplited = consumerRecord.split(" ")
      val timeMinute = DateUtils.formatTimeMinute(new Date(logSplited(0).toLong))
      val adid = logSplited(4).toLong

      (timeMinute + "_" + adid, 1L)
    }

    // 计算窗口函数，1小时滑动窗口内的广告点击趋势
    val aggrRDD = pairDStream.reduceByKeyAndWindow((a:Long,b:Long) => (a + b),Minutes(1L), Seconds(5L))

    // 最近1小时内，各分钟的点击量，并保存到数据库
    aggrRDD.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        //保存到数据库
        val adClickTrends = ArrayBuffer[AdClickTrend]()
        for (item <- items){
          val keySplited = item._1.split("_")
          // yyyyMMddHHmm
          val dateMinute = keySplited(0)
          val adid = keySplited(1).toLong
          val clickCount = item._2

          val date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
          val hour = dateMinute.substring(8, 10)
          val minute = dateMinute.substring(10)

          adClickTrends += AdClickTrend(date,hour,minute,adid,clickCount)
        }
        AdClickTrendDAO.updateBatch(adClickTrends.toArray)
      }
    }
  }
}





