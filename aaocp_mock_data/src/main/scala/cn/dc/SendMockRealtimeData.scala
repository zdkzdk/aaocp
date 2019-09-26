package cn.dc

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.{Properties, Random}

import cn.dc.commons.conf.ConfigurationManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

object SendMockRealtimeData {
  /**
    * 模拟的数据
    * 时间点: 当前时间毫秒
    * userId: 0 - 99
    * 省份、城市 ID相同 ： 1 - 9
    * adid: 0 - 19
    * ((0L,"北京","北京"),(1L,"上海","上海"),(2L,"南京","江苏省"),(3L,"广州","广东省"),(4L,"三亚","海南省"),(5L,"武汉","湖北省"),(6L,"长沙","湖南省"),(7L,"西安","陕西省"),(8L,"成都","四川省"),(9L,"哈尔滨","东北省"))
    * 格式 ：timestamp province city userid adid
    * 某个时间点 某个省份 某个城市 某个用户 某个广告
    */
  def generateMockData(): Array[String] = {
    val array = ArrayBuffer[String]()
    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {

      var timestamp = System.currentTimeMillis()
      //为了更好的展示数据，决定当前小时内，均匀生成每分钟的数据
      timestamp = LocalDateTime.of(LocalDate.now(), LocalTime.of(LocalDateTime.now().getHour,random.nextInt(60))).toInstant(ZoneOffset.of("+8")).toEpochMilli();
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(10)
      //val userid = random.nextInt(100)
      val userid = random.nextInt(10)

      // 拼接实时数据
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    new KafkaProducer[String, String](prop)
  }


  def main(args: Array[String]): Unit = {

    // 获取配置文件commerce.properties中的Kafka配置参数
    val broker = ConfigurationManager.config.getString("kafka.broker.list")
    val topic = ConfigurationManager.config.getString("kafka.topics")

    // 创建Kafka消费者
    val kafkaProducer = createKafkaProducer(broker)

    while (true) {
      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (item <- generateMockData()) {
        print(item)
        kafkaProducer.send(new ProducerRecord[String, String](topic, item))
      }
      Thread.sleep(500)
    }
  }

}
