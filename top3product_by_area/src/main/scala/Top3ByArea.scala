
import java.net.URLDecoder
import java.util.UUID

import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.constant.Constants
import cn.dc.commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
/**
  * 先从行动表中抽取出cityId -> clickProductId 的RDD
  * 再创建位置信息的RDD 元素为(cityId cityName Area) 因为没有对应的表，所以在内存中创建一个数组
  * 再把2个RDD join，就可以获取区域内的被点击的商品的点击次数
  */
object Top3ByArea {



  def main(args: Array[String]): Unit = {
    /*
    基本配置，环境搭建
      创建ss对象
      从配文中读取数据的基本过滤条件
      创建taskUUID
     */
    val sparkSession: SparkSession = SparkSession.builder().config( new SparkConf().setAppName("top3ByArea").setMaster("local[*]")).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setLogLevel("OFF")

    val taskParamJsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val tastParam = JSONObject.fromObject(taskParamJsonStr)
    val taskUUID = UUID.randomUUID().toString
    /*
    创建符合过滤条件的cityId->clickProductId的RDD
      是将所有点击行为的cityId和clickProductId的RDD2个属性过滤出来，肯定会有重复
      基本过滤条件就是时间
      因为要使用sql，所以要先把所有的cityId->clickProductId的信息映射为一个临时表
     */
   val cityId2PidRDD: RDD[(Long, Row)] = getCityId2PidRDD(sparkSession,tastParam)
    /*
     创建位置信息的RDD，因为没有在hive中创建这个表，所以在内存中手动建一个模拟
        元素是3个值的tuple，RDD[city_id -> (city_id,city_name,area)]
     */
    val cityid2CityInfoRDD: RDD[(Long, Row)] = getcityid2CityInfoRDD(sparkSession)
    /*
    将cityId2PidRDD和cityid2CityInfoRDD join，转为一个临时表
    RDD[cityId->clickProductId]   RDD[city_id -> (city_id,city_name,area)]
    join完RDD[cityId -> (clickProductId,(city_id,city_name,area))]
    要map为RDD[(cityId,city_name,area,pid)]
    表名：tmp_click_product_basic
     */
    generateTempClickProductBasicTable(sparkSession, cityId2PidRDD, cityid2CityInfoRDD)
   // sparkSession.sql("select area,product_id,count(*) from tmp_click_product_basic group by area,product_id ").show()
    /*
    使用UDAF创建聚合了城市信息的临时表
      注册UDF函数
      tmp_area_product_click_count
     */
    sparkSession.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => v1.toString + split + v2)

    sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF())
    generateTempAreaPrdocutClickCountTable(sparkSession)
    //sparkSession.sql("select * from tmp_area_product_click_count ").show()
  /*
   join进product的详细信息从productInfo表中

   productInfo中的extendInfo列是json数据，需要一个UDF函数解析
   tmp_area_fullprod_click_count
   */
    sparkSession.udf.register("get_json_object", (json: String, field: String) => {

      val jsonObject = JSONObject.fromObject(URLDecoder.decode(json,"UTF-8"));
      jsonObject.getString(field)
    })
    generateTempAreaFullProductClickCountTable(sparkSession: SparkSession)
    //sparkSession.sql("select * from tmp_area_fullprod_click_count ").show()
    /*

     */
    import sparkSession.implicits._
    val areaTop3ProductDF = getAreaTop3ProductRDD(taskUUID,sparkSession).rdd.map(row =>
      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"), row.getAs[Long]("product_id"), row.getAs[String]("city_infos"), row.getAs[Long]("click_count"), row.getAs[String]("product_name"), row.getAs[String]("product_status"))
    ).toDF()
    //areaTop3ProductDF.rdd.foreach(print)
    areaTop3ProductDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    sparkSession.close()

  }
  def getCityId2PidRDD(sparkSession: SparkSession, tastParam: JSONObject) = {
    /*
    基本过滤条件设置
      click_product_id是long类型，所以不能设为null，没有用-1表示
     */
    val startDate = ParamUtils.getParam(tastParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(tastParam,Constants.PARAM_END_DATE)
    val sql = "SELECT city_id,click_product_id FROM user_visit_action WHERE date >= '" + startDate + "' AND date <='" + endDate + "' AND click_product_id != -1"

    sparkSession.sql(sql).toDF("city_id","click_product_id").rdd.map{
      case city2pid =>
        (city2pid.getAs[Long]("city_id"),city2pid)
    }
  }


  def getcityid2CityInfoRDD(spark: SparkSession): RDD[(Long, Row)] = {
    import spark.implicits._
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    val cityInfoDF = spark.sparkContext.makeRDD(cityInfo).toDF("city_id", "city_name", "area")
    cityInfoDF.rdd.map(item => (item.getAs[Long]("city_id"), item))
  }

  def generateTempClickProductBasicTable(spark: SparkSession, cityid2clickActionRDD: RDD[(Long, Row)], cityid2cityInfoRDD: RDD[(Long, Row)]) {
    // 执行join操作，进行点击行为数据和城市数据的关联
    val joinedRDD: RDD[(Long, (Row, Row))] = cityid2clickActionRDD.join(cityid2cityInfoRDD)

    // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
    val mappedRDD = joinedRDD.map { case (cityid, (action, cityinfo)) =>
      val productid = action.getLong(1)
      val cityName = cityinfo.getString(1)
      val area = cityinfo.getString(2)
      (cityid, cityName, area, productid)
    }
    // 1 北京
    // 2 上海
    // 1 北京
    // group by area,product_id
    // 1:北京,2:上海

    // 两个函数
    // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
    // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
    import spark.implicits._
    val df = mappedRDD.toDF("city_id", "city_name", "area", "product_id")
    // 为df创建临时表
    df.createOrReplaceTempView("tmp_click_product_basic")
  }


  def generateTempAreaPrdocutClickCountTable(spark: SparkSession) {

    // 按照area和product_id两个字段进行分组
    // 计算出各区域各商品的点击次数
    // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
    val sql = "SELECT " +
      "area," +
      "product_id," +
      "count(*) click_count, " +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      "FROM tmp_click_product_basic " +
      "GROUP BY area,product_id "

    val df = spark.sql(sql)

    // 各区域各商品的点击次数（以及额外的城市列表）,再次将查询出来的数据注册为一个临时表
    df.createOrReplaceTempView("tmp_area_product_click_count")
  }

  def generateTempAreaFullProductClickCountTable(spark: SparkSession) {

    // 将之前得到的各区域各商品点击次数表，product_id
    // 去关联商品信息表，product_id，product_name和product_status
    // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
    // get_json_object()函数，可以从json串中获取指定的字段的值
    // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
    // area, product_id, click_count, city_infos, product_name, product_status

    // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
    // 其实是很重要的一件事

    // 技术点：内置if函数的使用

    val sql = "SELECT " +
      "tapcc.area," +
      "tapcc.product_id," +
      "tapcc.click_count," +
      "tapcc.city_infos," +
      "pi.product_name," +
      "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      "FROM tmp_area_product_click_count tapcc " +
      "JOIN product_info pi ON tapcc.product_id=pi.product_id "

    val df = spark.sql(sql)

    df.createOrReplaceTempView("tmp_area_fullprod_click_count")
  }


  def getAreaTop3ProductRDD(taskid: String, spark: SparkSession): DataFrame = {

    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

    val sql = "SELECT " +
      "area," +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A Level' " +
      "WHEN area='华南' OR area='华中' THEN 'B Level' " +
      "WHEN area='西北' OR area='西南' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level," +
      "product_id," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "FROM (" +
      "SELECT " +
      "area," +
      "product_id," +
      "click_count," +
      "city_infos," +
      "product_name," +
      "product_status," +
      "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
      "FROM tmp_area_fullprod_click_count " +
      ") t " +
      "WHERE rank<=3"

    spark.sql(sql)
  }

}
