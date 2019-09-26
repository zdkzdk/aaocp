package cn.dc

import java.net.URLDecoder
import java.util.UUID

import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.constant.Constants
import cn.dc.commons.model.UserVisitAction
import cn.dc.commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 页面单跳转化率模块spark作业
  *
  * 页面转化率的求解思路是通过UserAction表获取一个session的所有UserAction，根据时间顺序排序后获取全部PageId
  * 然后将PageId组合成PageFlow，即1,2,3,4,5的形式（按照时间顺序排列），之后，组合为1_2, 2_3, 3_4, ...的形式
  * 然后筛选出出现在targetFlow中的所有A_B
  *
  * 对每个A_B进行数量统计，然后统计startPage的PV，之后根据targetFlow的A_B顺序，计算每一层的转化率
  *
  */
object PageOneStepConvertRate {

  def main(args: Array[String]): Unit = {

    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    // 查询指定日期范围内的用户访问行为数据
    val actionRDD = this.getActionRDDByDateRange(spark, taskParam)

    // 将用户行为信息转换为 K-V 结构
    val sessionid2actionRDD = actionRDD.map(item => (item.session_id, item))

    // 将数据进行内存缓存
    sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY)

    // 对<sessionid,访问行为> RDD，做一次groupByKey操作，生成页面切片
    val sessionid2actionsRDD = sessionid2actionRDD.groupByKey()

    // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
    val pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionsRDD, taskParam)
    // 返回：(1_2, 1)，(3_4, 1), ..., (100_101, 1)
    // 统计每个跳转切片的总个数
    // pageSplitPvMap：(1_2, 102320), (3_4, 90021), ..., (100_101, 45789)
    val pageSplitPvMap = pageSplitRDD.countByKey

    // 使用者指定的页面流是3,2,5,8,6
    // 咱们现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6

    // 首先计算首页PV的数量
    val startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD)

    // 计算目标页面流的各个页面切片的转化率
    val convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv)

    // 持久化页面切片转化率
    persistConvertRate(spark, taskUUID, convertRateMap)

    spark.close()

  }

  /**
    * 持久化转化率
    *
    * @param convertRateMap
    */
  def persistConvertRate(spark: SparkSession, taskid: String, convertRateMap: collection.Map[String, Double]) {

    val convertRate = convertRateMap.map(item => item._1 + "=" + item._2).mkString("|")

    val pageSplitConvertRateRDD = spark.sparkContext.makeRDD(Array(PageSplitConvertRate(taskid, convertRate)))

    import spark.implicits._
    pageSplitConvertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * 计算页面切片转化率
    *
    * @param pageSplitPvMap 页面切片pv
    * @param startPagePv    起始页面pv
    * @return
    */
  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitPvMap: collection.Map[String, Long], startPagePv: Long): collection.Map[String, Double] = {

    val convertRateMap = new mutable.HashMap[String, Double]()
    //1,2,3,4,5,6,7
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPages = targetPageFlow.split(",").toList
    //(1_2,2_3,3_4,4_5,5_6,6_7)
    val targetPagePairs = targetPages.slice(0, targetPages.length - 1).zip(targetPages.tail).map(item => item._1 + "_" + item._2)

    // lastPageSplitPv：存储最新一次的页面PV数量
    var lastPageSplitPv = startPagePv.toDouble
    // 3,5,2,4,6
    // 3_5
    // 3_5 pv / 3 pv
    // 5_2 rate = 5_2 pv / 3_5 pv

    // 通过for循环，获取目标页面流中的各个页面切片（pv）
    for (targetPage <- targetPagePairs) {
      // 先获取pageSplitPvMap中记录的当前targetPage的数量
      val targetPageSplitPv = pageSplitPvMap.get(targetPage).get.toDouble
      println((targetPageSplitPv, lastPageSplitPv))
      // 用当前targetPage的数量除以上一次lastPageSplit的数量，得到转化率
      val convertRate = NumberUtils.formatDouble(targetPageSplitPv / lastPageSplitPv, 2)
      // 对targetPage和转化率进行存储
      convertRateMap.put(targetPage, convertRate)
      // 将本次的targetPage作为下一次的lastPageSplitPv
      lastPageSplitPv = targetPageSplitPv
    }

    convertRateMap
  }

  /**
    * 获取页面流中初始页面的pv
    *
    * @param taskParam
    * @param sessionid2actionsRDD
    * @return
    */
  def getStartPagePv(taskParam: JSONObject, sessionid2actionsRDD: RDD[(String, Iterable[UserVisitAction])]): Long = {

    // 获取配置文件中的targetPageFlow
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // 获取起始页面ID
    val startPageId = targetPageFlow.split(",")(0).toLong
    // sessionid2actionsRDD是聚合后的用户行为数据
    // userVisitAction中记录的是在一个页面中的用户行为数据
    val startPageRDD = sessionid2actionsRDD.flatMap { case (sessionid, userVisitActions) =>
      // 过滤出所有PageId为startPageId的用户行为数据
      userVisitActions.filter(_.page_id == startPageId).map(_.page_id)
    }
    // 对PageId等于startPageId的用户行为数据进行技术
    startPageRDD.count()
  }

  /**
    * 页面切片生成与匹配算法
    * 注意，一开始我们只有UserAciton信息，通过将UserAction按照时间进行排序，然后提取PageId，再进行连接，可以得到PageFlow
    *
    * @param sc
    * @param sessionid2actionsRDD
    * @param taskParam
    * @return
    */
  def generateAndMatchPageSplit(sc: SparkContext, sessionid2actionsRDD: RDD[(String, Iterable[UserVisitAction])], taskParam: JSONObject): RDD[(String, Int)] = {

    /* 对目标PageFlow进行解析 */

    //1,2,3,4,5,6,7
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    //将字符串转换成为了List[String]
    val targetPages = targetPageFlow.split(",").toList

    //targetPages.slice(0, targetPages.length-1) ：[1,2,3,4,5,6]
    //targetPages.tail ：[2,3,4,5,6,7]
    //targetPages.slice(0, targetPages.length-1).zip(targetPages.tail)：(1,2)(2,3)(3,4)(4,5)(5,6)(6,7)
    //map(item => item._1 + "_" + item._2)：(1_2,2_3,3_4,4_5,5_6,6_7)
    val targetPagePairs = targetPages.slice(0, targetPages.length - 1).zip(targetPages.tail).map(item => item._1 + "_" + item._2)
    //将结果转换为广播变量
    //targetPagePairs类型为List[String]
    val targetPageFlowBroadcast = sc.broadcast(targetPagePairs)

    /* 对所有PageFlow进行解析 */

    // 对全部数据进行处理
    sessionid2actionsRDD.flatMap { case (sessionid, userVisitActions) =>
      // 获取使用者指定的页面流
      // 使用者指定的页面流，1,2,3,4,5,6,7
      // 1->2的转化率是多少？2->3的转化率是多少？

      // 这里，我们拿到的session的访问行为，默认情况下是乱序的
      // 比如说，正常情况下，我们希望拿到的数据，是按照时间顺序排序的
      // 但是问题是，默认是不排序的
      // 所以，我们第一件事情，对session的访问行为数据按照时间进行排序

      // 举例，反例
      // 比如，3->5->4->10->7
      // 3->4->5->7->10

      // userVisitActions是Iterable[UserAction]，toList.sortWith将Iterable中的所有UserAction按照时间进行排序
      // 按照时间排序
      val sortedUVAs = userVisitActions.toList.sortWith((uva1, uva2) => DateUtils.parseTime(uva1.action_time).getTime() < DateUtils.parseTime(uva2.action_time).getTime())
      // 提取所有UserAction中的PageId信息
      val soredPages = sortedUVAs.map(item => if (item.page_id != null) item.page_id)

      //【注意】页面的PageFlow是将session的所有UserAction按照时间顺序排序后提取PageId,再将PageId进行连接得到的
      // 按照已经排好的顺序对PageId信息进行整合，生成所有页面切片：(1_2,2_3,3_4,4_5,5_6,6_7)
      val sessionPagePairs = soredPages.slice(0, soredPages.length - 1).zip(soredPages.tail).map(item => item._1 + "_" + item._2)

      /* 由此，得到了当前session的PageFlow */

      // 只要是当前session的PageFlow有一个切片与targetPageFlow中任一切片重合，那么就保留下来
      // 目标：(1_2,2_3,3_4,4_5,5_6,6_7)   当前：(1_2,2_5,5_6,6_7,7_8)
      // 最后保留：(1_2,5_6,6_7)
      // 输出：(1_2, 1) (5_6, 1) (6_7, 1)
      sessionPagePairs.filter(targetPageFlowBroadcast.value.contains(_)).map((_, 1))
    }
  }

  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    import spark.implicits._
    spark.sql("select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'")
      .map {
        case row: Row =>
          (row.get(1).toString, tran(row.get(2).toString), row.get(3).toString, tran(row.get(4).toString), URLDecoder.decode(row.get(5).toString, "UTF-8"),
            URLDecoder.decode(row.get(6).toString, "UTF-8"), tran(row.get(7).toString), tran(row.get(8).toString), row.get(9).toString, row.get(10).toString, row.get(11).toString, row.get(12).toString, tran(row.get(12).toString))
      }.toDF("date", "user_id", "session_id", "page_id", "action_time",
      "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")
      .as[UserVisitAction].rdd


  }

  def tran(input: String): Long = {
    if (input != "") return input.toLong else -1
  }

}
