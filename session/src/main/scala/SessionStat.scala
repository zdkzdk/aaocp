import java.net.URLDecoder
import java.util.{Date, Random, UUID}

import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.constant.Constants
import cn.dc.commons.model.{UserInfo, UserVisitAction}
import cn.dc.commons.utils._
import net.sf.json.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object SessonStat {
  def main(args: Array[String]): Unit = {
    /*
    从配置类读取json字符串然后转成对象
      配置包括3部分：
        mysql
        kafka
        清洗数据的过滤条件
     */
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    //封装ETL条件
    /*
    创建全局唯一的主键，任务执行完把结果放到mysql中时要用到taskUUID
    每次任务数据是相同的，但可能设置的筛选条件不同，导致结果不同，所以要标识一下
     */
    val taskUUID = UUID.randomUUID().toString
    /*
    spark
     */
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    //sparkSession包含sparkContext
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //日志，作用
    sparkSession.sparkContext.setLogLevel("OFF")
    /*
    第一次数据筛选，通过sql筛选日期，将action数据转成RDD[UserVisitAction]，作为后续分析的基础数据源
    初步根据设置的时间范围进行第一次数据清洗
      根据taskParam中设置的时间范围。
      把DS使用rdd转成RDD后能获取到RDD[T]，相当于RDD中每个元素都是T类型
     */
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"

    import sparkSession.implicits._
    val actionRDD: RDD[UserVisitAction] = sparkSession.sql(sql).map {
      case row: Row =>
        (row.get(1).toString, tran(row.get(2).toString), row.get(3).toString, tran(row.get(4).toString), URLDecoder.decode(row.get(5).toString, "UTF-8"),
          URLDecoder.decode(row.get(6).toString, "UTF-8"), tran(row.get(7).toString), tran(row.get(8).toString), row.get(9).toString, row.get(10).toString, row.get(11).toString, row.get(12).toString, tran(row.get(12).toString))
    }.toDF("date", "user_id", "session_id", "page_id", "action_time",
      "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")
      .as[UserVisitAction].rdd

    /*
    对actionRDD进行格式转换，从UserVisitAction对象，转为sessionId -> action ,根据id聚合 sessionId -> List(action)，
    然后转为userId -> 属性字符串，以userId为key是为了跟user_info join
    userId -> 属性字符串跟userId userFullInfo join 得到 sessionId2FullRDD(sessionId, 带user信息的完整信息字符串)
    使用accumulator统计符合条件的session数、步长、时长
     */
    val sessionId2action: RDD[(String, UserVisitAction)] = actionRDD.map(action => action.session_id -> action)
    val sessionId2ActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2action.groupByKey()

    /*
    sessionId2FullRDD为之后的4个子需求提供分析源
    sessionId2FullRDD:
    RDD[(sessionId, sessionId、搜索关键字、点击类目的ids、时长、步长信息、开始时间、年龄、职业、性别、城市的字符串)]
    k=v|k=v
     */
    val sessionId2FullRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, sessionId2ActionRDD)
    //进行缓存提高性能
    sessionId2FullRDD.cache()


    /*
    子需求一 ： 所有session中访问步长，访问时长和总访问步长、时长的比值
    指标：比值
    维度：
    getSessionFilteredRDD：根据设置的过滤条件过滤数据，然后统计各时长、步长各维度的数量放入累加器中
    然后把累加器的结果存到mysql中
     */
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator, "合格session数，访问时长，步长")
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullRDD, sessionAccumulator)
    println(sessionId2FilterRDD.count())
    print(sessionAccumulator.value.size)

    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)
    /*
    子需求二 ：按比例随机抽取session
    抽取100个session
    按天平均分配，一天是 100/d = cd
    一天的session按24个小时group，每个小时的session数 = 每个小时的session总数占当天总session数的比例 * cd

     */
    randomExtractSession(sparkSession, taskUUID, sessionId2FullRDD, sessionId2action)
    /*
    子需求3：按点击数、下单、付款数求top10的category
      获取所有有3种行为的categoryId，然后去重，获取总的allRDD
      分别获取有3种行为的categoryId -> 个数 的RDD，然后跟allRDD进行累加式join，获取没有排序的cid - 点击数 - 下单数 - 付款数的RDD
      将这个RDD改造为RDD[SortKey,line]的形式，使用sortByKey排序
      返回排名前十的品类是为了在业务功能四中进行使用
     */

    val top10CategoryList = getTop10Category(sparkSession, taskUUID, sessionId2action)

    val categoryArray = new ArrayBuffer[(Long, Long)]
    actionRDD.map({
      case action if (action.click_category_id != -1) => categoryArray += ((action.click_category_id, action.click_category_id))
      case action if action.order_category_ids != null =>
        for (cid <- action.order_category_ids.split(",")) categoryArray += ((cid.toLong, cid.toLong))
      case action if action.pay_category_ids != null =>
        for (pid <- action.pay_category_ids.split(",")) categoryArray += ((pid.toLong, pid.toLong))
    })
    categoryArray.distinct

    /**
      * 子需求4：点击top10商品次数最多的10个sessionId
      */

    getTop10Session(sparkSession, taskUUID, top10CategoryList, sessionId2action)
  }


  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    // userId2AggrInfoRDD: RDD[(userId, 全部信息的字符串)]
    val userId2AggrInfoRDD = session2GroupActionRDD.map {
      case (sessionId, iterableAction) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null
        /*
        行动步长，表示一个session中做了多少动作
         */
        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          //求开始时间和结束时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          //searchKeywords有多个
          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        // searchKeywords.toString.substring(0, searchKeywords.toString.length)
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)
        // 时间步长，秒为单位，一个session持续多少秒
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }
    /*
    把userId2AggrInfoRDD: RDD[(userId, sessionId、搜索关键字、点击类目的ids、时长、步长信息、开始时间的字符串)]
    和
    RDD[UserInfo]  join ,获取 sessionId2FullRDD(sessionId, aggrInfo + 年龄、职业、性别、城市的字符串)
     */
    import sparkSession.implicits._
    val sqlUser = "SELECT * FROM user_info"
    val userId2UserRDD: RDD[(Long, UserInfo)] = sparkSession.sql(sqlUser).as[UserInfo].rdd.map(user => user.user_id -> user)
    val sessionId2FullRDD = userId2AggrInfoRDD.join(userId2UserRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
    sessionId2FullRDD
  }

  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {
    /*
    过滤条件
     */
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo =
      (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
        (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
        (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
        (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
        (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
        (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
        (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          /*  visitLength match {
              case i if (i >= 1 && i <= 3) => sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
              case i if (i >= 4 && i <= 6) => sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
              case i if (i >= 7 && i <= 9) => sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
              case i if (i >= 10 && i <= 30) => sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
              case i if (i > 30 && i <= 60) => sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
              case i if (i > 60 && i <= 180) => sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
              case i if (i > 180 && i <= 600) => sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
              case i if (i > 600 && i <= 1800) => sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
              case i if (i > 1800) => sessionAccumulator.add(Constants.TIME_PERIOD_30m)
            }*/
          if (visitLength >= 1 && visitLength <= 3) {
            sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
          } else if (visitLength > 1800) {
            sessionAccumulator.add(Constants.TIME_PERIOD_30m)
          }

          if (stepLength >= 1 && stepLength <= 3) {
            sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
          } else if (stepLength > 60) {
            sessionAccumulator.add(Constants.STEP_PERIOD_60)
          }
        }
        success
    }
  }

  def tran(input: String): Long = {
    if (input != "") return input.toLong else -1
  }

  /**
    * 业务需求二：随机抽取session
    *
    * @param sessionid2AggrInfoRDD
    */
  def randomExtractSession(spark: SparkSession, taskUUID: String, sessionid2AggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, UserVisitAction)]) {

    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val time2sessionidRDD = sessionid2AggrInfoRDD.map { case (sessionid, aggrInfo) =>
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      // 将key改为yyyy-MM-dd_HH的形式（小时粒度）
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    }

    // 得到每天每小时的session数量
    // countByKey()计算每个不同的key有多少个数据
    // countMap<yyyy-MM-dd_HH, count>
    val countMap = time2sessionidRDD.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    // dateHourCountMap <yyyy-MM-dd,<HH,count>>
    val dateHourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]()
    //将countMap转为dateHourCountMap
    for ((dateHour, count) <- countMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      // 通过模式匹配实现了if的功能
      dateHourCountMap.get(date) match {
        // 对应日期的数据不存在，则新增
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        // 对应日期的数据存在，则更新
        // 如果有值，Some(hourCountMap)将值取到了hourCountMap中
        case Some(hourCountMap) => hourCountMap += (hour -> count)
      }
    }

    /*
     获取每一天要抽取的数量
     按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
     */
    val extractNumberPerDay = 100 / dateHourCountMap.size

    // dateHourExtractMap[天，[小时，index列表]]
    val dateHourExtractMap = mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()
    val random = new Random()

    /**
      * 根据每个小时应该抽取的数量，来产生随机值
      * 遍历每个小时，填充Map<date,<hour,(3,5,20,102)>>
      *
      * @param hourExtractMap 主要用来存放生成的随机值
      * @param hourCountMap   每个小时的session总数
      * @param sessionCount   当天所有的seesion总数
      */
    def hourExtractMapFunc(hourExtractMap: mutable.HashMap[String, mutable.ListBuffer[Int]], hourCountMap: mutable.HashMap[String, Long], sessionCount: Long) {

      for ((hour, count) <- hourCountMap) {
        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        var hourExtractNumber = ((count / sessionCount.toDouble) * extractNumberPerDay).toInt
        if (hourExtractNumber > count) {
          hourExtractNumber = count.toInt
        }

        // 仍然通过模式匹配实现有则追加，无则新建
        hourExtractMap.get(hour) match {
          case None => hourExtractMap(hour) = new mutable.ListBuffer[Int]();
            // 根据数量随机生成下标
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt);
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt);
              }
              hourExtractMap(hour) += (extractIndex)
            }
          case Some(extractIndexList) =>
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt);
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt);
              }
              hourExtractMap(hour) += (extractIndex)
            }
        }
      }
    }

    // session随机抽取功能
    for ((date, hourCountMap) <- dateHourCountMap) {

      /*
       计算出这一天的session总数
       hourCountMap <HH,count>    hourCountMap.values就是当天每个小时的session数的集合
       */
      val sessionCount = hourCountMap.values.sum

      // dateHourExtractMap[天，[小时，产生的随机index的列表]]
      dateHourExtractMap.get(date) match {
        case None => dateHourExtractMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]();
          // 更新index
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCount)
        case Some(hourExtractMap) => hourExtractMapFunc(hourExtractMap, hourCountMap, sessionCount)
      }
    }

    /* 至此，index获取完毕 */

    /*
    将Map进行广播
    对于容量大且计算时每次计算都要使用的变量，一般进行广播，提高效率
        注册，使用
     */
    val dateHourExtractMapBroadcast = spark.sparkContext.broadcast(dateHourExtractMap)

    // time2sessionidRDD <yyyy-MM-dd_HH,aggrInfo>
    // 执行groupByKey算子，得到<yyyy-MM-dd_HH,(session aggrInfo)> 为了根据dateHourExtractMap从里面随机抽取
    val time2sessionsRDD = time2sessionidRDD.groupByKey()

    // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取,我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    val sessionRandomExtract = time2sessionsRDD.flatMap { case (dateHour, items) =>
      /*
      是为了从广播变量中获取当前小时要extract的index集合
       */
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      val dateHourExtractMap = dateHourExtractMapBroadcast.value
      val extractIndexList = dateHourExtractMap.get(date).get(hour)


      // index是在外部进行维护
      var index = 0
      /*
      根据业务需要，根据index抽取出的session只封装几个有用的属性SessionRandomExtract
       */
      val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
      /*
      开始遍历所有的aggrInfo，然后和extractIndexList中的index匹配，
      匹配到的，就放到sessionRandomExtractArray中，最终sessionRandomExtractArray就是全部抽取的数据
       */
      for (sessionAggrInfo <- items) {
        // 如果筛选List中包含当前的index，则提取此sessionAggrInfo中的数据
        if (extractIndexList.contains(index)) {
          val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          val starttime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
          val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
          sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
        }
        // index自增
        index += 1
      }
      sessionRandomExtractArray
    }

    /* 将抽取后的数据保存到MySQL */

    // 引入隐式转换，准备进行RDD向Dataframe的转换
    import spark.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    // 提取抽取出来的数据中的sessionId
    val extractSessionidsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取抽取出来的session的明细数据
    // 根据sessionId与详细数据进行聚合
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD)

    // 对extractSessionDetailRDD中的数据进行聚合，提炼有价值的明细数据
    val sessionDetailRDD = extractSessionDetailRDD.map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将明细数据保存到MySQL中
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def getTop10Session(spark: SparkSession, taskid: String, top10CategoryList: Array[(CategorySortKey, String)], sessionid2ActionRDD: RDD[(String, UserVisitAction)]) {

    // 第一步：将top10热门品类的id，生成一份RDD

    // 获得所有需要求的category集合
    val top10CategoryIdRDD = spark.sparkContext.makeRDD(top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
      (categoryid, categoryid)
    })

    // 第二步：计算top10品类被各session点击的次数

    // sessionid2ActionRDD是符合过滤(职业、年龄等)条件的完整数据
    // sessionid2detailRDD ( sessionId, userAction )
    val sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey()

    // 获取每个品类被每一个Session点击的次数
    val categoryid2sessionCountRDD = sessionid2ActionsRDD.flatMap { case (sessionid, userVisitActions) =>
      val categoryCountMap = new mutable.HashMap[Long, Long]()
      // userVisitActions中聚合了一个session的所有用户行为数据
      // 遍历userVisitActions是提取session中的每一个用户行为，并对每一个用户行为中的点击事件进行计数
      for (userVisitAction <- userVisitActions) {

        // 如果categoryCountMap中尚不存在此点击品类，则新增品类
        if (!categoryCountMap.contains(userVisitAction.click_category_id))
          categoryCountMap.put(userVisitAction.click_category_id, 0)

        // 如果categoryCountMap中已经存在此点击品类，则进行累加
        if (userVisitAction.click_category_id != null && userVisitAction.click_category_id != -1L) {
          categoryCountMap.update(userVisitAction.click_category_id, categoryCountMap(userVisitAction.click_category_id) + 1)
        }
      }

      // 对categoryCountMap中的数据进行格式转化
      for ((categoryid, count) <- categoryCountMap)
        yield (categoryid, sessionid + "," + count)

    }

    // 通过top10热门品类top10CategoryIdRDD与完整品类点击统计categoryid2sessionCountRDD进行join，仅获取热门品类的数据信息
    // 获取到to10热门品类，被各个session点击的次数【将数据集缩小】
    val top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).map { case (cid, (ccid, value)) => (cid, value) }

    // 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户

    // 先按照品类分组
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey()

    // 将每一个品类的所有点击排序，取前十个，并转换为对象
    val top10SessionObjectRDD = top10CategorySessionCountsRDD.flatMap { case (categoryid, clicks) =>
      // 先排序，然后取前10
      val top10Sessions = clicks.toList.sortWith(_.split(",")(1) > _.split(",")(1)).take(10)
      // 重新整理数据
      top10Sessions.map { case line =>
        val sessionid = line.split(",")(0)
        val count = line.split(",")(1).toLong
        Top10Session(taskid, categoryid, sessionid, count)
      }
    }

    // 将结果以追加方式写入到MySQL中
    import spark.implicits._
    top10SessionObjectRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    val top10SessionRDD = top10SessionObjectRDD.map(item => (item.sessionid, item.sessionid))

    // 第四步：获取top10活跃session的明细数据
    val sessionDetailRDD = top10SessionRDD.join(sessionid2ActionRDD).map { case (sid, (sessionid, userVisitAction)) =>
      SessionDetail(taskid, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    // 将活跃Session的明细数据，写入到MySQL
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_detail")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }


  /**
    * 业务需求三：获取top10热门品类
    *
    * @param spark
    * @param taskid
    * @param sessionid2detailRDD
    * @return
    */
  def getTop10Category(spark: SparkSession, taskid: String, sessionid2detailRDD: RDD[(String, UserVisitAction)]): Array[(CategorySortKey, String)] = {

    // 第一步：获取每一个Sessionid 点击过、下单过、支付过的数量

    // 获取所有产生过点击、下单、支付中任意行为的商品类别
    val categoryidRDD = sessionid2detailRDD.flatMap { case (sessionid, userVisitAction) =>
      val list = ArrayBuffer[(Long, Long)]()

      // 一个session中点击的商品ID
      if (userVisitAction.click_category_id != null) {
        list += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
      }
      // 一个session中下单的商品ID集合
      if (userVisitAction.order_category_ids != null) {
        if (!"".equals(userVisitAction.order_category_ids)) {
          for (orderCategoryId <- userVisitAction.order_category_ids.split(","))
            list += ((orderCategoryId.toLong, orderCategoryId.toLong))
        }

      }
      // 一个session中支付的商品ID集合
      if (userVisitAction.pay_category_ids != null && !"".equals(userVisitAction.pay_category_ids) ) {
        for (payCategoryId <- userVisitAction.pay_category_ids.split(","))
          list += ((payCategoryId.toLong, payCategoryId.toLong))
      }
      list
    }

    // 对重复的categoryid进行去重
    // 得到了所有被点击、下单、支付的商品的品类
    val distinctCategoryIdRDD = categoryidRDD.distinct

    // 第二步：计算各品类的点击、下单和支付的次数

    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的下单次数
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的支付次数
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD)

    // 第三步：join各品类与它的点击、下单和支付的次数
    // distinctCategoryIdRDD中是所有产生过点击、下单、支付行为的商品类别
    // 通过distinctCategoryIdRDD与各个统计数据的LeftJoin保证数据的完整性
    val categoryid2countRDD = joinCategoryAndData(distinctCategoryIdRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

    // 第四步：自定义二次排序key

    // 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
    // 创建用于二次排序的联合key —— (CategorySortKey(clickCount, orderCount, payCount), line)
    // 按照：点击次数 -> 下单次数 -> 支付次数 这一顺序进行二次排序
    val sortKey2countRDD = categoryid2countRDD.map { case (categoryid, line) =>
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong
      (CategorySortKey(clickCount, orderCount, payCount), line)
    }

    // 降序排序
    val sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false)

    // 第六步：用take(10)取出top10热门品类，并写入MySQL
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    val top10Category = top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong

      Top10Category(taskid, categoryid, clickCount, orderCount, payCount)
    }

    // 将Map结构转化为RDD
    val top10CategoryRDD = spark.sparkContext.makeRDD(top10Category)

    // 写入MySQL之前，将RDD转化为Dataframe
    import spark.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

    top10CategoryList
  }


  /**
    * 获取各品类点击次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 只将点击行为过滤出来
    val clickActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.click_category_id != null }

    // 获取每种类别的点击次数
    // map阶段：(品类ID，1L)
    val clickCategoryIdRDD = clickActionRDD.map { case (sessionid, userVisitAction) => (userVisitAction.click_category_id, 1L) }

    // 计算各个品类的点击次数
    // reduce阶段：对map阶段的数据进行汇总
    // (品类ID1，次数) (品类ID2，次数) (品类ID3，次数) ... ... (品类ID4，次数)
    clickCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 获取各品类的下单次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 过滤订单数据
    val orderActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.order_category_ids != null }

    // 获取每种类别的下单次数
    val orderCategoryIdRDD = orderActionRDD.flatMap {
      case (sessionid, userVisitAction) =>
        if (!"".equals(userVisitAction.order_category_ids)) {
          userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L))
        } else {
          userVisitAction.order_category_ids.map(item => (0L, 0L))
        }

    }

    // 计算各个品类的下单次数
    orderCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 获取各个品类的支付次数RDD
    *
    * @param sessionid2detailRDD
    * @return
    */
  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 过滤支付数据
    val payActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids != null }

    // 获取每种类别的支付次数
    val payCategoryIdRDD = payActionRDD.flatMap {
      case (sessionid, userVisitAction) =>
        if (!"".equals(userVisitAction.pay_category_ids)) {
          userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L))
        } else {
          userVisitAction.pay_category_ids.map(item => (0L, 0L))
        }
    }

    // 计算各个品类的支付次数
    payCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
    * 连接品类RDD与数据RDD
    *
    * @param categoryidRDD
    * @param clickCategoryId2CountRDD
    * @param orderCategoryId2CountRDD
    * @param payCategoryId2CountRDD
    * @return
    */
  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)], clickCategoryId2CountRDD: RDD[(Long, Long)], orderCategoryId2CountRDD: RDD[(Long, Long)], payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] = {

    // 将所有品类信息与点击次数信息结合【左连接】
    val clickJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map { case (categoryid, (cid, optionValue)) =>
      val clickCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
      (categoryid, value)
    }

    // 将所有品类信息与订单次数信息结合【左连接】
    val orderJoinRDD = clickJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val orderCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
      (categoryid, value)
    }

    // 将所有品类信息与付款次数信息结合【左连接】
    val payJoinRDD = orderJoinRDD.leftOuterJoin(payCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val payCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
      (categoryid, value)
    }

    payJoinRDD
  }
}

/*
 * process:
 *   extends AccumulatorV2[IN,OUT]
 *   设置存值成员变量
 *   重写6个方法
 *     isZero 判断是否初始化
 *     copy 复制一个当前accu的对象，返回值是对象
 *     reset 清零
 *     add 添加，为merge提供数据
 *     merge 设置聚合规则
 *     value 一般就是返回存值变量
 */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  var countMap = new mutable.HashMap[String, Int]

  override def isZero: Boolean = countMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    var accumulator = new SessionAccumulator()
    accumulator.countMap ++= this.countMap
    accumulator
  }

  override def reset(): Unit = countMap.clear()

  override def add(v: String): Unit = if (countMap.contains(v)) this.countMap.update(v, countMap(v) + 1) else this.countMap += v -> 1

  /*
  2个累加器的countMap之间进行计算
  如果key相同，就累加，如果key不同，就添加
  foldLeft函数如果是计算2个map时，是把参数中的map的每个tuple跟调用map整体做计算
   */
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other.asInstanceOf[SessionAccumulator].countMap.foldLeft(this.countMap) {
      case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
    }
  }

  override def value: mutable.HashMap[String, Int] = this.countMap
}



