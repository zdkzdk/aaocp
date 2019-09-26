import java.net.URLDecoder
import java.util.UUID

import org.apache.spark.sql.Row
import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.constant.Constants
import cn.dc.commons.model.UserVisitAction
import cn.dc.commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TTT {
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
    //起作用
    sparkSession.sparkContext.setLogLevel("OFF")
    /*
    1.勿忘
    2.sparkSession
     */
    /*
    进行数据筛选
     */
    /*
    初步根据设置的时间范围进行第一次数据清洗
      根据taskParam中设置的时间范围
     */
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    //URLDecoder.decode(action.getAs("action_time"),"UTF-8")
    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).map{
      case row: Row =>

        (row.get(1).toString,tran(row.get(2).toString),row.get(3).toString,tran(row.get(4).toString),URLDecoder.decode(row.get(5).toString,"UTF-8"),
          URLDecoder.decode(row.get(6).toString,"UTF-8"),tran(row.get(7).toString),tran(row.get(8).toString),row.get(9).toString,row.get(10).toString,row.get(11).toString,row.get(12).toString,tran(row.get(12).toString))
    }.toDF("date","user_id","session_id","page_id","action_time",
      "search_keyword","click_category_id","click_product_id","order_category_ids","order_product_ids","pay_category_ids","pay_product_ids","city_id")
      .as[UserVisitAction].show()
    def tran(input:String):Long = {
      if (input != "") return input.toLong else -1
    }


  }
}

