/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package cn.dc.commons.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import net.sf.json.JSONObject
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable

/**
  * 日期时间工具类
  * 使用Joda实现，使用Java提供的Date会存在线程安全问题
  * 使用Joda实现，使用Java提供的Date会存在线程安全问题
  *
  */
object DateUtils {

  val TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd")
  val DATEKEY_FORMAT = DateTimeFormat.forPattern("yyyyMMdd")
  val DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm")

  /**
    * 判断一个时间是否在另一个时间之前
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1:String, time2:String):Boolean = {
    if(TIME_FORMAT.parseDateTime(time1).isBefore(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1:String, time2:String):Boolean = {
    if(TIME_FORMAT.parseDateTime(time1).isAfter(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
    * 计算时间差值（单位为秒）
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1:String, time2:String): Int = {
    return (TIME_FORMAT.parseDateTime(time1).getMillis - TIME_FORMAT.parseDateTime(time2).getMillis)/1000 toInt
  }

  /**
    * 获取年月日和小时
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果（yyyy-MM-dd_HH）
    */
  def getDateHour(datetime:String):String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  /**
    * 获取当天日期（yyyy-MM-dd）
    * @return 当天日期
    */
  def getTodayDate():String = {
    DateTime.now().toString(DATE_FORMAT)
  }

  /**
    * 获取昨天的日期（yyyy-MM-dd）
    * @return 昨天的日期
    */
  def getYesterdayDate():String = {
    DateTime.now().minusDays(1).toString(DATE_FORMAT)
  }

  /**
    * 格式化日期（yyyy-MM-dd）
    * @param date Date对象
    * @return 格式化后的日期
    */
  def formatDate(date:Date):String = {
    new DateTime(date).toString(DATE_FORMAT)
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    * @param date Date对象
    * @return 格式化后的时间
    */
  def formatTime(date:Date):String = {
    new DateTime(date).toString(TIME_FORMAT)
  }

  /**
    * 解析时间字符串
    * @param time 时间字符串
    * @return Date
    */
  def parseTime(time:String):Date = {
    TIME_FORMAT.parseDateTime(time).toDate
  }

  def main(args: Array[String]): Unit = {
    print(DateUtils.parseTime("2017-10-31 20:27:53"))
  }

  /**
    * 格式化日期key
    * @param date
    * @return
    */
  def formatDateKey(date:Date):String = {
    new DateTime(date).toString(DATEKEY_FORMAT)
  }

  /**
    * 格式化日期key
    * @return
    */
  def parseDateKey(datekey: String ):Date = {
    DATEKEY_FORMAT.parseDateTime(datekey).toDate
  }

  /**
    * 格式化时间，保留到分钟级别
    * yyyyMMddHHmm
    * @param date
    * @return
    */
  def formatTimeMinute(date: Date):String = {
    new DateTime(date).toString(DATE_TIME_FORMAT)
  }

}

/**
  * 数字格工具类
  *
  */
object NumberUtils {

  /**
    * 格式化小数
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num:Double, scale:Int):Double = {
    val bd = BigDecimal(num)
    bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

}


/**
  * 参数工具类
  *
  */
object ParamUtils {

  /**
    * 从JSON对象中提取参数
    * @param jsonObject JSON对象
    * @return 参数
    */
  def getParam(jsonObject:JSONObject, field:String):String = {
    jsonObject.getString(field)
    /*val jsonArray = jsonObject.getJSONArray(field)
    if(jsonArray != null && jsonArray.size() > 0) {
      return jsonArray.getString(0)
    }
    null*/
  }

}



/**
  * 字符串工具类
  *
  */
object StringUtils {

  /**
    * 判断字符串是否为空
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str:String):Boolean = {
    str == null || "".equals(str)
  }

  /**
    * 判断字符串是否不为空
    * @param str 字符串
    * @return 是否不为空
    */
  def isNotEmpty(str:String):Boolean = {
    str != null && !"".equals(str)
  }

  /**
    * 截断字符串两侧的逗号
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str:String):String = {
    var result = ""
    if(str.startsWith(",")) {
      result = str.substring(1)
    }
    if(str.endsWith(",")) {
      result = str.substring(0, str.length() - 1)
    }
    result
  }

  /**
    * 补全两位数字
    * @param str
    * @return
    */
  def fulfuill(str: String):String = {
    if(str.length() == 2) {
      str
    } else {
      "0" + str
    }
  }

  /**
    * 从拼接的字符串中提取字段
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段
    * @return 字段值
    */
  def getFieldFromConcatString(str:String, delimiter:String, field:String):String = {
    try {
      val fields = str.split(delimiter);
      for(concatField <- fields) {
        // searchKeywords=|clickCategoryIds=1,2,3
        if(concatField.split("=").length == 2) {
          val fieldName = concatField.split("=")(0)
          val fieldValue = concatField.split("=")(1)
          if(fieldName.equals(field)) {
            return fieldValue
          }
        }
      }
    } catch{
      case e:Exception => e.printStackTrace()
    }
    null
  }

  /**
    * 从拼接的字符串中给字段设置值
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field 字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str:String, delimiter:String, field:String, newFieldValue:String):String = {

    val fieldsMap = new mutable.HashMap[String,String]()

    for(fileds <- str.split(delimiter)){
      var arra = fileds.split("=")
      if(arra(0).compareTo(field) == 0)
        fieldsMap += (field -> newFieldValue)
      else
        fieldsMap += (arra(0) -> arra(1))
    }
    fieldsMap.map(item=> item._1 + "=" + item._2).mkString(delimiter)
  }

}


/**
  * 校验工具类
  *
  */
object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param startParamField 起始参数字段
    * @param endParamField 结束参数字段
    * @return 校验结果
    */
  def between(data:String, dataField:String, parameter:String, startParamField:String, endParamField:String):Boolean = {

    val startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if(startParamFieldStr == null || endParamFieldStr == null) {
      return true
    }

    val startParamFieldValue = startParamFieldStr.toInt
    val endParamFieldValue = endParamFieldStr.toInt

    val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldStr != null) {
      val dataFieldValue = dataFieldStr.toInt
      if(dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue) {
        return true
      } else {
        return false
      }
    }
    false
  }

  /**
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      return true
    }
    val paramFieldValueSplited = paramFieldValue.split(",")

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null && dataFieldValue != "-1") {
      val dataFieldValueSplited = dataFieldValue.split(",")

      for(singleDataFieldValue <- dataFieldValueSplited) {
        for(singleParamFieldValue <- paramFieldValueSplited) {
          if(singleDataFieldValue.compareTo(singleParamFieldValue) ==0) {
            return true
          }
        }
      }
    }
    false
  }

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def equal(data:String, dataField:String, parameter:String, paramField:String):Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if(paramFieldValue == null) {
      return true
    }

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if(dataFieldValue != null) {
      if(dataFieldValue.compareTo(paramFieldValue) == 0) {
        return true
      }
    }
    false
  }

}