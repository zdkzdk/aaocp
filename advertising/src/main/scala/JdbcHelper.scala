import java.sql.ResultSet

import cn.dc.commons.pool.{CreateMySqlPool, QueryCallback}

import scala.collection.mutable.ArrayBuffer

/**
  * 用户黑名单DAO类
  */
object AdBlacklistDAO {

  /**
    * 批量插入广告黑名单用户
    *
    * @param adBlacklists
    */
  def insertBatch(adBlacklists: Array[Long]) {
    // 批量插入
    val sql = "INSERT IGNORE INTO ad_blacklist VALUES(?) "



    val paramsList = new ArrayBuffer[Array[Any]]()

    // 向paramsList添加userId
    for (adBlacklist <- adBlacklists) {
      val params: Array[Any] = Array(adBlacklist)
      paramsList += params
    }
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 执行批量插入操作
    client.executeBatch(sql, paramsList.toArray)
    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

  /**
    * 查询所有广告黑名单用户
    *
    * @return
    */
  def findAll(): Array[AdBlacklist] = {
    // 将黑名单中的所有数据查询出来
    val sql = "SELECT * FROM ad_blacklist"

    val adBlacklists = new ArrayBuffer[AdBlacklist]()

    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 执行sql查询并且通过处理函数将所有的userid加入array中
    client.executeQuery(sql, null, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          val userid = rs.getInt(1).toLong
          adBlacklists += AdBlacklist(userid)
        }
      }
    })

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
    adBlacklists.toArray
  }
}


/**
  * 用户广告点击量DAO实现类
  *
  */
object AdUserClickCountDAO {

  def updateBatch(adUserClickCounts: Array[AdUserClickCount]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 首先对用户广告点击量进行分类，分成待插入的和待更新的
    val insertAdUserClickCounts = ArrayBuffer[AdUserClickCount]()
    val updateAdUserClickCounts = ArrayBuffer[AdUserClickCount]()

    val selectSQL = "SELECT count(*) FROM ad_user_click_count WHERE date=? AND userid=? AND adid=? "

    for (adUserClickCount <- adUserClickCounts) {

      val selectParams: Array[Any] = Array(adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
      // 根据传入的用户点击次数统计数据从已有的ad_user_click_count中进行查询
      client.executeQuery(selectSQL, selectParams, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          // 如果能查询到并且点击次数大于0，则认为是待更新项
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdUserClickCounts += adUserClickCount
          } else {
            insertAdUserClickCounts += adUserClickCount
          }
        }
      })
    }

    // 执行批量插入
    val insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    // 将待插入项全部加入到参数列表中
    for (adUserClickCount <- insertAdUserClickCounts) {
      insertParamsList += Array[Any](adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid, adUserClickCount.clickCount)
    }

    // 执行批量插入
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 执行批量更新
    // clickCount=clickCount + ：此处的UPDATE是进行累加
    val updateSQL = "UPDATE ad_user_click_count SET clickCount=clickCount + ? WHERE date=? AND userid=? AND adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    // 将待更新项全部加入到参数列表中
    for (adUserClickCount <- updateAdUserClickCounts) {
      updateParamsList += Array[Any](adUserClickCount.clickCount, adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
    }

    // 执行批量更新
    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

  /**
    * 根据多个key查询用户广告点击量
    *
    * @param date   日期
    * @param userid 用户id
    * @param adid   广告id
    * @return
    */
  def findClickCountByMultiKey(date: String, userid: Long, adid: Long): Int = {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    val sql = "SELECT clickCount FROM ad_user_click_count " +
      "WHERE date=? " +
      "AND userid=? " +
      "AND adid=?"

    var clickCount = 0
    val params = Array[Any](date, userid, adid)

    // 根据多个条件查询指定用户的点击量，将查询结果累加到clickCount中
    client.executeQuery(sql, params, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        if (rs.next()) {
          clickCount = rs.getInt(1)
        }
      }
    })
    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
    clickCount
  }
}


/**
  * 广告实时统计DAO实现类
  *
  */
object AdStatDAO {

  def updateBatch(adStats: Array[AdStat]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()


    // 区分开来哪些是要插入的，哪些是要更新的
    val insertAdStats = ArrayBuffer[AdStat]()
    val updateAdStats = ArrayBuffer[AdStat]()

    val selectSQL = "SELECT count(*) " +
      "FROM ad_stat " +
      "WHERE date=? " +
      "AND province=? " +
      "AND city=? " +
      "AND adid=?"

    for (adStat <- adStats) {

      val params = Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid)
      // 通过查询结果判断当前项时待插入还是待更新
      client.executeQuery(selectSQL, params, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdStats += adStat
          } else {
            insertAdStats += adStat
          }
        }
      })
    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adStat <- insertAdStats) {
      insertParamsList += Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid, adStat.clickCount)
    }

    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
    val updateSQL = "UPDATE ad_stat SET clickCount=? " +
      "WHERE date=? " +
      "AND province=? " +
      "AND city=? " +
      "AND adid=?"

    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adStat <- updateAdStats) {
      updateParamsList += Array[Any](adStat.clickCount, adStat.date, adStat.province, adStat.city, adStat.adid)
    }

    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

}


/**
  * 各省份top3热门广告DAO实现类
  *
  */
object AdProvinceTop3DAO {

  def updateBatch(adProvinceTop3s: Array[AdProvinceTop3]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // dateProvinces可以实现一次去重
    // AdProvinceTop3：date province adid clickCount，由于每条数据由date province adid组成
    // 当只取date province时，一定会有重复的情况
    val dateProvinces = ArrayBuffer[String]()

    for (adProvinceTop3 <- adProvinceTop3s) {
      // 组合新key
      val key = adProvinceTop3.date + "_" + adProvinceTop3.province

      // dateProvinces中不包含当前key才添加
      // 借此去重
      if (!dateProvinces.contains(key)) {
        dateProvinces += key
      }
    }

    // 根据去重后的date和province，进行批量删除操作
    // 先将原来的数据全部删除
    val deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?"

    val deleteParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (dateProvince <- dateProvinces) {

      val dateProvinceSplited = dateProvince.split("_")
      val date = dateProvinceSplited(0)
      val province = dateProvinceSplited(1)

      val params = Array[Any](date, province)
      deleteParamsList += params
    }

    client.executeBatch(deleteSQL, deleteParamsList.toArray)

    // 批量插入传入进来的所有数据
    val insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    // 将传入的数据转化为参数列表
    for (adProvinceTop3 <- adProvinceTop3s) {
      insertParamsList += Array[Any](adProvinceTop3.date, adProvinceTop3.province, adProvinceTop3.adid, adProvinceTop3.clickCount)
    }

    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

}


/**
  * 广告点击趋势DAO实现类
  *
  */
object AdClickTrendDAO {

  def updateBatch(adClickTrends: Array[AdClickTrend]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()

    // 区分开来哪些是要插入的，哪些是要更新的
    val updateAdClickTrends = ArrayBuffer[AdClickTrend]()
    val insertAdClickTrends = ArrayBuffer[AdClickTrend]()

    val selectSQL = "SELECT count(*) " +
      "FROM ad_click_trend " +
      "WHERE date=? " +
      "AND hour=? " +
      "AND minute=? " +
      "AND adid=?"

    for (adClickTrend <- adClickTrends) {
      // 通过查询结果判断当前项时待插入还是待更新
      val params = Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
      client.executeQuery(selectSQL, params, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateAdClickTrends += adClickTrend
          } else {
            insertAdClickTrends += adClickTrend
          }
        }
      })

    }

    // 执行批量更新操作
    // 此处的UPDATE是覆盖
    val updateSQL = "UPDATE ad_click_trend SET clickCount=? " +
      "WHERE date=? " +
      "AND hour=? " +
      "AND minute=? " +
      "AND adid=?"

    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adClickTrend <- updateAdClickTrends) {
      updateParamsList += Array[Any](adClickTrend.clickCount, adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
    }

    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 执行批量更新操作
    val insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adClickTrend <- insertAdClickTrends) {
      insertParamsList += Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid, adClickTrend.clickCount)
    }

    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

}

