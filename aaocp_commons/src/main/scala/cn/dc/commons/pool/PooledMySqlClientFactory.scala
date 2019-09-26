/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package cn.dc.commons.pool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import cn.dc.commons.conf.ConfigurationManager
import cn.dc.commons.constant.Constants
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

trait QueryCallback {
  def process(rs: ResultSet)
}

/**
  * MySQL客户端代理对象
  *
  * @param jdbcUrl      MySQL URL
  * @param jdbcUser     MySQL 用户
  * @param jdbcPassword MySQL 密码
  * @param client       默认客户端实现
  */
case class MySqlProxy(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None) {

  // 客户端连接对象
  private val mysqlClient = client getOrElse {
    DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
  }

  /**
    * 执行增删改SQL语句
    *
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null

    try {
      mysqlClient.setAutoCommit(false)
      pstmt = mysqlClient.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  /**
    * 执行查询SQL语句
    *
    * @param sql
    * @param params
    */
  def executeQuery(sql: String, params: Array[Any], queryCallback: QueryCallback) {
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      pstmt = mysqlClient.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }

      rs = pstmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  /**
    * 批量执行SQL语句
    *
    * @param sql
    * @param paramsList
    * @return 每条SQL语句影响的行数
    */
  def executeBatch(sql: String, paramsList: Array[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      // 第一步：使用Connection对象，取消自动提交
      mysqlClient.setAutoCommit(false)
      pstmt = mysqlClient.prepareStatement(sql)

      // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
      if (paramsList != null && paramsList.length > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }

      // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
      rtn = pstmt.executeBatch()

      // 最后一步：使用Connection对象，提交批量的SQL语句
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  // 关闭MySQL客户端
  def shutdown(): Unit = mysqlClient.close()
}

/**
  * 继承一个基础的连接池，需要提供池化的对象类型
  * @param jdbcUrl
  * @param jdbcUser
  * @param jdbcPassword
  * @param client
  */
class PooledMySqlClientFactory(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None) extends BasePooledObjectFactory[MySqlProxy] with Serializable {

  // 用于池来创建对象
  override def create(): MySqlProxy = MySqlProxy(jdbcUrl, jdbcUser, jdbcPassword, client)

  // 用于池来包装对象
  override def wrap(obj: MySqlProxy): PooledObject[MySqlProxy] = new DefaultPooledObject(obj)

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[MySqlProxy]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}

/**
  * 创建MySQL池工具类
  */
object CreateMySqlPool {

  // 加载JDBC驱动，只需要一次
  Class.forName("com.mysql.jdbc.Driver")

  private var genericObjectPool: GenericObjectPool[MySqlProxy] = null

  // 用于返回真正的对象池GenericObjectPool
  def apply(): GenericObjectPool[MySqlProxy] = {
    if (this.genericObjectPool == null) {
      this.synchronized {
        val jdbcUrl = ConfigurationManager.config.getString(Constants.JDBC_URL)
        val jdbcUser = ConfigurationManager.config.getString(Constants.JDBC_USER)
        val jdbcPassword = ConfigurationManager.config.getString(Constants.JDBC_PASSWORD)
        val size = ConfigurationManager.config.getInt(Constants.JDBC_DATASOURCE_SIZE)

        val pooledFactory = new PooledMySqlClientFactory(jdbcUrl, jdbcUser, jdbcPassword)
        val poolConfig = {
          val c = new GenericObjectPoolConfig
          c.setMaxTotal(size)
          c.setMaxIdle(size)
          c
        }
        //返回一个对象池
        this.genericObjectPool = new GenericObjectPool[MySqlProxy](pooledFactory, poolConfig)
      }
    }
    genericObjectPool
  }
}

