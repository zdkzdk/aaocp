/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

/**
  * 广告黑名单
  * @author wuyufei
  *
  */
case class AdBlacklist(userid:Long)

/**
  * 用户广告点击量
  * @author wuyufei
  *
  */
case class AdUserClickCount(date:String,
                            userid:Long,
                            adid:Long,
                            clickCount:Long)


/**
  * 广告实时统计
  * @author wuyufei
  *
  */
case class AdStat(date:String,
                  province:String,
                  city:String,
                  adid:Long,
                  clickCount:Long)

/**
  * 各省top3热门广告
  * @author wuyufei
  *
  */
case class AdProvinceTop3(date:String,
                          province:String,
                          adid:Long,
                          clickCount:Long)

/**
  * 广告点击趋势
  * @author wuyufei
  *
  */
case class AdClickTrend(date:String,
                        hour:String,
                        minute:String,
                        adid:Long,
                        clickCount:Long)