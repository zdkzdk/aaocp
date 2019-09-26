package cn.dc

import cn.dc.GenerateActionByArrayBuffer.mockUserInfo
import cn.dc.commons.model.UserInfo

object SendUserInfo {
  def main(args: Array[String]): Unit = {
    mockUserInfo().foreach {
      case userinfo: UserInfo => AnalyticsEngineSDK.onChargeSuccess("http://node6:83/userinfo", userinfo)
    }
  }
}
