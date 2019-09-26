package cn.dc

import cn.dc.commons.model.UserVisitAction

object SendAction {
  def main(args: Array[String]): Unit = {
    GenerateActionByArrayBuffer.mockUserVisitActionData().foreach {
      case action: UserVisitAction => AnalyticsEngineSDK.onChargeSuccess("http://node6:81/action", action)
    }
  }
}
