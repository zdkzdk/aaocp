package cn.dc

import cn.dc.GenerateActionByArrayBuffer.mockProductInfo
import cn.dc.commons.model.ProductInfo

object SendProduct {
  def main(args: Array[String]): Unit = {
    mockProductInfo().foreach {
      case product: ProductInfo => AnalyticsEngineSDK.onChargeSuccess("http://node6:82/product", product)
    }
  }
}
