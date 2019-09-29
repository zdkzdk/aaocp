package cn.dc;

import cn.dc.commons.model.ProductInfo;
import cn.dc.commons.model.UserInfo;
import cn.dc.commons.model.UserVisitAction;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 分析引擎sdk java服务器端数据收集
 * 
 * @author root
 * @version 1.0
 *
 */
public class AnalyticsEngineSDK {
	// 日志打印对象
	private static final Logger log = Logger.getGlobal();
	// 请求url的主体部分


	/**
	 * 触发订单支付成功事件，发送事件数据到服务器
	 *  * @param date               用户点击行为的日期
	 *   * @param user_id            用户的ID
	 *   * @param session_id         Session的ID
	 *   * @param page_id            某个页面的ID
	 *   * @param action_time        点击行为的时间点
	 *   * @param search_keyword     用户搜索的关键词
	 *   * @param click_category_id  某一个商品品类的ID
	 *   * @param click_product_id   某一个商品的ID
	 *   * @param order_category_ids 一次订单中所有品类的ID集合
	 *   * @param order_product_ids  一次订单中所有商品的ID集合
	 *   * @param pay_category_ids   一次支付中所有品类的ID集合
	 *   * @param pay_product_ids    一次支付中所有商品的ID集合
	 *   * @param city_id            城市ID
	 * @return 如果发送数据成功(加入到发送队列中)，那么返回true；否则返回false(参数异常&添加到发送队列失败).
	 */
	public static boolean onChargeSuccess(String accessUrl, UserVisitAction action) {
		try {
			// 代码执行到这儿，表示订单id和会员id都不为空。
			Map<String, String> data = new HashMap<>();
			data.put("date", action.date());
			data.put("user_id", String.valueOf(action.user_id()));
			data.put("session_id", action.session_id());
			data.put("page_id",String.valueOf(action.page_id()));
			data.put("action_time", action.action_time());
			data.put("search_keyword", action.search_keyword());
			data.put("click_category_id", String.valueOf(action.click_category_id()));
			data.put("click_product_id", String.valueOf(action.click_product_id()));
			data.put("order_category_ids", action.order_category_ids());
			data.put("order_product_ids", action.order_product_ids());
			data.put("pay_category_ids", action.pay_category_ids());
			data.put("pay_product_ids", action.pay_product_ids());
			data.put("city_id", String.valueOf(action.city_id()));

			// 创建url
			String url = buildUrl(accessUrl,data);
			// 发送url&将url加入到队列
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}
	/*
	*   * @param product_id   商品的ID
	 * @param product_name 商品的名称
	 * @param extend_info  商品额外的信息
	* */
	public static boolean onChargeSuccess(String accessUrl, ProductInfo product) {
		try {
			// 代码执行到这儿，表示订单id和会员id都不为空。
			Map<String, String> data = new HashMap<>();
			data.put("product_id", String.valueOf(product.product_id()));
			data.put("product_name", String.valueOf(product.product_name()));
			data.put("extend_info", String.valueOf(product.extend_info()));

			// 创建url
			String url = buildUrl(accessUrl,data);
			// 发送url&将url加入到队列
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}
	/*
	 * @param user_id      用户的ID
	 * @param username     用户的名称
	 * @param name         用户的名字
	 * @param age          用户的年龄
	 * @param professional 用户的职业
	 * @param city         用户所在的城市
	 * @param sex          用户的性别
	 * */
	public static boolean onChargeSuccess(String accessUrl, UserInfo userInfo) {
		try {
			// 代码执行到这儿，表示订单id和会员id都不为空。
			Map<String, String> data = new HashMap<>();
			data.put("user_id", String.valueOf(userInfo.user_id()));
			data.put("username", String.valueOf(userInfo.username()));
			data.put("name", String.valueOf(userInfo.name()));
			data.put("age", String.valueOf(userInfo.age()));
			data.put("professional", String.valueOf(userInfo.professional()));
			data.put("city", String.valueOf(userInfo.city()));
			data.put("sex", String.valueOf(userInfo.sex()));

			// 创建url
			String url = buildUrl(accessUrl,data);
			// 发送url&将url加入到队列
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}

	/**
	 * 触发订单退款事件，发送退款数据到服务器
	 * 

	 *            退款会员id
	 * @return 如果发送数据成功，返回true。否则返回false。
	 */
	public static boolean onChargeRefund(String accessUrl) {
		try {

			// 代码执行到这儿，表示订单id和会员id都不为空。
			Map<String, String> data = new HashMap<String, String>();

			data.put("c_time", String.valueOf(System.currentTimeMillis()));

			data.put("en", "e_cr");

			// 构建url
			String url = buildUrl(accessUrl,data);
			// 发送url&将url添加到队列中
			SendDataMonitor.addSendUrl(url);
			return true;
		} catch (Throwable e) {
			log.log(Level.WARNING, "发送数据异常", e);
		}
		return false;
	}

	/**
	 * 根据传入的参数构建url
	 *
	 */
	private static String buildUrl(String accessUrl,Map<String, String> data)
			throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		sb.append(accessUrl).append("?");
		for (Map.Entry<String, String> entry : data.entrySet()) {
			if (isNotEmpty(entry.getKey()) && isNotEmpty(entry.getValue())) {
				sb.append(entry.getKey().trim())
						.append("=")
						.append(URLEncoder.encode(entry.getValue().trim(), "utf-8"))
						.append("&");
			}
		}
		return sb.substring(0, sb.length() - 1);// 去掉最后&
	}

	/**
	 * 判断字符串是否为空，如果为空，返回true。否则返回false。
	 * 
	 * @param value
	 * @return
	 */
	private static boolean isEmpty(String value) {
		return value == null || value.trim().isEmpty();
	}

	/**
	 * 判断字符串是否非空，如果不是空，返回true。如果是空，返回false。
	 * 
	 * @param value
	 * @return
	 */
	private static boolean isNotEmpty(String value) {
		return !isEmpty(value);
	}
}
