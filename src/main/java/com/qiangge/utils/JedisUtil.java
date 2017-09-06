package com.qiangge.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by zhazha on 2017/1/3.
 */
public class JedisUtil {

	private static JedisPool pool = null;
	private static Logger logger = Logger.getLogger(JedisUtil.class);

	/**
	 * 构建redis连接池
	 * @return
	 */
	static {

		JedisPoolConfig config = new JedisPoolConfig();
		//控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取

		//如果赋值为-1表示不受限制；如果pool已经分配了maxActive实例，则此时pool的状态为exhausted（耗尽）
		config.setMaxTotal(CommonConstant.MAX_TOTAL);
		//控制一个pool最多有多少个状态为idle（空闲）的jedis实例
		config.setMaxIdle(5);
		//borrow一个jedis实例时，最大等待时间，如果超过等待时间，则直接抛出JedisConnectException
		config.setMaxWaitMillis(CommonConstant.MAX_WAIT);
		//在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		config.setTestOnBorrow(true);
		pool = new JedisPool(config, CommonConstant.REDIS_URL, CommonConstant.PORT);
	}

	//获取连接池
	public static JedisPool getPool() {
		return pool;
	}

	//释放连接
	public static void returnResource(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

	//获取jedis对象
	public static Jedis getJedis() {
		return pool.getResource();
	}


	public static void main(String[] args) {
		Jedis jedis = getJedis();
		jedis.setex("a", 10, "20");
		//for (int i = 0; i < 10000; i++) {
			jedis.incr("abc");

		//}


		System.out.println(jedis.ttl("a"));

		System.out.println(jedis.get("abc"));

	}

}
