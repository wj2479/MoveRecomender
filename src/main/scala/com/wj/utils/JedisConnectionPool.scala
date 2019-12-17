package com.wj.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  val config = new JedisPoolConfig
  // 设置最大连接数
  config.setMaxTotal(20)
  // 设置最大空闲数
  config.setMaxIdle(10)
  // 当调用 borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(true)
  // 超时时间
  val pool = new JedisPool(config, "192.168.1.101", 6379, 10000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
