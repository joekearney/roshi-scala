package libjoe.roshi

object TestServers {
  import libjoe.roshi.RedisServerAddress._
  val REDIS_1 = RedisServerAddress("127.0.0.1", 6381)
  val REDIS_2 = RedisServerAddress("127.0.0.1", 6382)
}