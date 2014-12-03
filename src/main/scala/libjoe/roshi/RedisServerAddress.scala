package libjoe.roshi

import com.redis.RedisClient

case class RedisServerAddress(host: String, port: Int)

object RedisServerAddress {
  implicit def redisServerToClient(rs: RedisServerAddress): RedisClient = new RedisClient(rs.host, rs.port)
}