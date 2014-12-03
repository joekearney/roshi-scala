package libjoe.roshi

import com.redis.RedisClient
import com.redis.RedisConnectionException
import scala.collection.immutable.SortedSet
import scala.annotation.tailrec
import com.redis.serialization.Parse

private object RedisLwwElementSet {
  /*
   * TODO bug in the redis client means that it can't handle returning numerics from scripts,
   * so we return strings and convert them on the client side. I submitted a pull request with
   * a first attempt at fixing it.
   */
  private[this] val SCRIPT_BASE = """
		local addKey = KEYS[1] .. 'ADD_SUFFIX'
		local remKey = KEYS[1] .. 'REM_SUFFIX'
		
		local maxSize = tonumber(ARGV[3])
		local atCapacity = tonumber(redis.call('ZCARD', addKey)) >= maxSize
		if atCapacity then
		        local oldestTs = redis.call('ZRANGE', addKey, 0, 0, 'WITHSCORES')[2]
		        if oldestTs and tonumber(ARGV[1]) < tonumber(oldestTs) then
		                return tostring(-1)
		        end
		end
		
		local insertTs = redis.call('ZSCORE', KEYS[1] .. 'INSERT_SUFFIX', ARGV[2])
		local deleteTs = redis.call('ZSCORE', KEYS[1] .. 'DELETE_SUFFIX', ARGV[2])
		if insertTs and tonumber(ARGV[1]) < tonumber(insertTs) then
		        return tostring(-1)
		elseif deleteTs and tonumber(ARGV[1]) <= tonumber(deleteTs) then
		        return tostring(-1)
		end
		
		redis.call('ZREM', remKey, ARGV[2])
		local n = redis.call('ZADD', addKey, ARGV[1], ARGV[2])
		redis.call('ZREMRANGEBYRANK', addKey, 0, -(maxSize+1))
		return tostring(n)
    """.replaceAll("INSERT_SUFFIX", "+").replaceAll("DELETE_SUFFIX", "-").trim

  private val md = java.security.MessageDigest.getInstance("SHA-1")
  private def sha1(string: String): String = md.digest(string.getBytes("UTF-8")).map("%02x".format(_)).mkString
  case class RedisScript(val text: String) {
    val hash: String = sha1(text)
  }
  val insertScript = RedisScript(SCRIPT_BASE.replaceAll("ADD_SUFFIX", "+").replaceAll("REM_SUFFIX", "-"))
  val deleteScript = RedisScript(SCRIPT_BASE.replaceAll("ADD_SUFFIX", "-").replaceAll("REM_SUFFIX", "+"))
  val id = RedisScript("""return "a"""")
}
/**
 * A
 */
class RedisLwwElementSet(private val redisConnection: RedisClient, maxSize: Int) extends LwwElementSet {
  import RedisClient._
  import com.redis.serialization._
  import RedisLwwElementSet._
  private def send(script: RedisScript, ksm: KeyScoreMember): Boolean = {
    val keys = List(ksm.key)
    val args = List(ksm.score, ksm.member, maxSize)

    def tryEval: Boolean = {
      try {
        val r = redisConnection evalSHA[String](script.hash, keys, args)
        r.map(_.toInt).getOrElse(-1) > 0
      } catch {
        case e: Exception if (e.getMessage startsWith "NOSCRIPT") => {
          val hash = redisConnection scriptLoad script.text
          tryEval
        }
      }
    }
    tryEval
  }
  override def insert(ksm: KeyScoreMember): Boolean = send(insertScript, ksm)
  override def delete(ksm: KeyScoreMember): Boolean = send(deleteScript, ksm)
  override def select(key: String): List[KeyScoreMember] = redisConnection.zrangeWithScore(key + "+", 0, Integer.MAX_VALUE, ASC)
    .map(_ map { case (m, s) => KeyScoreMember(key, s, m) })
    .getOrElse(List())
}
