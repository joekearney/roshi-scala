package libjoe.roshi

import com.redis.RedisClient
import java.util.concurrent.ThreadLocalRandom

/*
 * TODO
 * * scalacheck laws to test associative/commutative/idempotent?
 * * Should we be using scalaz laws to do some of this? I bet it comes with a bunch of useful stuff.
 */

/*
 * Domain classes. In the original Go implementation, these things are in a package each, but they have very
 * little actual implementation. This organisation keeps the structure separate from the actual implementation.
 */
final case class Pool[R <: LwwElementSet](index: String => R) {
  def withKey[T](key: String)(op: R => T): T = op(index(key))
}
final case class Cluster[R <: LwwElementSet](pool: Pool[R]) extends LwwElementSet {
  override def insert(ksm: KeyScoreMember): Boolean = pool.withKey(ksm.key)(_ insert ksm)
  override def delete(ksm: KeyScoreMember): Boolean = pool.withKey(ksm.key)(_ delete ksm)
  override def select(key: String) = pool.withKey(key)(_ select key)
}
final case class Farm[R <: LwwElementSet](clusters: IndexedSeq[Cluster[R]], strategy: FarmStrategy[R]) extends LwwElementSet {
  override def insert(ksm: KeyScoreMember) = strategy insert (ksm, clusters)
  override def delete(ksm: KeyScoreMember) = strategy delete (ksm, clusters)
  override def select(key: String) = strategy select (key, clusters)
}

case class KeyScoreMember(key: String, score: Double, member: String)
