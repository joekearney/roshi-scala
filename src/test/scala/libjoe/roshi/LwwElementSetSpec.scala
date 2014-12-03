package libjoe.roshi

import org.junit.Test
import org.scalatest._
import org.scalatest.junit.JUnitSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.redis.RedisClient

private object LwwElementSetSpec {
  val KEY = "key"
}

trait LwwElementSetSpec extends FlatSpec with Matchers {
  import LwwElementSetSpec._

  "An LwwElementSet when empty" should "be empty" in {
    assert(createFarm.select(KEY).isEmpty)
  }

  "An LwwElementSet" should "have an element present after adding" in {
    val farm = createFarm
    assert(farm insert (KeyScoreMember(KEY, 1.0, "a")))
    val items = farm.select(KEY) map (_.member)
    assert(items.size == 1)
    assert(items contains "a")
  }
  it should "not have an element present after adding and then removing" in {
    val farm = createFarm
    assert(farm insert (KeyScoreMember(KEY, 1.0, "a")))
    assert(farm delete (KeyScoreMember(KEY, 2.0, "a")))
    val items = farm.select(KEY) map (_.member)
    assert(items.isEmpty)
  }
  it should "have an element present after adding and then seeing an earlier removal" in {
    val farm = createFarm
    assert(farm insert (KeyScoreMember(KEY, 3.0, "a")))
    farm.delete(KeyScoreMember(KEY, 2.0, "a"))
    val items = farm.select(KEY) map (_.member)
    assert(items.size == 1)
    assert(items contains "a")
  }

  def create(index: Int): LwwElementSet
  private[this] def createFarm = {
    val r1 = create(0)
    val r2 = create(1)
    val c1 = new Cluster(new Pool(_ => r1))
    val c2 = new Cluster(new Pool(_ => r2))
    new Farm(Vector(c1), SendAllReadAll())
  }
}

@RunWith(classOf[JUnitRunner])
class InMemLwwElementSetSpec extends LwwElementSetSpec {
  def create(index: Int) = new InMemLwwElementSet
}
@RunWith(classOf[JUnitRunner])
class RedisLwwElementSetSpec extends LwwElementSetSpec {
  import RedisServerAddress._
  val connections: IndexedSeq[RedisClient] = IndexedSeq(TestServers.REDIS_1, TestServers.REDIS_2)
  def create(index: Int) = new RedisLwwElementSet(connections(index), 100)

  override def withFixture(test: NoArgTest) = {
    connections foreach (c => c.flushdb)
    try super.withFixture(test) // Invoke the test function
    finally {
      connections foreach (c => c.flushdb)
    }
  }
}