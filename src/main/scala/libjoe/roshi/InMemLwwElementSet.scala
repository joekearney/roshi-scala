package libjoe.roshi

import java.util.TreeMap
import scala.collection.mutable
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * Pretend implementation of Redis sorted set. Useful for validating tests, and for modelling my own understanding of what's going on.
 * 
 * From the Redis documentation:
 * <blockquote>
 * ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 * </blockquote>
 */
final class InMemRedisSortedSet[T](implicit ordering: Ordering[T]) {
  import scala.collection.JavaConverters._

  private[this] val dataByScore = new TreeMap[Double, mutable.TreeSet[T]]().asScala
  private[this] val dataByValue: mutable.Map[T, Double] = mutable.HashMap()
  
  private[this] def remove(value: T, score: Option[Double]) = {
    val existingValues = score flatMap (dataByScore get _)
    // remove empty maps
    existingValues filter(_.isEmpty) foreach (_ => score map (dataByScore remove _))
  }
  def zadd(value: T, score: Double): Unit = {
    val oldScore = dataByValue get value
    remove(value, oldScore)
    
    dataByScore.getOrElseUpdate(score, mutable.TreeSet()).add(value)
    dataByValue update (value, score)
  }
  def zrem(value: T): Unit = {
    val oldScore = dataByValue remove value 
    remove(value, oldScore)
  }
  /** Gets the list of items and their scores, in no particular order */
  def zscan: Map[T, Double] = dataByValue.toMap
  /** Gets the list of items and their scores, sorted by score then value */
  def zrange: List[(T, Double)] = dataByScore.toList.flatMap(p => p._2.map((_, p._1)))
  def maxScore: Option[Double] = dataByScore.lastOption.map(_._1)
  
  override def toString() = dataByValue.toString
}

object InMemLwwElementSet {
  type SS = InMemRedisSortedSet[String]
  type AllSS = mutable.Map[String, SS]
  import scala.collection.JavaConverters._
  
  def write(positive: AllSS, negative: AllSS, monitor: AnyRef, ksm: KeyScoreMember): Boolean =
    monitor synchronized {
      val inserted = positive getOrElseUpdate(ksm.key, new SS)
      val deleted = negative getOrElseUpdate(ksm.key, new SS)
      def notTooLate(nm: SS) = nm.maxScore map (_ < ksm.score) getOrElse(true)
      val canProceed = notTooLate(inserted) && notTooLate(deleted)
      if (canProceed) {
        inserted.zadd(ksm.member, ksm.score)
        deleted.zrem(ksm.member)
        true
      } else false
    }
}
/**
 * Simple in-memory implementation of an LWW-element-set, using trees and hashmaps under the covers.
 */
class InMemLwwElementSet extends LwwElementSet {
  import InMemLwwElementSet._
  private[this] val inserts = mutable.Map[String, SS]()
  private[this] val deletes = mutable.Map[String, SS]()
  private[this] val monitor = new Object

  override def insert(ksm: KeyScoreMember): Boolean = write(inserts, deletes, monitor, ksm)
  override def delete(ksm: KeyScoreMember): Boolean = write(deletes, inserts, monitor, ksm)
  override def select(key: String): List[KeyScoreMember] = monitor synchronized {
    val inserted = inserts.get(key).map(_.zscan)
    val deleted = deletes.get(key).map(_.zscan)
    val f = for {
      l <- inserted
      d <- deleted
      ff = for {
    	(m, s) <- l
    	// if the deletion score is greater, then false
        if (d get m map (_ < s) getOrElse(true))
      } yield KeyScoreMember(key, s, m)
    } yield ff
    f map (_.toList) getOrElse(List())
  }
  override def toString() = (inserts.keys ++ deletes.keys) map { k =>
    s"$k+ ${inserts getOrElse(k, "")} $k- ${deletes getOrElse(k, "")}"
  } mkString ("\n")
}