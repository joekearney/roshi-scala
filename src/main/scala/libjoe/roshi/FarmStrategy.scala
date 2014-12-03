package libjoe.roshi

import java.util.concurrent.ThreadLocalRandom

sealed trait FarmStrategy[R <: LwwElementSet] {
  def insert(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]): Boolean
  def delete(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]): Boolean
  def select(key: String, clusters: IndexedSeq[Cluster[R]]): List[KeyScoreMember]
  
  protected def random[R <: LwwElementSet](clusters: IndexedSeq[Cluster[R]]) = clusters(ThreadLocalRandom.current nextInt(clusters size))
}
case class SendOneReadOne[R <: LwwElementSet]() extends FarmStrategy[R] {
  override def insert(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]) = random(clusters) insert ksm
  override def delete(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]) = random(clusters) delete ksm
  override def select(key: String, clusters: IndexedSeq[Cluster[R]]) = random(clusters) select key
}
case class SendAllReadAll[R <: LwwElementSet]() extends FarmStrategy[R] {
  override def insert(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]) = {
    val g = clusters map(_.insert(ksm)) groupBy(identity) mapValues (_.size)
    // did it succeed anywhere?
    g contains true
  }
  override def delete(ksm: KeyScoreMember, clusters: IndexedSeq[Cluster[R]]) = {
    val g = clusters map(_.delete(ksm)) groupBy(identity) mapValues (_.size)
    g contains true
  }
  override def select(key: String, clusters: IndexedSeq[Cluster[R]]) = {
    val g = clusters map(_.select(key)) groupBy(identity) mapValues (_.size)
    g.headOption.map(_._1).getOrElse(List())
  }
}