package libjoe.roshi

/**
 * Abstraction over a last-writer-wins element set.
 */
trait LwwElementSet {
  def insert(ksm: KeyScoreMember): Boolean
  def delete(ksm: KeyScoreMember): Boolean
  def select(key: String): List[KeyScoreMember]
}