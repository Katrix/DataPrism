package dataprism.sql

case class QueryResult[Res](rows: Seq[Res]) {

  def map[Res2](f: Res => Res2): QueryResult[Res2] = QueryResult(rows.map(f))

  private def checkLength(min: Int = -1, max: Int = -1): Unit = {
    val length = rows.length
    if min >= 0 then require(length >= min, s"Less than $min rows gotten")
    if max >= 0 then require(length <= max, s"More than $max rows gotten")
  }

  def one: Res = {
    checkLength(min = 1, max = 1)
    rows.head
  }

  def maybeOne: Option[Res] = {
    checkLength(min = 0, max = 1)
    rows.headOption
  }

  def checkNone(): Unit =
    checkLength(min = 0, max = 0)

  def asSeq: Seq[Res] = rows

  def oneOrMore: Seq[Res] = {
    checkLength(min = 1)
    rows
  }
}
