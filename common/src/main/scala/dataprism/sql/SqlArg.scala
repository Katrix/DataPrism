package dataprism.sql

trait SqlArg[+Codec[_]] {
  type A
  def value(batch: Int): A
  def batchSize: Int
  def tpe: Codec[A]

  protected[dataprism] def compile(replacements: Map[Object, Seq[Any]]): SqlArg.Aux[Codec, A]
}
object SqlArg {
  type Aux[+Codec[_], A0] = SqlArg[Codec] {
    type A = A0
  }

  case class SqlArgObj[A0, Codec[_]](valueSeq: Seq[A0], tpe: Codec[A0]) extends SqlArg[Codec] {
    type A = A0
    override def batchSize: Int        = valueSeq.length
    override def value(batch: Int): A0 = valueSeq(batch)

    override protected[dataprism] def compile(replacements: Map[Object, Seq[Any]]): Aux[Codec, A0] = this
  }
  case class CompileArg[A0, Codec[_]](identifier: Object, tpe: Codec[A0]) extends SqlArg[Codec] {
    override type A = A0
    override def value(batch: Int): A0 = throw new IllegalStateException(
      "Tried to get value of CompileArg before it has been substituted with an actual value"
    )
    override def batchSize: Int = throw new IllegalStateException(
      "Tried to get batchSize of CompileArg before it has been substituted with an actual value"
    )

    override protected[dataprism] def compile(replacements: Map[Object, Seq[Any]]): Aux[Codec, A0] =
      replacements.get(identifier).fold(this)(v => SqlArgObj(v.asInstanceOf[Seq[A]], tpe))
  }
}
