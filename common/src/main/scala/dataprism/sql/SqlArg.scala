package dataprism.sql

trait SqlArg[+Type[_]] {
  type A
  def value: A
  def tpe: Type[A]

  protected[dataprism] def compile(replacements: Map[Object, Any]): SqlArg.Aux[Type, A]
}
object SqlArg {
  type Aux[+Type[_], A0] = SqlArg[Type] {
    type A = A0
  }

  case class SqlArgObj[A0, Type[_]](value: A0, tpe: Type[A0]) extends SqlArg[Type] {
    type A = A0
    override protected[dataprism] def compile(replacements: Map[Object, Any]): Aux[Type, A0] = this
  }
  case class CompileArg[A0, Type[_]](identifier: Object, tpe: Type[A0]) extends SqlArg[Type] {
    override type A = A0
    override def value: A0 = throw new IllegalStateException(
      "Tried to get value of CompileArg before it has been substituted with an actual value"
    )

    override protected[dataprism] def compile(replacements: Map[Object, Any]): Aux[Type, A0] =
      replacements.get(identifier).fold(this)(v => SqlArgObj(v.asInstanceOf[A], tpe))
  }
}
