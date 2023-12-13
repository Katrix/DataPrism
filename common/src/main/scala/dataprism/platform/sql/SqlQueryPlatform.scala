package dataprism.platform.sql

import cats.data.State
import dataprism.platform.base.{MapRes, QueryPlatform}
import dataprism.sharedast.AstRenderer
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends QueryPlatform
    with SqlQueryPlatformDbValue
    with SqlQueryPlatformValueSource
    with SqlQueryPlatformQuery { platform =>

  type Type[_]
  def AnsiTypes: AnsiTypes[Type]

  val sqlRenderer: AstRenderer[Type]

  val Compile: Compile
  type Compile <: SqlCompile

  trait SqlCompile:
    protected def simple[A[_[_]]: ApplyKC: TraverseKC, B](types: A[Type])(f: A[DbValue] => B)(doReplacement: (B, Map[Object, Any]) => B): A[Id] => B =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] = types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues = tpesWithIdentifiers.mapK([Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue)
      val b = f(dbValues)

      (values: A[Id]) => {
        val replacements = values.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
        doReplacement(b, replacements)
      }

    def rawK[A[_[_]]: ApplyKC: TraverseKC](types: A[Type])(f: A[DbValue] => SqlStr[Type]): A[Id] => SqlStr[Type] =
      simple(types)(f)(_.compileWithValues(_))

    inline def raw[A](types: A)(using res: MapRes[Type, A])(f: res.K[DbValue] => SqlStr[Type]): res.K[Id] => SqlStr[Type] =
      rawK(res.toK(types))(f)(using res.applyKC, res.traverseKC)

  trait SqlTaggedState {
    def queryNum: Int

    def columnNum: Int

    def withNewQueryNum(newQueryNum: Int): TaggedState

    def withNewColumnNum(newColumnNum: Int): TaggedState
  }
  type TaggedState <: SqlTaggedState
  protected def freshTaggedState: TaggedState

  type TagState[A] = State[TaggedState, A]
}
