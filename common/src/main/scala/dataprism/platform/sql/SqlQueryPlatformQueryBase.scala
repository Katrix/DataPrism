package dataprism.platform.sql

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*
import perspective.derivation.ProductKPar

import scala.annotation.targetName

//noinspection ScalaUnusedSymbol
trait SqlQueryPlatformQueryBase extends SqlQueryPlatformBase, SqlQueryPlatformDbValueBase { platform =>

  case class QueryAstMetadata[A[_[_]]](ast: SelectAst[Codec], aliases: A[Const[String]], values: A[DbValue])

  trait LateralJoinCapability

  trait ExceptAllCapability
  trait ExceptCapability
  trait IntersectAllCapability
  trait IntersectCapability

  trait SqlQueryBase[A[_[_]]] extends QueryBase[A] {

    private[platform] def selectAstAndValues: TagState[QueryAstMetadata[A]]

    def selectAst: SelectAst[Codec] = selectAstAndValues.runA(freshTaggedState).value.ast

    def nested: Query[A]

    def distinct: Query[A]

    def join[B[_[_]]](that: Table[Codec, B])(
        on: InJoinConditionCapability ?=> (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[InnerJoin[A, B]] = this.join(Query.from(that))(on)

    def crossJoin[B[_[_]]](that: Table[Codec, B]): Query[InnerJoin[A, B]] =
      this.crossJoin(Query.from(that))

    def leftJoin[B[_[_]]](that: Table[Codec, B])(
        on: InJoinConditionCapability ?=> (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = this.leftJoin(Query.from(that))(on)

    def fullJoin[B[_[_]]](that: Table[Codec, B])(
        on: InJoinConditionCapability ?=> (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using FullJoinCapability): Query[FullJoin[A, B]] = this.fullJoin(Query.from(that))(on)

    def flatMap[B[_[_]]](f: A[DbValue] => Query[B])(using LateralJoinCapability): Query[B]

    inline def limit(n: Int): Query[A] = take(n)

    inline def offset(n: Int): Query[A] = drop(n)

    def union(that: Query[A]): Query[A]
    def unionAll(that: Query[A]): Query[A]

    def intersect(that: Query[A])(using IntersectCapability): Query[A]
    def intersectAll(that: Query[A])(using IntersectAllCapability): Query[A]

    def except(that: Query[A])(using ExceptCapability): Query[A]
    def exceptAll(that: Query[A])(using ExceptAllCapability): Query[A]

    // TODO: Ensure the type of this will always be Long
    def size: DbValue[Long] = this.map(_ => Query.queryCount).asDbValue

    inline def count: DbValue[Long] = this.size

    def nonEmpty: DbValue[Boolean] = size > 0.toLong.as(AnsiTypes.bigint)

    def isEmpty: DbValue[Boolean] = size === 0.toLong.as(AnsiTypes.bigint)

    def applyK: ApplyKC[A]

    given ApplyKC[A] = applyK

    def traverseK: TraverseKC[A]

    given TraverseKC[A] = traverseK
  }

  type Query[A[_[_]]] <: SqlQueryBase[A]

  type QueryCompanion <: SqlQueryCompanion
  trait SqlQueryCompanion:
    def from[A[_[_]]](table: Table[Codec, A]): Query[A]

    def queryCount: DbValue[Long]

    def ofK[A[_[_]]: ApplyKC: TraverseKC](value: A[DbValue]): Query[A]

    inline def of[A](value: A)(using MR: MapRes[DbValue, A]): Query[MR.K] =
      ofK(MR.toK(value))(using MR.applyKC, MR.traverseKC)

    def valuesK[A[_[_]]: ApplyKC: TraverseKC](types: A[Type], value: A[Id], values: A[Id]*): Query[A]

    inline def values[T](types: T)(using mr: MapRes[Type, T])(value: mr.K[Id], values: mr.K[Id]*): Query[mr.K] =
      valuesK(mr.toK(types), value, values*)(using mr.applyKC, mr.traverseKC)

    def valuesKBatch[A[_[_]]: ApplyKC: TraverseKC: DistributiveKC](
        types: A[Type],
        value: A[Id],
        values: A[Id]*
    ): Query[A]

    def valuesOf[A[_[_]]](table: Table[Codec, A], value: A[Id], values: A[Id]*): Query[A] =
      import table.given
      given FunctorKC[A] = table.FA
      Query.valuesK(table.columns.mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values *)

    def valuesOfBatch[A[_[_]]: DistributiveKC](table: Table[Codec, A], value: A[Id], values: A[Id]*): Query[A] =
      import table.given
      given FunctorKC[A] = table.FA
      Query.valuesKBatch(table.columns.mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values*)
  end SqlQueryCompanion

  extension [A](query: Query[[F[_]] =>> F[A]])
    // TODO: Make use of an implicit conversion here?
    @targetName("queryAsMany") def asMany: Many[A]

    @targetName("queryAsDbValue") def asDbValue: DbValue[A]

  type Api <: SqlQueryApi & QueryApi
  trait SqlQueryApi {
    export platform.{asDbValue, asMany}
  }
}
