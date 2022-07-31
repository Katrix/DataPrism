package dataprism.platform

import scala.annotation.targetName

import dataprism.sql.Table
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait DbPlatform {

  type DbValue[A]

  type OrdSeq
  type Ord <: OrdSeq

  extension (ordSeq: OrdSeq) def andThen(ord: Ord): OrdSeq

  type Grouped[A]
  type Many[A]

  extension [A](dbVal: DbValue[A])
    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean]
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean]

    def asc: Ord

    def desc: Ord

  extension [A](grouped: Grouped[A])
    def asMany: Many[A]
    def groupedBy: DbValue[A]

  type FullJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[F], B[F])
  type LeftJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[Compose2[F, Option]], B[F])
  type RightJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[F], B[Compose2[F, Option]])

  type Query[A[_[_]]]

  extension [A[_[_]]](query: Query[A])
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    def map[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B]
    inline def mapT[T <: NonEmptyTuple](f: A[DbValue] => T)(
        using ev: Tuple.IsMappedBy[DbValue][T]
    ): Query[ProductKPar[T]] =
      given ValueOf[Tuple.Size[T]] = new ValueOf(scala.compiletime.constValue[Tuple.Size[T]])

      map[ProductKPar[T]](f.andThen(t => ProductK.of[DbValue, T](t.asInstanceOf[Tuple.Map[T, DbValue]])))(
        using ProductK.productKInstance,
        ProductK.productKInstance
      )

    def join[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]]

    def leftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]]

    def rightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]]

    def groupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B]
    inline def groupByT[T <: NonEmptyTuple](f: A[Grouped] => T)(
        using ev: Tuple.IsMappedBy[DbValue][T]
    ): Query[ProductKPar[T]] =
      given ValueOf[Tuple.Size[T]] = new ValueOf(scala.compiletime.constValue[Tuple.Size[T]])

      groupBy[ProductKPar[T]](f.andThen(t => ProductK.of[DbValue, T](t.asInstanceOf[Tuple.Map[T, DbValue]])))(
        using ProductK.productKInstance,
        ProductK.productKInstance
      )

    def having(f: A[Grouped] => DbValue[Boolean]): Query[A]

    def orderBy(f: A[DbValue] => OrdSeq): Query[A]

    def limit(n: Int): Query[A]

    def offset(n: Int): Query[A]

  type QueryCompanion
  val Query: QueryCompanion

  extension (q: QueryCompanion) def from[A[_[_]]](table: Table[A])(using TraverseKC[A]): Query[A]
}
