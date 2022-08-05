package dataprism.platform.base

import scala.annotation.targetName

import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait QueryPlatform {

  type DbValue[A]

  type OrdSeq
  type Ord <: OrdSeq

  extension (ordSeq: OrdSeq) @targetName("ordSeqAndThen") def andThen(ord: Ord): OrdSeq

  extension [A](dbVal: DbValue[A])
    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean]
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean]

    @targetName("dbValAsc") def asc: Ord

    @targetName("dbValDesc") def desc: Ord

  type Grouped[A]
  type Many[A]

  extension [A](grouped: Grouped[A])
    @targetName("groupedAsMany") def asMany: Many[A]
    @targetName("groupedGroupedBy") def groupedBy: DbValue[A]

  type InnerJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[F], B[F])
  type LeftJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[Compose2[F, Option]], B[F])
  type RightJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[F], B[Compose2[F, Option]])
  type FullJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[Compose2[F, Option]], B[Compose2[F, Option]])

  type Query[A[_[_]]]

  extension [A[_[_]]](query: Query[A])
    @targetName("queryFilter") def filter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    @targetName("queryWithFilter") inline def withFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      filter(f)

    @targetName("queryMap") inline def map[R](f: A[DbValue] => R)(using res: MapRes[DbValue, R]): Query[res.K] =
      mapK(values => res.toK(f(values)))(using res.applicativeKC, res.traverseKC)

    @targetName("queryMapK") def mapK[B[_[_]]](
        f: A[DbValue] => B[DbValue]
    )(using ApplicativeKC[B], TraverseKC[B]): Query[B]

    @targetName("queryMapT") inline def mapT[T <: NonEmptyTuple](f: A[DbValue] => T)(
        using ev: Tuple.IsMappedBy[DbValue][T]
    ): Query[ProductKPar[Tuple.InverseMap[T, DbValue]]] =
      given ValueOf[Tuple.Size[Tuple.InverseMap[T, DbValue]]] = new ValueOf(
        scala.compiletime.constValue[Tuple.Size[Tuple.InverseMap[T, DbValue]]]
      )

      mapK[ProductKPar[Tuple.InverseMap[T, DbValue]]](
        f.andThen(t =>
          ProductK.of[DbValue, Tuple.InverseMap[T, DbValue]](
            t
          )
        )
      )(
        using ProductK.productKInstance,
        ProductK.productKInstance
      )

    @targetName("queryFlatmap") def flatMap[B[_[_]]](
        f: A[DbValue] => Query[B]
    )(using ApplicativeKC[B], TraverseKC[B]): Query[B]

    @targetName("queryJoin") def join[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]]

    @targetName("queryJoin") def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]]

    @targetName("queryLeftJoin") def leftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]]

    @targetName("queryRightJoin") def rightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]]

    @targetName("queryFullJoin") def fullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]]

    @targetName("queryGroupBy") def groupByK[B[_[_]]](
        f: A[Grouped] => B[DbValue]
    )(using ApplicativeKC[B], TraverseKC[B]): Query[B]

    @targetName("queryGroupByT") inline def groupByT[T <: NonEmptyTuple](f: A[Grouped] => T)(
        using ev: Tuple.IsMappedBy[DbValue][T]
    ): Query[ProductKPar[T]] =
      given ValueOf[Tuple.Size[T]] = new ValueOf(scala.compiletime.constValue[Tuple.Size[T]])

      groupByK[ProductKPar[T]](f.andThen(t => ProductK.of[DbValue, T](t.asInstanceOf[Tuple.Map[T, DbValue]])))(
        using ProductK.productKInstance,
        ProductK.productKInstance
      )

    @targetName("queryHaving") def having(f: A[Grouped] => DbValue[Boolean]): Query[A]

    @targetName("queryOrderBy") def orderBy(f: A[DbValue] => OrdSeq): Query[A]

    @targetName("queryLimit") def take(n: Int): Query[A]

    @targetName("queryOffset") def drop(n: Int): Query[A]

  type QueryCompanion
  val Query: QueryCompanion
}
