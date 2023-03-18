package dataprism.platform.base

import scala.annotation.targetName

import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

//noinspection ScalaUnusedSymbol
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

  type Many[A]

  type Nullable[A] = A match {
    case Option[b] => Option[b]
    case _         => Option[A]
  }

  type InnerJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[F], B[F])
  type LeftJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[F], B[Compose2[F, Nullable]])
  type RightJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[Compose2[F, Nullable]], B[F])
  type FullJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[Compose2[F, Nullable]], B[Compose2[F, Nullable]])

  type Query[A[_[_]]]

  type QueryGrouped[A[_[_]]] <: Query[A]

  extension [A[_[_]]](query: Query[A])
    @targetName("queryFilter") def filter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    @targetName("queryWithFilter") inline def withFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      filter(f)

    @targetName("queryWhere") def where(f: A[DbValue] => DbValue[Boolean]): Query[A]

    @targetName("queryMap") inline def map[R](f: A[DbValue] => R)(using res: MapRes[DbValue, R]): Query[res.K] =
      mapK(values => res.toK(f(values)))(using res.applyKC, res.traverseKC)

    @targetName("queryMapK") def mapK[B[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => B[DbValue]): Query[B]

    @targetName("queryFlatmap") def flatMap[B[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => Query[B]): Query[B]

    @targetName("queryJoin") def join[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]]

    @targetName("queryCrossJoin") def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]]

    @targetName("queryLeftJoin") def leftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]]

    @targetName("queryRightJoin") def rightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]]

    @targetName("queryFullJoin") def fullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]]

    @targetName("queryGroupMapK") def groupMapK[B[_[_]]: FoldableKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C]

    @targetName("queryGroupMap") inline def groupMap[B, C](group: A[DbValue] => B)(using MRB: MapRes[DbValue, B])(
        map: (B, A[Many]) => C
    )(using MRC: MapRes[DbValue, C]): QueryGrouped[MRC.K] =
      groupMapK(a => MRB.toK(group(a)))((a, b) => MRC.toK(map(MRB.fromK(a), b)))(using MRB.traverseKC, MRC.applyKC, MRC.traverseKC)

    @targetName("queryOrderBy") def orderBy(f: A[DbValue] => OrdSeq): Query[A]

    @targetName("queryLimit") def take(n: Int): Query[A]

    @targetName("queryOffset") def drop(n: Int): Query[A]
  end extension

  extension [A[_[_]]](query: QueryGrouped[A])

    @targetName("queryHaving") def having(f: A[DbValue] => DbValue[Boolean]): QueryGrouped[A]

  type QueryCompanion
  val Query: QueryCompanion
}
