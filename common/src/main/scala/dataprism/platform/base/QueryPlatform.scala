package dataprism.platform.base

import scala.annotation.targetName

import cats.Applicative
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

//noinspection ScalaUnusedSymbol
trait QueryPlatform {

  trait Lift[A, B]:
    extension (a: A) def lift: B

  type Nullability[A] <: NullabilityBase[A]
  trait NullabilityBase[A]:
    type NNA
    type N[_]

  trait DbValueBase[A]:
    def liftDbValue: DbValue[A]

    @targetName("dbEquals") def ===(that: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]
    @targetName("dbNotEquals") def !==(that: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]

    def asc: Ord
    def desc: Ord

  type DbValue[A] <: DbValueBase[A]

  trait OrdSeqBase:
    def andThen(ord: Ord): OrdSeq

  type OrdSeq <: OrdSeqBase
  type Ord <: OrdSeq

  type Many[A]

  type Nullable[A] <: Option[_] = A match {
    case Option[b] => Option[b]
    case _         => Option[A]
  }

  type InnerJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[F], B[F])
  type LeftJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[F], B[Compose2[F, Nullable]])
  type RightJoin[A[_[_]], B[_[_]]] = [F[_]] =>> (A[Compose2[F, Nullable]], B[F])
  type FullJoin[A[_[_]], B[_[_]]]  = [F[_]] =>> (A[Compose2[F, Nullable]], B[Compose2[F, Nullable]])

  trait QueryBase[A[_[_]]]:
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    inline def withFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      filter(f)

    def where(f: A[DbValue] => DbValue[Boolean]): Query[A]

    inline def map[R](f: A[DbValue] => R)(using res: MapRes[DbValue, R]): Query[res.K] =
      mapK(values => res.toK(f(values)))(using res.applyKC, res.traverseKC)

    def mapK[B[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => B[DbValue]): Query[B]

    def flatMap[B[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => Query[B]): Query[B]

    def join[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[InnerJoin[A, B]]

    def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]]

    def leftJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[LeftJoin[A, B]]

    def rightJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[RightJoin[A, B]]

    def fullJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[FullJoin[A, B]]

    def groupMapK[B[_[_]]: TraverseKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C]

    inline def groupMap[B, C](group: A[DbValue] => B)(using MRB: MapRes[DbValue, B])(
        map: (B, A[Many]) => C
    )(using MRC: MapRes[DbValue, C]): QueryGrouped[MRC.K] =
      groupMapK(a => MRB.toK(group(a)))((a, b) => MRC.toK(map(MRB.fromK(a), b)))(
        using MRB.traverseKC,
        MRC.applyKC,
        MRC.traverseKC
      )

    def mapSingleGroupedK[B[_[_]]: ApplyKC: TraverseKC](
        f: A[Many] => B[DbValue]
    ): QueryGrouped[B] =
      given TraverseKC[[F[_]] =>> Unit] with {
        extension [X[__], C](fa: Unit)
          def foldLeftK[Y](b: Y)(f: Y => X :~>#: Y): Y    = b
          def foldRightK[Y](b: Y)(f: X :~>#: (Y => Y)): Y = b

        extension [X[_], C](fa: Unit)
          def traverseK[G[_]: Applicative, Y[_]](f: X :~>: Compose2[G, Y]): G[Unit] = summon[Applicative[G]].unit
      }
      groupMapK[[F[_]] =>> Unit, B](_ => ())((_, a) => f(a))

    inline def mapSingleGrouped[B](f: A[Many] => B)(
        using MR: MapRes[DbValue, B]
    ): QueryGrouped[MR.K] =
      mapSingleGroupedK(a => MR.toK(f(a)))(using MR.applyKC, MR.traverseKC)

    def orderBy(f: A[DbValue] => OrdSeq): Query[A]

    def take(n: Int): Query[A]

    def drop(n: Int): Query[A]

    // TODO: Ensure the type of this will always be Long in all DBs
    def size: DbValue[Long]

    def nonEmpty: DbValue[Boolean]

    def isEmpty: DbValue[Boolean]
  end QueryBase

  trait QueryGroupedBase[A[_[_]]]:
    def having(f: A[DbValue] => DbValue[Boolean]): QueryGrouped[A]

  type Query[A[_[_]]] <: QueryBase[A]
  type QueryGrouped[A[_[_]]] <: QueryGroupedBase[A] & Query[A]

  type QueryCompanion
  val Query: QueryCompanion
}
