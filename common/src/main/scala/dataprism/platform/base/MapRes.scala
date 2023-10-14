package dataprism.platform.base

import cats.Applicative
import perspective.*
import perspective.derivation.{ProductK, ProductKPar, TypeLength}

trait MapRes[F[_], R] {
  type K[_[_]]

  inline def toK(r: R): K[F]
  inline def fromK(k: K[F]): R

  inline def applyKC: ApplyKC[K]
  inline def traverseKC: TraverseKC[K]
}
object MapRes {
  type Aux[F[_], R, K0[_[_]]] = MapRes[F, R] { type K[G[_]] = K0[G] }

  given [F[_], K0[_[_]]](using FA: ApplyKC[K0], FT: TraverseKC[K0]): MapRes[F, K0[F]] with {
    type K[F0[_]] = K0[F0]
    override inline def toK(r: K0[F]): K[F]   = r
    override inline def fromK(k: K[F]): K0[F] = k

    override inline def applyKC: ApplyKC[K] = FA

    override inline def traverseKC: TraverseKC[K] = FT
  }

  given [F[_], T <: NonEmptyTuple](
      using ev: Tuple.IsMappedBy[F][T],
      size: ValueOf[Tuple.Size[T]]
  ): MapRes[F, T] with {
    override type K[F0[_]] = ProductK[F0, Tuple.InverseMap[T, F]]

    override inline def toK(r: T): ProductK[F, Tuple.InverseMap[T, F]]   = ProductK.ofScalaTuple(r)
    override inline def fromK(k: ProductK[F, Tuple.InverseMap[T, F]]): T = k.tuple.asInstanceOf[T]

    private val instance = {
      given TypeLength.Aux[Tuple.InverseMap[T, F], Tuple.Size[T]] = TypeLength.TypeLengthImpl(size.value)
      ProductK.productKInstance[Tuple.InverseMap[T, F]]
    }

    override inline def applyKC: ApplyKC[K] = instance

    override inline def traverseKC: TraverseKC[K] = instance
  }

  given [F[_], A]: MapRes[F, F[A]] with {
    override type K[F0[_]] = F0[A]

    override inline def toK(r: F[A]): K[F] = r
    override inline def fromK(k: K[F]): F[A] = k

    private val instance: ApplyKC[K] with TraverseKC[K] = new ApplyKC[K] with TraverseKC[K] {
      extension [A[_], C](fa: K[A])
        def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (A[X], B[X]) => Z[X]): K[Z] =
          f(fa, fb)

        override def mapK[B[_]](f: A :~>: B): K[B] = f(fa)

        def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[K[B]] =
          f(fa)

        def foldLeftK[B](b: B)(f: B => A :~>#: B): B = f(b)(fa)

        def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = f(fa)(b)
    }

    override inline def applyKC: ApplyKC[K] = instance

    override inline def traverseKC: TraverseKC[K] = instance
  }

}
