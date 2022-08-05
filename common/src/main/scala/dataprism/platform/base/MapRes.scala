package dataprism.platform.base

import cats.Applicative
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait MapRes[F[_], R] {
  type K[_[_]]

  def toK(r: R): K[F]

  def applicativeKC: ApplicativeKC[K]
  def traverseKC: TraverseKC[K]
}
object MapRes {
  given [F[_], K0[_[_]]](using FA: ApplicativeKC[K0], FT: TraverseKC[K0]): MapRes[F, K0[F]] with {
    type K[F0[_]] = K0[F0]
    override inline def toK(r: K0[F]): K[F] = r

    override inline def applicativeKC: ApplicativeKC[K] = FA

    override inline def traverseKC: TraverseKC[K] = FT
  }

  given [F[_], T <: NonEmptyTuple](
      using ev: Tuple.IsMappedBy[F][T],
      size: ValueOf[Tuple.Size[Tuple.InverseMap[T, F]]]
  ): MapRes[F, T] with {
    override type K[F0[_]] = ProductK[F0, Tuple.InverseMap[T, F]]

    override inline def toK(r: T): ProductK[F, Tuple.InverseMap[T, F]] = ProductK.of(r)

    private val instance = ProductK.productKInstance[Tuple.InverseMap[T, F]]

    override inline def applicativeKC: ApplicativeKC[K] = instance

    override inline def traverseKC: TraverseKC[K] = instance
  }

  given [F[_], A]: MapRes[F, F[A]] with {
    override type K[F0[_]] = F0[A]

    override inline def toK(r: F[A]): K[F] = r

    private val instance: ApplicativeKC[K] with TraverseKC[K] = new ApplicativeKC[K] with TraverseKC[K] {
      extension [A[_]](a: ValueK[A]) def pure[C]: K[A] = a()

      extension[A[_], C](fa: K[A])
        def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (A[X], B[X]) => Z[X]): K[Z] =
          f(fa, fb)

        override def mapK[B[_]](f: A ~>: B): K[B] = f(fa)

        def traverseK[G[_]: Applicative, B[_]](f: A ~>: Compose2[G, B]): G[K[B]] =
          f(fa)

        def foldLeftK[B](b: B)(f: B => A ~>#: B): B = f(b)(fa)
    }

    override def applicativeKC: ApplicativeKC[K] = instance

    override def traverseKC: TraverseKC[K] = instance
  }

}
