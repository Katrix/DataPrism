package dataprism

import scala.compiletime.constValue
import scala.deriving.Mirror
import cats.Applicative
import cats.syntax.all.*
import perspective.*
import perspective.Finite.NotZero

import scala.annotation.unused

//noinspection DuplicatedCode
object KMacros {

  type MirrorProductK[F[_[_]]] = Mirror.Product {
    type MirroredType[A[_]] = F[A]
    type MirroredMonoType   = F[Id]
    type MirroredElemTypes[A[_]] <: Tuple
    type MirroredElemLabels <: Tuple
  }

  private inline def mapKImpl[F[_[_]] <: Product, A[_], B[_]](fa: F[A], f: A :~>: B)(using m: MirrorProductK[F]): F[B] =
    m.fromProduct(Tuple.fromArray(fa.productIterator.map(a => f(a.asInstanceOf[A[Any]])).toArray))
      .asInstanceOf[F[B]]

  private inline def map2KImpl[F[_[_]] <: Product, A[_], B[_], Z[_]](
      fa: F[A],
      fb: F[B],
      f: [X] => (A[X], B[X]) => Z[X]
  )(using m: MirrorProductK[F]): F[Z] =
    m.fromProduct(
      Tuple.fromArray(
        fa.productIterator
          .zip(fb.productIterator)
          .map((a, b) => f(a.asInstanceOf[A[Any]], b.asInstanceOf[B[Any]]))
          .toArray
      )
    ).asInstanceOf[F[Z]]

  private inline def pureKImpl[F[_[_]] <: Product, A[_]](a: ValueK[A])(using m: MirrorProductK[F]): F[A] =
    val size = constValue[Tuple.Size[m.MirroredElemLabels]]
    m.fromProduct(Tuple.fromArray(Array.fill(size)(a[Any]()))).asInstanceOf[F[A]]

  private inline def foldLeftKImpl[F[_[_]] <: Product, A[_], B](fa: F[A], b: B, f: B => A :~>#: B): B =
    fa.productIterator.foldLeft(b)((acc, a) => f(acc)(a.asInstanceOf[A[Any]]))

  private inline def foldRightKImpl[F[_[_]] <: Product, A[_], B](fa: F[A], b: B, f: A :~>#: (B => B)): B =
    fa.productIterator.foldRight(b)((a, acc) => f(a.asInstanceOf[A[Any]])(acc))

  private inline def traverseKImpl[F[_[_]] <: Product, A[_], G[_]: Applicative, B[_]](
      fa: F[A],
      f: A :~>: Compose2[G, B]
  )(using m: MirrorProductK[F]): G[F[B]] =
    fa.productIterator.toSeq
      .traverse(a => f(a.asInstanceOf[A[Any]]): G[B[Any]])
      .map(l => m.fromProduct(Tuple.fromArray(l.toArray)).asInstanceOf[F[B]])

  private inline def tabulateKImpl[F[_[_]] <: Product, A[_], Size <: Int](f: Finite[Size] :#~>: A)(
      using m: MirrorProductK[F],
      notZero: NotZero[Size] =:= true
  ): F[A] =
    val size = constValue[Size]
    m.fromProduct(Tuple.fromArray(Array.tabulate(size)(n => f[Any](Finite(size, n))))).asInstanceOf[F[A]]

  private inline def indexKImpl[F[_[_]] <: Product, A[_], Size <: Int, X](
      fa: F[A],
      i: Finite[Size]
  ): A[X] =
    fa.productElement(i.value).asInstanceOf[A[X]]

  inline def deriveFunctorKC[F[_[_]] <: Product](using m: MirrorProductK[F]): FunctorKC[F] = new FunctorKC[F] {
    extension [A[_], C](fa: F[A]) def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)
  }

  inline def deriveApplyKC[F[_[_]] <: Product](using m: MirrorProductK[F]): ApplyKC[F] = new ApplyKC[F] {
    extension [A[_], C](fa: F[A]) def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

    extension [A[_], C](fa: F[A])
      def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] = map2KImpl(fa, fb, f)
  }

  inline def deriveApplicativeKC[F[_[_]] <: Product](using m: MirrorProductK[F]): ApplicativeKC[F] =
    new ApplicativeKC[F] {
      extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

      extension [A[_], C](fa: F[A])
        def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] = map2KImpl(fa, fb, f)

      extension [A[_]](a: ValueK[A])
        def pure[C]: F[A] =
          pureKImpl(a)
    }

  inline def deriveFoldableKC[F[_[_]] <: Product](using m: MirrorProductK[F]): FoldableKC[F] = new FoldableKC[F] {
    extension [A[_], C](fa: F[A])
      def foldLeftK[B](b: B)(f: B => A :~>#: B): B = foldLeftKImpl(fa, b, f)

      def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = foldRightKImpl(fa, b, f)
  }

  inline def deriveTraverseKC[F[_[_]] <: Product](using m: MirrorProductK[F]): TraverseKC[F] = new TraverseKC[F] {
    extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

    extension [A[_], C](fa: F[A])
      def foldLeftK[B](b: B)(f: B => A :~>#: B): B = foldLeftKImpl(fa, b, f)

      def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = foldRightKImpl(fa, b, f)

    extension [A[_], C](fa: F[A])
      def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[B]] =
        traverseKImpl(fa, f)
  }

  inline def deriveRepresentableKC[F[_[_]] <: Product](
      using m: MirrorProductK[F],
      @unused notZero: NotZero[Tuple.Size[m.MirroredElemLabels]] =:= true
  ): RepresentableKC.Aux[F, [A] =>> Finite[Tuple.Size[m.MirroredElemLabels]]] =
    new RepresentableKC[F] {
      type RepresentationK[_] = Finite[Tuple.Size[m.MirroredElemLabels]]

      def tabulateK[A[_], C](f: RepresentationK :~>: A): F[A] = tabulateKImpl(f)

      extension [A[_], C](fa: F[A])
        def indexK[Z](rep: RepresentationK[Z]): A[Z] = indexKImpl[F, A, Tuple.Size[m.MirroredElemLabels], Z](fa, rep)

      extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

      extension [A[_], C](fa: F[A])
        override def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] = map2KImpl(fa, fb, f)

      extension [A[_]](a: ValueK[A])
        override def pure[C]: F[A] =
          pureKImpl(a)
    }

  type RepresentableTraverseKC[F[_[_]]] = RepresentableKC[F] with TraverseKC[F]

  inline def deriveRepresentableTraverseKC[F[_[_]] <: Product](
      using m: MirrorProductK[F],
      @unused notZero: NotZero[Tuple.Size[m.MirroredElemLabels]] =:= true
  ): RepresentableKC.Aux[F, [A] =>> Finite[Tuple.Size[m.MirroredElemLabels]]] with TraverseKC[F] =
    new RepresentableKC[F] with TraverseKC[F] {
      type RepresentationK[_] = Finite[Tuple.Size[m.MirroredElemLabels]]

      def tabulateK[A[_], C](f: RepresentationK :~>: A): F[A] = tabulateKImpl(f)

      extension [A[_], C](fa: F[A])
        def indexK[Z](rep: RepresentationK[Z]): A[Z] = indexKImpl[F, A, Tuple.Size[m.MirroredElemLabels], Z](fa, rep)

      extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

      extension [A[_], C](fa: F[A])
        override def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] = map2KImpl(fa, fb, f)

      extension [A[_]](a: ValueK[A])
        override def pure[C]: F[A] =
          pureKImpl(a)

      extension [A[_], C](fa: F[A])
        def foldLeftK[B](b: B)(f: B => A :~>#: B): B = foldLeftKImpl(fa, b, f)
        def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = foldRightKImpl(fa, b, f)

      extension [A[_], C](fa: F[A])
        def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[B]] =
          traverseKImpl(fa, f)
    }
}
