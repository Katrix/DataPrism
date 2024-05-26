package dataprism.platform.base

import cats.Applicative
import cats.syntax.all.*
import perspective.*
import perspective.derivation.{ProductK, TypeLength}

import scala.annotation.implicitNotFound

@implicitNotFound("Do not know how ${R} incorporates the type ${F}. If you're working with Types, try calling forgetNNA on all the types")
trait MapRes[F[_], R] {
  type K[_[_]]

  /*/*inline*/*/
  def toK(r: R): K[F]
  /*/*inline*/*/
  def fromK(k: K[F]): R

  /*/*inline*/*/
  def applyKC: ApplyKC[K]
  /*/*inline*/*/
  def traverseKC: TraverseKC[K]
}
object MapRes extends LowPriorityMapRes {
  type Aux[F[_], R, K0[_[_]]] = MapRes[F, R] { type K[G[_]] = K0[G] }

  given [F[_], K0[_[_]]](using FA: ApplyKC[K0], FT: TraverseKC[K0]): MapRes.Aux[F, K0[F], K0] = new MapRes[F, K0[F]] {
    type K[F0[_]] = K0[F0]
    override /*inline*/ def toK(r: K0[F]): K[F]   = r
    override /*inline*/ def fromK(k: K[F]): K0[F] = k

    override /*inline*/ def applyKC: ApplyKC[K] = FA

    override /*inline*/ def traverseKC: TraverseKC[K] = FT
  }

  given [F[_], T <: NonEmptyTuple](
      using ev: Tuple.IsMappedBy[F][T],
      size: ValueOf[Tuple.Size[T]]
  ): MapRes.Aux[F, T, [F0[_]] =>> Tuple.Map[Tuple.InverseMap[T, F], F0]] = new MapRes[F, T] {
    override type K[F0[_]] = Tuple.Map[Tuple.InverseMap[T, F], F0]

    override /*inline*/ def toK(r: T): Tuple.Map[Tuple.InverseMap[T, F], F] = r
    override /*inline*/ def fromK(k: Tuple.Map[Tuple.InverseMap[T, F], F]): T =
      given (Tuple.Map[Tuple.InverseMap[T, F], F] =:= T) = ev.flip
      k

    private val instance = {
      val baseInstance = {
        given TypeLength.Aux[Tuple.InverseMap[T, F], Tuple.Size[T]] = TypeLength.TypeLengthImpl(size.value)
        ProductK.productKInstance[Tuple.InverseMap[T, F]]
      }

      new ApplyKC[K] with TraverseKC[K] {
        import cats.syntax.all.*
        extension [A[_], C](fa: K[A])
          override def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (A[X], B[X]) => Z[X]): K[Z] =
            baseInstance.map2K[A, C](ProductK.ofScalaTuple(fa))[B, Z](ProductK.ofScalaTuple(fb))(f).scalaTuple
          override def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[K[B]] =
            baseInstance.traverseK(ProductK.ofScalaTuple(fa))(f).map(_.scalaTuple)
          override def foldLeftK[B](b: B)(f: B => A :~>#: B): B =
            baseInstance.foldLeftK(ProductK.ofScalaTuple(fa))(b)(f)
          override def foldRightK[B](b: B)(f: A :~>#: (B => B)): B =
            baseInstance.foldRightK(ProductK.ofScalaTuple(fa))(b)(f)
      }
    }

    override /*inline*/ def applyKC: ApplyKC[K] = instance.asInstanceOf[ApplyKC[K]]

    override /*inline*/ def traverseKC: TraverseKC[K] = instance.asInstanceOf[TraverseKC[K]]
  }

  given [F[_], A]: MapRes.Aux[F, F[A], [F0[_]] =>> F0[A]] = new MapRes[F, F[A]] {
    override type K[F0[_]] = F0[A]

    override /*inline*/ def toK(r: F[A]): K[F]   = r
    override /*inline*/ def fromK(k: K[F]): F[A] = k

    private val instance: ApplyKC[K] with TraverseKC[K] = new ApplyKC[K] with TraverseKC[K] {
      extension [D[_], C](fa: K[D])
        def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (D[X], B[X]) => Z[X]): K[Z] =
          f(fa, fb)

        override def mapK[B[_]](f: D :~>: B): K[B] = f(fa)

        def traverseK[G[_]: Applicative, B[_]](f: D :~>: Compose2[G, B]): G[K[B]] =
          f(fa)

        def foldLeftK[B](b: B)(f: B => D :~>#: B): B = f(b)(fa)

        def foldRightK[B](b: B)(f: D :~>#: (B => B)): B = f(fa)(b)
    }

    override /*inline*/ def applyKC: ApplyKC[K] = instance

    override /*inline*/ def traverseKC: TraverseKC[K] = instance
  }

  given recurTuple1[F[_], V, MRK[_[_]]](
      using mr: MapRes.Aux[F, V, MRK]
  ): MapRes.Aux[F, Tuple1[V], [F0[_]] =>> Tuple1[MRK[F0]]] = new MapRes[F, Tuple1[V]]:
    override type K[F0[_]] = Tuple1[MRK[F0]]

    override /*inline*/ def toK(r: Tuple1[V]): K[F] = Tuple1(mr.toK(r._1))

    override /*inline*/ def fromK(k: K[F]): Tuple1[V] = Tuple1(mr.fromK(k._1))

    override /*inline*/ def applyKC: ApplyKC[K] = new ApplyKC[K]:
      given applyInstance: ApplyKC[MRK] = mr.applyKC

      extension [A[_], C](fa: Tuple1[MRK[A]])
        override def map2K[B[_], Z[_]](fb: Tuple1[MRK[B]])(f: [X] => (A[X], B[X]) => Z[X]): Tuple1[MRK[Z]] = Tuple1(
          fa._1.map2K(fb._1)(f)
        )
        override def mapK[B[_]](f: A :~>: B): Tuple1[MRK[B]] = Tuple1(fa._1.mapK(f))
    end applyKC

    override /*inline*/ def traverseKC: TraverseKC[K] = new TraverseKC[K]:
      given traverseInstance: TraverseKC[MRK] = mr.traverseKC

      extension [A[_], C](fa: Tuple1[MRK[A]])
        override def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[Tuple1[MRK[B]]] =
          fa._1.traverseK(f).map(v => Tuple1(v))
        override def foldLeftK[B](b: B)(f: B => A :~>#: B): B    = fa._1.foldLeftK(b)(f)
        override def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = fa._1.foldRightK(b)(f)
  end recurTuple1
}
trait LowPriorityMapRes {

  // TODO: Make instances for more common tuples
  given recurTuple[F[_], H, T <: Tuple, KH[_[_]], KT[_[_]] <: Tuple](
      using mrh: MapRes.Aux[F, H, KH],
      mrt: MapRes.Aux[F, T, KT]
  ): MapRes.Aux[F, H *: T, [F0[_]] =>> KH[F0] *: KT[F0]] = new MapRes[F, H *: T]:
    override type K[F0[_]] = KH[F0] *: KT[F0]

    override /*inline*/ def toK(r: H *: T): K[F] = mrh.toK(r.head) *: mrt.toK(r.tail)

    override /*inline*/ def fromK(k: K[F]): H *: T = mrh.fromK(k.head) *: mrt.fromK(k.tail)

    override /*inline*/ def applyKC: ApplyKC[K] = new ApplyKC[K]:
      given headApplyInstance: ApplyKC[KH] = mrh.applyKC
      given tailApplyInstance: ApplyKC[KT] = mrt.applyKC

      extension [A[_], C](fa: K[A])
        override def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (A[X], B[X]) => Z[X]): K[Z] =
          fa.head.map2K(fb.head)(f) *: fa.tail.map2K(fb.tail)(f)

      extension [A[_], C](fa: K[A]) override def mapK[B[_]](f: A :~>: B): K[B] = fa.head.mapK(f) *: fa.tail.mapK(f)
    end applyKC

    override /*inline*/ def traverseKC: TraverseKC[K] = new TraverseKC[K]:
      given headTraverseInstance: TraverseKC[KH] = mrh.traverseKC
      given tailTraverseInstance: TraverseKC[KT] = mrt.traverseKC

      extension [A[_], C](fa: K[A])
        override def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[K[B]] =
          val r1: G[KH[B]] = fa.head.traverseK(f)
          val r2: G[KT[B]] = fa.tail.traverseK(f)
          Applicative[G].map2(r1, r2)(_ *: _)

        override def foldLeftK[B](b: B)(f: B => A :~>#: B): B =
          val r1 = fa.head.foldLeftK(b)(f)
          val r2 = fa.tail.foldLeftK(r1)(f)
          r2

        override def foldRightK[B](b: B)(f: A :~>#: (B => B)): B =
          val r1 = fa.head.foldRightK(b)(f)
          val r2 = fa.tail.foldRightK(r1)(f)
          r2
  end recurTuple

  given applyTraversable[F[_], G[_]: cats.Apply: cats.Traverse, V, MRK[_[_]]](
      using mr: MapRes.Aux[F, V, MRK]
  ): MapRes.Aux[F, G[V], [F0[_]] =>> G[MRK[F0]]] = new MapRes[F, G[V]] {
    import cats.*
    override type K[F0[_]] = G[MRK[F0]]
    given Functor[G] = summon[Apply[G]]

    override def toK(r: G[V]): G[MRK[F]] = r.map(v => mr.toK(v))

    override def fromK(k: G[MRK[F]]): G[V] = k.map(v => mr.fromK(v))

    override val applyKC: ApplyKC[K] = new ApplyKC[K] {
      given applyInstance: ApplyKC[MRK] = mr.applyKC

      extension [A[_], C](fa: K[A])
        override def map2K[B[_], Z[_]](fb: K[B])(f: [X] => (A[X], B[X]) => Z[X]): K[Z] =
          Apply[G].map2(fa, fb)((a, b) => a.map2K(b)(f))

        override def mapK[B[_]](f: A :~>: B): K[B] = fa.map(_.mapK(f))
    }

    override val traverseKC: TraverseKC[K] = new TraverseKC[K] {
      given traverseInstance: TraverseKC[MRK] = mr.traverseKC

      extension [A[_], C](fa: K[A])
        override def traverseK[I[_]: Applicative, B[_]](f: A :~>: Compose2[I, B]): I[K[B]] =
          fa.traverse(a => a.traverseK[I, B](f))

        override def foldLeftK[B](b: B)(f: B => A :~>#: B): B =
          fa.foldLeft(b)((bacc, mrka) => mrka.foldLeftK(bacc)(f))

        override def foldRightK[B](b: B)(f: A :~>#: (B => B)): B =
          fa.foldRight(Eval.now(b))((mrka, bacce) => bacce.map(bacc => mrka.foldRightK(bacc)(f))).value
    }
  }
}
