package dataprism

import scala.annotation.unused
import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.reflect.TypeTest
import cats.Applicative
import cats.syntax.all.*
import cats.{Applicative, Functor}
import perspective.*
import perspective.Finite.NotZero

import scala.quoted.Quotes

//noinspection DuplicatedCode
object KMacros {

  type MirrorProductK[F[_[_]]] = Mirror.Product {
    type MirroredType[A[_]] = F[A]
    type MirroredMonoType   = F[Id]
    type MirroredElemTypes[A[_]] <: Tuple
    type MirroredElemLabels <: Tuple
  }

  private transparent inline def getInstancesForFields[Labels <: Tuple, F[_[_]], Instance[_[_[_]]]]: Seq[Any] = ${
    getInstancesForFieldsImpl[Labels, F, Instance]
  }

  // Can't use substitute type as we didn't get a nice symbol to substitute
  def replaceTypes(using q: Quotes)(dest: q.reflect.TypeRepr)(from: List[q.reflect.TypeRepr], to: List[q.reflect.TypeRepr]): q.reflect.TypeRepr =
    import q.reflect.*
    from.zipWithIndex.find(_._1 =:= dest).fold(dest)(t => to(t._2)) match
      case t: TermRef => TermRef(replaceTypes(t.qualifier)(from, to), t.name)
      case t: TypeRef => t
      case t: ConstantType => t
      case t: SuperType => SuperType(replaceTypes(t.thistpe)(from, to), replaceTypes(t.supertpe)(from, to))
      case t: Refinement => Refinement(replaceTypes(t.parent)(from, to), t.name, replaceTypes(t.info)(from, to))
      case t: AppliedType => AppliedType(replaceTypes(t.tycon)(from, to), t.args.map(replaceTypes(_)(from, to)))
      case t: AnnotatedType => AnnotatedType(replaceTypes(t.underlying)(from, to), t.annotation)
      case t: AndType => AndType(replaceTypes(t.left)(from, to), replaceTypes(t.right)(from, to))
      case t: OrType => OrType(replaceTypes(t.left)(from, to), replaceTypes(t.right)(from, to))
      case t: MatchType =>
        MatchType(
          replaceTypes(t.bound)(from, to),
          replaceTypes(t.scrutinee)(from, to),
          t.cases.map(replaceTypes(_)(from, to))
        )
      case t: ByNameType => ByNameType(replaceTypes(t.underlying)(from, to))
      case t: ParamRef => t
      case t: ThisType => t
      case t: RecursiveThis => t
      case t: RecursiveType => t
      case t: MethodType => t
      case t: PolyType => t
      case t: TypeLambda => t
      case t: MatchCase => MatchCase(replaceTypes(t.pattern)(from, to), replaceTypes(t.rhs)(from, to))
      case t: TypeBounds => TypeBounds(replaceTypes(t.low)(from, to), replaceTypes(t.hi)(from, to))
      case t: NoPrefix => t
      case t => t
  end replaceTypes

  private def getInstancesForFieldsImpl[Labels <: Tuple: scala.quoted.Type, F[_[_]]: scala.quoted.Type, Instance[_[_[
      _
  ]]]: scala.quoted.Type](
      using q: scala.quoted.Quotes
  ): scala.quoted.Expr[Seq[Any]] =
    import scala.quoted.*
    import q.reflect.*
    val labels: Labels = Type.valueOfTuple[Labels].get
    if !labels.productIterator.forall(_.isInstanceOf[String]) then report.error("Labels are not all strings")

    val fType: TypeRepr = TypeRepr.of[F]

    if !summon[TypeTest[TypeRepr, TypeLambda]].unapply(fType).isDefined then
      report.errorAndAbort(s"${Type.show[F]} not instance of TypeLambda")
    val fTypeLambda                                        = fType.asInstanceOf[TypeLambda]
    val TypeLambda(fTypeLambdaNames, fTypeLambdaBounds, _) = fTypeLambda

    val instanceType: TypeRepr = TypeRepr.of[Instance]

    val (errors, values) = labels.productIterator
      .map(_.asInstanceOf[String])
      .toSeq
      .partitionMap: label =>
        val fieldSym = fType.typeSymbol.declaredField(label)
        val fieldTpe2 = TypeLambda(
          fTypeLambdaNames.map(_ + "Upd"),
          _ => fTypeLambdaBounds,
          l =>
            replaceTypes(fType.memberType(fieldSym))(
              fTypeLambdaNames.indices.toList.map(idx => fTypeLambda.param(idx)),
              fTypeLambdaNames.indices.toList.map(idx => l.param(idx))
            )
        )

        Implicits.search(AppliedType(instanceType, List(fieldTpe2))) match
          case iss: ImplicitSearchSuccess => Right(iss.tree.asExpr)
          case isf: ImplicitSearchFailure => Left(isf.explanation)
    end val

    if errors.nonEmpty then
      errors.init.foreach(report.error)
      report.errorAndAbort(errors.last)
    else Expr.ofSeq(values)

  // Types here are a complete lie, but I take some safety over none
  private inline def functionImpl[F[_[_]] <: Product, Z[_], Instance[_[_[_]]]](
      length: Int
  )(f: [X] => (Int, Instance[IdFC[X]]) => Z[X])(using m: MirrorProductK[F]): Seq[Z[Any]] = {
    val instances = getInstancesForFields[m.MirroredElemLabels, F, FunctorKC]
    (0 until length).map(i => f[Any](i, instances(i).asInstanceOf[Instance[IdFC[Any]]]))
  }

  private inline def function1Impl[F[_[_]] <: Product, A[_], Z[_], Instance[_[_[_]]]](
      fa: F[A]
  )(f: [X] => (Int, A[X], Instance[IdFC[X]]) => Z[X])(using m: MirrorProductK[F]): Seq[Z[Any]] =
    functionImpl[F, Z, Instance](fa.productArity)(
      [X] => (i: Int, instance: Instance[IdFC[X]]) => f[X](i, fa.productElement(i).asInstanceOf[A[X]], instance)
    )

  private inline def function2Impl[F[_[_]] <: Product, A[_], B[_], Z[_], Instance[_[_[_]]]](
      fa: F[A],
      fb: F[B]
  )(f: [X] => (Int, A[X], B[X], Instance[IdFC[X]]) => Z[X])(using m: MirrorProductK[F]): Seq[Z[Any]] =
    functionImpl[F, Z, Instance](fa.productArity)(
      [X] =>
        (i: Int, instance: Instance[IdFC[X]]) =>
          f[X](i, fa.productElement(i).asInstanceOf[A[X]], fb.productElement(i).asInstanceOf[B[X]], instance)
    )

  private inline def mapKImpl[F[_[_]] <: Product, A[_], B[_]](fa: F[A], f: A :~>: B)(using m: MirrorProductK[F]): F[B] =
    m.fromProduct(
      Tuple.fromArray(
        function1Impl[F, A, B, FunctorKC](fa)(
          [X] => (_: Int, a: A[X], instance: FunctorKC[IdFC[X]]) => instance.mapK(a)(f)
        ).toArray
      )
    ).asInstanceOf[F[B]]

  private inline def map2KImpl[F[_[_]] <: Product, A[_], B[_], Z[_]](
      fa: F[A],
      fb: F[B],
      f: [X] => (A[X], B[X]) => Z[X]
  )(using m: MirrorProductK[F]): F[Z] =
    m.fromProduct(
      Tuple.fromArray(
        function2Impl[F, A, B, Z, ApplyKC](fa, fb)(
          [X] => (_: Int, a: A[X], b: B[X], instance: ApplyKC[IdFC[X]]) => instance.map2K(a)(b)(f)
        ).toArray
      )
    ).asInstanceOf[F[Z]]

  private inline def pureKImpl[F[_[_]] <: Product, A[_]](a: ValueK[A])(using m: MirrorProductK[F]): F[A] =
    val size = constValue[Tuple.Size[m.MirroredElemLabels]]
    m.fromProduct(
      Tuple.fromArray(
        functionImpl[F, A, ApplicativeKC](size)(
          [X] => (_: Int, instance: ApplicativeKC[IdFC[X]]) => instance.pure(a)
        ).toArray
      )
    ).asInstanceOf[F[A]]

  private inline def foldLeftKImpl[F[_[_]] <: Product, A[_], B](fa: F[A], b: B, f: B => A :~>#: B)(
      using m: MirrorProductK[F]
  ): B =
    val fs = function1Impl[F, A, Const[B => B], FoldableKC](fa)(
      [X] => (_: Int, a: A[X], instance: FoldableKC[IdFC[X]]) => (b: B) => instance.foldLeftK(a)(b)(f)
    )
    fs.foldLeft(b)((b, f) => f(b))

  private inline def foldRightKImpl[F[_[_]] <: Product, A[_], B](fa: F[A], b: B, f: A :~>#: (B => B))(
      using m: MirrorProductK[F]
  ): B =
    val fs = function1Impl[F, A, Const[B => B], FoldableKC](fa)(
      [X] => (_: Int, a: A[X], instance: FoldableKC[IdFC[X]]) => (b: B) => instance.foldRightK(a)(b)(f)
    )
    fs.foldRight(b)((f, b) => f(b))

  private inline def traverseKImpl[F[_[_]] <: Product, A[_], G[_]: Applicative, B[_]](
      fa: F[A],
      f: A :~>: Compose2[G, B]
  )(using m: MirrorProductK[F]): G[F[B]] =
    val vs = function1Impl[F, A, Compose2[G, B], TraverseKC](fa)(
      [X] => (_: Int, a: A[X], instance: TraverseKC[IdFC[X]]) => instance.traverseK(a)(f)
    )
    vs.sequence[G, B[Any]].map(s => m.fromProduct(Tuple.fromArray(s.toArray)).asInstanceOf[F[B]])

  private inline def cosequenceKImpl[F[_[_]] <: Product, G[_]: Functor, A[_]](gfa: G[F[A]])(
      using m: MirrorProductK[F]
  ): F[Compose2[G, A]] =
    val size = constValue[Tuple.Size[m.MirroredElemLabels]]
    m.fromProduct(
      Tuple.fromArray(
        functionImpl[F, Compose2[G, A], DistributiveKC](size)(
          [X] =>
            (idx: Int, instance: DistributiveKC[IdFC[X]]) => instance.cosequenceK(gfa.map(fa => fa.productElement(idx).asInstanceOf[A[X]]))
        ).toArray
      )
    ).asInstanceOf[F[Compose2[G, A]]]

  private inline def tabulateKImpl[F[_[_]] <: Product, A[_], Size <: Int](f: (Finite[Size], Any) :#~>: A)(
      using m: MirrorProductK[F],
      notZero: NotZero[Size] =:= true
  ): F[A] =
    val size = constValue[Size]
    m.fromProduct(
      Tuple.fromArray(
        functionImpl[F, A, RepresentableKC](size)(
          [X] =>
            (i: Int, instance: RepresentableKC[IdFC[X]]) =>
              instance.tabulateK([Y] => (rep: instance.RepresentationK[Y]) => f[Y]((Finite(size, i), rep)))
        ).toArray
      )
    ).asInstanceOf[F[A]]

  private inline def indexKImpl[F[_[_]] <: Product, A[_], ThisSize <: Int, X](
      fa: F[A],
      i: (Finite[ThisSize], Any)
  )(using m: MirrorProductK[F]): A[X] =
    val instances = getInstancesForFields[m.MirroredElemLabels, F, RepresentableKC]
    val instance  = instances(i._1.value).asInstanceOf[RepresentableKC[IdFC[Any]]]
    instance.indexK(fa.productElement(i._1.value).asInstanceOf[A[Any]])(i._2.asInstanceOf[instance.RepresentationK[X]])

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

  inline def deriveDistributiveKC[F[_[_]] <: Product](using m: MirrorProductK[F]): DistributiveKC[F] =
    new DistributiveKC[F] {
      extension [G[_]: Functor, A[_], C](gfa: G[F[A]])
        override def cosequenceK: F[Compose2[G, A]]                            = cosequenceKImpl(gfa)
      extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)
    }

  // TODO: Improve representation type
  inline def deriveRepresentableKC[F[_[_]] <: Product](
      using m: MirrorProductK[F],
      @unused notZero: NotZero[Tuple.Size[m.MirroredElemLabels]] =:= true
  ): RepresentableKC.Aux[F, [A] =>> (Finite[Tuple.Size[m.MirroredElemLabels]], Any)] =
    new RepresentableKC[F] {
      type RepresentationK[_] = (Finite[Tuple.Size[m.MirroredElemLabels]], Any)

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

  type RepresentableTraverseKC[F[_[_]]] = RepresentableKC[F] & TraverseKC[F]

  // TODO: Improve representation type
  inline def deriveRepresentableTraverseKC[F[_[_]] <: Product](
      using m: MirrorProductK[F],
      @unused notZero: NotZero[Tuple.Size[m.MirroredElemLabels]] =:= true
  ): RepresentableKC.Aux[F, [A] =>> (Finite[Tuple.Size[m.MirroredElemLabels]], Any)] & TraverseKC[F] =
    new RepresentableKC[F] with TraverseKC[F] {
      type RepresentationK[_] = (Finite[Tuple.Size[m.MirroredElemLabels]], Any)

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
        def foldLeftK[B](b: B)(f: B => A :~>#: B): B    = foldLeftKImpl(fa, b, f)
        def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = foldRightKImpl(fa, b, f)

      extension [A[_], C](fa: F[A])
        def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[B]] =
          traverseKImpl(fa, f)
    }

  type ApplyTraverseKC[F[_[_]]] = ApplyKC[F] & TraverseKC[F]

  inline def deriveApplyTraverseKC[F[_[_]] <: Product](
      using m: MirrorProductK[F],
      @unused notZero: NotZero[Tuple.Size[m.MirroredElemLabels]] =:= true
  ): ApplyKC[F] & TraverseKC[F] =
    new ApplyKC[F] with TraverseKC[F] {
      extension [A[_], C](fa: F[A]) override def mapK[B[_]](f: A :~>: B): F[B] = mapKImpl(fa, f)

      extension [A[_], C](fa: F[A])
        override def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] = map2KImpl(fa, fb, f)

      extension [A[_], C](fa: F[A])
        def foldLeftK[B](b: B)(f: B => A :~>#: B): B    = foldLeftKImpl(fa, b, f)
        def foldRightK[B](b: B)(f: A :~>#: (B => B)): B = foldRightKImpl(fa, b, f)

      extension [A[_], C](fa: F[A])
        def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[B]] =
          traverseKImpl(fa, f)
    }
}
