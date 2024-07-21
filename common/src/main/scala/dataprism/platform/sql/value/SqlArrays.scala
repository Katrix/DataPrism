package dataprism.platform.sql.value

import scala.annotation.targetName

import cats.Applicative
import dataprism.platform.MapRes
import dataprism.sharedast.SqlExpr
import perspective.{:~>#:, :~>:, ApplyKC, Compose2, TraverseKC}

trait SqlArrays extends SqlDbValuesBase { platform =>

  def arrayOfType[A](tpe: Type[A]): Type[Seq[A]]

  case class ArrayConcatBinOp[A, LHS <: Seq[A] | A, RHS <: Seq[A] | A](arrType: Type[Seq[A]])
      extends BinOp[LHS, RHS, Seq[A]]:
    override def name: String = "array_concat"

    override def ast: SqlExpr.BinaryOperation = SqlExpr.BinaryOperation.ArrayConcat

    override def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[Seq[A]] = arrType

  trait SqlDbArrayCompanion {
    def of[A](value: DbValue[A], values: DbValue[A]*): DbValue[Seq[A]] =
      Impl.function(SqlExpr.FunctionName.ArrayConstruction, (value +: values).map(_.asAnyDbVal), arrayOfType(value.tpe))

    private type NullableK[A[_[_]]] = [F[_]] =>> A[Compose2[F, Nullable]]

    private given nullableUnnestInstance[F[_[_]]](
        using FA: ApplyKC[F],
        FT: TraverseKC[F]
    ): (ApplyKC[NullableK[F]] & TraverseKC[NullableK[F]]) = new ApplyKC[NullableK[F]] with TraverseKC[NullableK[F]] {

      extension [A[_], C](fa: F[Compose2[A, Nullable]])
        override def mapK[B[_]](f: A :~>: B): F[Compose2[B, Nullable]] =
          FA.mapK(fa)([X] => (a: A[Nullable[X]]) => f(a))

        override def map2K[B[_], Z[_]](fb: F[Compose2[B, Nullable]])(
            f: [X] => (A[X], B[X]) => Z[X]
        ): F[Compose2[Z, Nullable]] =
          FA.map2K(fa)(fb)([X] => (a: A[Nullable[X]], b: B[Nullable[X]]) => f(a, b))

        override def foldLeftK[B](b: B)(f: B => A :~>#: B): B =
          FT.foldLeftK(fa)(b)(b => [X] => (a: A[Nullable[X]]) => f(b)(a))
        override def foldRightK[B](b: B)(f: A :~>#: (B => B)): B =
          FT.foldRightK(fa)(b)([X] => (a: A[Nullable[X]]) => b => f(a)(b))

        override def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[Compose2[B, Nullable]]] =
          FT.traverseK(fa)([X] => (a: A[Nullable[X]]) => f(a))
    }

    def unnestK[A[_[_]]](
        arrs: A[Compose2[DbValue, Seq]]
    )(using FA: ApplyKC[A], FT: TraverseKC[A]): Query[[F[_]] =>> A[Compose2[F, Nullable]]] =
      Impl.queryFunction(
        SqlExpr.FunctionName.Unnest,
        arrs.foldMapK([X] => (arr: DbValue[Seq[X]]) => List(arr.asAnyDbVal)),
        arrs.mapK[Compose2[Type, Nullable]](
          [X] =>
            (arr: DbValue[Seq[X]]) =>
              val tpe = arr.tpe
              import scala.compiletime.ops.int.*
              tpe
                .elementType(using <:<.refl.asInstanceOf[(tpe.Dimension > 0) =:= true])
                .choice
                .nullable
                .asInstanceOf[Type[Nullable[X]]]
        )
      )

    def unnest[T](arrs: T)(using mr: MapRes[Compose2[DbValue, Seq], T]): Query[[F[_]] =>> mr.K[Compose2[F, Nullable]]] =
      unnestK(mr.toK(arrs))(using mr.applyKC, mr.traverseKC)
  }
  type DbArrayCompanion <: SqlDbArrayCompanion
  val DbArray: DbArrayCompanion

  trait DbArrayLike[Col, A] {
    extension (arr: DbValue[Col])
      def apply(idx: DbValue[Int])(using n: Nullability[A]): DbValue[n.N[A]]

      def cardinality: DbValue[Int]

      def concat(other: DbValue[Col]): DbValue[Col]

      @targetName("append")
      def :+(other: DbValue[A]): DbValue[Col]

      def contains(a: DbValue[A]): DbValue[Boolean]

      def dropRight(n: DbValue[Int]): DbValue[Col]

    extension (v: DbValue[A])
      @targetName("prepend")
      def +:(other: DbValue[Col]): DbValue[Col]
  }
  object DbArrayLike {
    given seqIsDbArray[A]: DbArrayLike[Seq[A], A] with {
      extension (arr: DbValue[Seq[A]])

        override def apply(idx: DbValue[Int])(using n: Nullability[A]): DbValue[n.N[A]] =
          import scala.compiletime.ops.int.*
          val tpe = arr.tpe
          Impl.function(
            SqlExpr.FunctionName.ArrayGet,
            Seq(arr.asAnyDbVal, idx.asAnyDbVal),
            n.wrapType(
              tpe.elementType(using <:<.refl.asInstanceOf[(tpe.Dimension > 0) =:= true]).asInstanceOf[Type[A]]
            )
          )

        override def cardinality: DbValue[Int] =
          Impl.function(SqlExpr.FunctionName.Cardinality, Seq(arr.asAnyDbVal), AnsiTypes.integer)

        override def concat(other: DbValue[Seq[A]]): DbValue[Seq[A]] =
          Impl.binaryOp(arr, other, ArrayConcatBinOp(arr.tpe))

        @targetName("append")
        override def :+(other: DbValue[A]): DbValue[Seq[A]] = Impl.binaryOp(arr, other, ArrayConcatBinOp(arr.tpe))

        override def contains(a: DbValue[A]): DbValue[Boolean] =
          Impl.function(SqlExpr.FunctionName.ArrayContains, Seq(arr.asAnyDbVal, a.asAnyDbVal), AnsiTypes.boolean)

        override def dropRight(n: DbValue[Int]): DbValue[Seq[A]] =
          Impl.function(SqlExpr.FunctionName.TrimArray, Seq(arr.asAnyDbVal, n.asAnyDbVal), arr.tpe)

      extension (v: DbValue[A])
        @targetName("prepend")
        override def +:(arr: DbValue[Seq[A]]): DbValue[Seq[A]] = Impl.binaryOp(v, arr, ArrayConcatBinOp(arr.tpe))
    }
  }

  type Impl <: SqlArraysImpl & SqlValuesBaseImpl & SqlBaseImpl
  trait SqlArraysImpl {
    def queryFunction[A[_[_]]: ApplyKC: TraverseKC](
        function: SqlExpr.FunctionName,
        arguments: Seq[AnyDbValue],
        types: A[Type]
    ): Query[A]
  }

  type Api <: SqlArraysApi & SqlDbValueApi & QueryApi
  trait SqlArraysApi {
    export platform.DbArrayLike
    export platform.DbArrayLike.given

    type DbArrayCompanion = platform.DbArrayCompanion
    inline def DbArray: DbArrayCompanion = platform.DbArray
  }
}
