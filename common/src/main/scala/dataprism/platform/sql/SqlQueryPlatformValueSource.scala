package dataprism.platform.sql

import scala.annotation.targetName

import cats.data.State
import cats.syntax.all.*
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformValueSource { this: SqlQueryPlatform =>

  case class ValueSourceAstMetaData[A[_[_]]](ast: SelectAst.From[Codec], values: A[DbValue])

  trait SqlValueSourceBase[A[_[_]]] {
    def applyKC: ApplyKC[A]

    given ApplyKC[A] = applyKC

    def fromPartAndValues: TagState[ValueSourceAstMetaData[A]]
  }

  type ValueSource[A[_[_]]] <: SqlValueSourceBase[A]
  type ValueSourceCompanion
  val ValueSource: ValueSourceCompanion

  enum SqlValueSource[A[_[_]]] extends SqlValueSourceBase[A] {
    case FromQuery(q: Query[A])
    case FromTable(t: Table[Codec, A])
    case InnerJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[F], B[F])]
    case CrossJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B]
    ) extends SqlValueSource[[F[_]] =>> (A[F], B[F])]
    case LeftJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[F], B[Compose2[F, Nullable]])]
    case RightJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[Compose2[F, Nullable]], B[F])]
    case FullJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[Compose2[F, Nullable]], B[Compose2[F, Nullable]])]

    private def mapOptCompose[F[_[_]], X[_], Y[_]](fa: F[Compose2[X, Nullable]])(f: X :~>: Y)(
        using F: FunctorKC[F]
    ): F[Compose2[Y, Nullable]] =
      fa.mapK([Z] => (v: X[Nullable[Z]]) => f[Nullable[Z]](v))

    private def map2OptCompose[F[_[_]], X[_], Y[_], Z[_]](fa: F[Compose2[X, Nullable]], fb: F[Compose2[Y, Nullable]])(
        f: [W] => (X[W], Y[W]) => Z[W]
    )(using F: ApplyKC[F]): F[Compose2[Z, Nullable]] =
      fa.map2K(fb)([W] => (v1: X[Nullable[W]], v2: Y[Nullable[W]]) => f[Nullable[W]](v1, v2))

    def makeApplyKC[lt[_[_]]: ApplyKC, rt[_[_]]: ApplyKC] = new ApplyKC[[F[_]] =>> (lt[F], rt[F])] {
      extension [X[_], C](fa: (lt[X], rt[X]))
        def mapK[Y[_]](f: X :~>: Y): (lt[Y], rt[Y]) =
          (fa._1.mapK(f), fa._2.mapK(f))

        def map2K[Y[_], Z[_]](fb: (lt[Y], rt[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (lt[Z], rt[Z]) =
          (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
    }

    def applyKC: ApplyKC[A] = this match
      case SqlValueSource.FromQuery(q)     => q.applyK
      case SqlValueSource.FromTable(table) => table.FA
      case SqlValueSource.InnerJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given ApplyKC[lt] = l.applyKC
        given ApplyKC[rt] = r.applyKC

        def make[Lt[_[_]]: ApplyKC, Rt[_[_]]: ApplyKC] = new ApplyKC[[F[_]] =>> (Lt[F], Rt[F])] {
          extension [X[_], C](fa: (Lt[X], Rt[X]))
            def mapK[Y[_]](f: X :~>: Y): (Lt[Y], Rt[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))

            def map2K[Y[_], Z[_]](fb: (Lt[Y], Rt[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (Lt[Z], Rt[Z]) =
              (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
        }

        make[lt, rt] // Doesn't work otherwise

      case SqlValueSource.CrossJoin(l: ValueSource[lt], r: ValueSource[rt]) =>
        given ApplyKC[lt] = l.applyKC
        given ApplyKC[rt] = r.applyKC

        def make[Lt[_[_]]: ApplyKC, Rt[_[_]]: ApplyKC] = new ApplyKC[[F[_]] =>> (Lt[F], Rt[F])] {
          extension [X[_], C](fa: (Lt[X], Rt[X]))
            def mapK[Y[_]](f: X :~>: Y): (Lt[Y], Rt[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))

            def map2K[Y[_], Z[_]](fb: (Lt[Y], Rt[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (Lt[Z], Rt[Z]) =
              (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
        }

        make[lt, rt] // Doesn't work otherwise

      case SqlValueSource.LeftJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given ApplyKC[lt] = l.applyKC
        given ApplyKC[rt] = r.applyKC

        def make[Lt[_[_]]: ApplyKC, Rt[_[_]]: ApplyKC] = new ApplyKC[[F[_]] =>> (Lt[F], Rt[Compose2[F, Nullable]])] {
          extension [X[_], C](fa: (Lt[X], Rt[Compose2[X, Nullable]]))
            def mapK[Y[_]](f: X :~>: Y): (Lt[Y], Rt[Compose2[Y, Nullable]]) =
              (fa._1.mapK(f), mapOptCompose(fa._2)(f))

            def map2K[Y[_], Z[_]](fb: (Lt[Y], Rt[Compose2[Y, Nullable]]))(
                f: [W] => (X[W], Y[W]) => Z[W]
            ): (Lt[Z], Rt[Compose2[Z, Nullable]]) =
              (fa._1.map2K(fb._1)(f), map2OptCompose(fa._2, fb._2)(f))
        }

        make[lt, rt] // Doesn't work otherwise

      case SqlValueSource.RightJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given ApplyKC[lt] = l.applyKC
        given ApplyKC[rt] = r.applyKC

        def make[Lt[_[_]]: ApplyKC, Rt[_[_]]: ApplyKC] = new ApplyKC[[F[_]] =>> (Lt[Compose2[F, Nullable]], Rt[F])] {
          extension [X[_], C](fa: (Lt[Compose2[X, Nullable]], Rt[X]))
            def mapK[Y[_]](f: X :~>: Y): (Lt[Compose2[Y, Nullable]], Rt[Y]) =
              (mapOptCompose(fa._1)(f), fa._2.mapK(f))

            def map2K[Y[_], Z[_]](fb: (Lt[Compose2[Y, Nullable]], Rt[Y]))(
                f: [W] => (X[W], Y[W]) => Z[W]
            ): (Lt[Compose2[Z, Nullable]], Rt[Z]) =
              (map2OptCompose(fa._1, fb._1)(f), fa._2.map2K(fb._2)(f))
        }

        make[lt, rt] // Doesn't work otherwise

      case SqlValueSource.FullJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given ApplyKC[lt] = l.applyKC
        given ApplyKC[rt] = r.applyKC

        def make[Lt[_[_]]: ApplyKC, Rt[_[_]]: ApplyKC] =
          new ApplyKC[[F[_]] =>> (Lt[Compose2[F, Nullable]], Rt[Compose2[F, Nullable]])] {
            extension [X[_], C](fa: (Lt[Compose2[X, Nullable]], Rt[Compose2[X, Nullable]]))
              def mapK[Y[_]](f: X :~>: Y): (Lt[Compose2[Y, Nullable]], Rt[Compose2[Y, Nullable]]) =
                (mapOptCompose(fa._1)(f), mapOptCompose(fa._2)(f))

              def map2K[Y[_], Z[_]](fb: (Lt[Compose2[Y, Nullable]], Rt[Compose2[Y, Nullable]]))(
                  f: [W] => (X[W], Y[W]) => Z[W]
              ): (Lt[Compose2[Z, Nullable]], Rt[Compose2[Z, Nullable]]) =
                (map2OptCompose(fa._1, fb._1)(f), map2OptCompose(fa._2, fb._2)(f))
          }

        make[lt, rt]
    end applyKC

    private def fromPartJoin[A[_[_]], B[_[_]], R[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        make: (SelectAst.From[Codec], SelectAst.From[Codec], SqlExpr[Codec]) => SelectAst.From[Codec],
        doJoin: (A[DbValue], B[DbValue]) => R[DbValue]
    ): TagState[ValueSourceAstMetaData[R]] =
      for
        lmeta <- lhs.fromPartAndValues
        rmeta <- rhs.fromPartAndValues
        onAst <- on(lmeta.values, rmeta.values).ast
      yield ValueSourceAstMetaData(
        make(lmeta.ast, rmeta.ast, onAst),
        doJoin(lmeta.values, rmeta.values)
      )

    private def mapJoinNullable[A[_[_]]: FunctorKC](values: A[DbValue]): A[Compose2[DbValue, Nullable]] =
      values.mapK([X] => (value: DbValue[X]) => SqlDbValue.JoinNullable(value).lift)

    def fromPartAndValues: TagState[ValueSourceAstMetaData[A]] = this match
      case SqlValueSource.FromQuery(q) =>
        q.selectAstAndValues.flatMap { meta =>
          State { st =>
            val queryNum  = st.queryNum
            val queryName = s"y$queryNum"

            val newValues =
              meta.aliases.map2K(meta.values)(
                [X] => (alias: String, value: DbValue[X]) => SqlDbValue.QueryColumn[X](alias, queryName, value.tpe).lift
              )

            (
              st.withNewQueryNum(queryNum + 1),
              ValueSourceAstMetaData(SelectAst.From.FromQuery(meta.ast, queryName), newValues)
            )
          }
        }

      case SqlValueSource.FromTable(table) =>
        State { st =>
          given FunctorKC[A] = table.FA

          val queryNum  = st.queryNum
          val queryName = s"${table.tableName}_y$queryNum"

          val values = table.columns.mapK(
            [X] => (column: Column[Codec, X]) => SqlDbValue.QueryColumn[X](column.nameStr, queryName, column.tpe).lift
          )

          (
            st.withNewQueryNum(queryNum + 1),
            ValueSourceAstMetaData(SelectAst.From.FromTable(table.tableName, Some(queryName)), values)
          )
        }

      case SqlValueSource.InnerJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        fromPartJoin(lhs, rhs, on, SelectAst.From.InnerJoin.apply, (a, b) => (a, b))
      case SqlValueSource.CrossJoin(lhs: ValueSource[l], rhs: ValueSource[r]) =>
        (lhs.fromPartAndValues, rhs.fromPartAndValues).mapN { (lmeta, rmeta) =>
          ValueSourceAstMetaData(SelectAst.From.CrossJoin(lmeta.ast, rmeta.ast), (lmeta.values, rmeta.values))
        }
      case SqlValueSource.LeftJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import rhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.LeftOuterJoin.apply, (a, b) => (a, mapJoinNullable(b)))
      case SqlValueSource.RightJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import lhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.RightOuterJoin.apply, (a, b) => (mapJoinNullable(a), b))
      case SqlValueSource.FullJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import lhs.given
        import rhs.given
        fromPartJoin(
          lhs,
          rhs,
          on,
          SelectAst.From.FullOuterJoin.apply,
          (a, b) => (mapJoinNullable[l](a), mapJoinNullable[r](b))
        )
    end fromPartAndValues
  }

  extension [A[_[_]]](sqlValueSource: SqlValueSource[A]) def liftSqlValueSource: ValueSource[A]

  extension (c: ValueSourceCompanion)
    @targetName("valueSourceGetFromQuery") def getFromQuery[A[_[_]]](query: Query[A]): ValueSource[A]

}
