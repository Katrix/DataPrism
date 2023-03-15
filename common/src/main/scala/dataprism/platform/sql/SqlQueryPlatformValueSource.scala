package dataprism.platform.sql

import scala.annotation.targetName

import cats.data.State
import cats.syntax.all.*

import perspective.*
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.{Table, Column}

trait SqlQueryPlatformValueSource { this: SqlQueryPlatform =>

  trait SqlValueSourceBase[A[_[_]]] {
    def functorKC: FunctorKC[A]

    given FunctorKC[A] = functorKC

    def fromPartAndValues: TagState[(SelectAst.From, A[DbValue])]
  }

  type ValueSource[A[_[_]]] <: SqlValueSourceBase[A]
  type ValueSourceCompanion
  val ValueSource: ValueSourceCompanion

  enum SqlValueSource[A[_[_]]] extends SqlValueSourceBase[A] {
    case FromQuery(q: Query[A])
    case FromTable(t: Table[A])
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

    private def mapOptCompose[F[_[_]], A[_], B[_]](fa: F[Compose2[A, Nullable]])(f: A ~>: B)(
        using F: FunctorKC[F]
    ): F[Compose2[B, Nullable]] =
      fa.mapK([Z] => (v: A[Nullable[Z]]) => f[Nullable[Z]](v))

    def functorKC: FunctorKC[A] = this match
      case SqlValueSource.FromQuery(q)     => q.applicativeK
      case SqlValueSource.FromTable(table) => table.FA
      case SqlValueSource.InnerJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[F])] {
          extension [X[_], C](fa: (lt[X], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))
        }
      case SqlValueSource.CrossJoin(l: ValueSource[lt], r: ValueSource[rt]) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[F])] {
          extension [X[_], C](fa: (lt[X], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))
        }
      case SqlValueSource.LeftJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[Compose2[F, Nullable]])] {
          extension[X[_], C] (fa: (lt[X], rt[Compose2[X, Nullable]]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Compose2[Y, Nullable]]) =
              (fa._1.mapK(f), mapOptCompose(fa._2)(f))
        }
      case SqlValueSource.RightJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[Compose2[F, Nullable]], rt[F])] {
          extension [X[_], C](fa: (lt[Compose2[X, Nullable]], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Compose2[Y, Nullable]], rt[Y]) =
              val l: lt[Compose2[X, Nullable]] = fa._1
              (mapOptCompose(fa._1)(f), fa._2.mapK(f))
        }
      case SqlValueSource.FullJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[Compose2[F, Nullable]], rt[Compose2[F, Nullable]])] {
          extension [X[_], C](fa: (lt[Compose2[X, Nullable]], rt[Compose2[X, Nullable]]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Compose2[Y, Nullable]], rt[Compose2[Y, Nullable]]) =
              (mapOptCompose(fa._1)(f), mapOptCompose(fa._2)(f))
        }
    end functorKC

    private def fromPartJoin[A[_[_]], B[_[_]], R](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        make: (SelectAst.From, SelectAst.From, SqlExpr) => SelectAst.From,
        doJoin: (A[DbValue], B[DbValue]) => R
    ): TagState[(SelectAst.From, R)] =
      lhs.fromPartAndValues.map2(rhs.fromPartAndValues) { case ((lfrom, lvalues), (rfrom, rvalues)) =>
        (make(lfrom, rfrom, on(lvalues, rvalues).ast), doJoin(lvalues, rvalues))
      }

    private def mapJoinNullable[A[_[_]]: FunctorKC](values: A[DbValue]): A[Compose2[DbValue, Nullable]] =
      values.mapK([X] => (value: DbValue[X]) => SqlDbValue.JoinNullable(value).liftSqlDbValue)

    def fromPartAndValues: TagState[(SelectAst.From, A[DbValue])] = this match
      case SqlValueSource.FromQuery(q) =>
        q.selectAstAndValues.flatMap { case (queryAst, aliases, _) =>
          State { st =>
            val queryNum  = st.queryNum
            val queryName = s"y$queryNum"

            val newValues =
              aliases.mapK([X] => (alias: String) => SqlDbValue.QueryColumn[X](alias, queryName).liftSqlDbValue)

            (st.withNewQueryNum(queryNum + 1), (SelectAst.From.FromQuery(queryAst, queryName), newValues))
          }
        }

      case SqlValueSource.FromTable(table) =>
        State { st =>
          given FunctorKC[A] = table.FA

          val queryNum  = st.queryNum
          val queryName = s"${table.tableName}_y$queryNum"

          val values = table.columns.mapK(
            [X] => (column: Column[X]) => SqlDbValue.QueryColumn[X](column.nameStr, queryName).liftSqlDbValue
          )

          (st.withNewQueryNum(queryNum + 1), (SelectAst.From.FromTable(table.tableName, Some(queryName)), values))
        }
      case SqlValueSource.InnerJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        fromPartJoin(lhs, rhs, on, SelectAst.From.InnerJoin.apply, (a, b) => (a, b))
      case SqlValueSource.CrossJoin(lhs: ValueSource[l], rhs: ValueSource[r]) =>
        lhs.fromPartAndValues.map2(rhs.fromPartAndValues) { case ((lfrom, lvalues), (rfrom, rvalues)) =>
          (SelectAst.From.CrossJoin(lfrom, rfrom), (lvalues, rvalues))
        }
      case SqlValueSource.LeftJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import rhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.RightOuterJoin.apply, (a, b) => (a, mapJoinNullable(b)))
      case SqlValueSource.RightJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import lhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.LeftOuterJoin.apply, (a, b) => (mapJoinNullable(a), b))
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
