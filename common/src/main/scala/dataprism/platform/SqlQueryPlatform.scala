package dataprism.platform

import java.util.UUID

import scala.annotation.targetName

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.sharedast
import dataprism.sharedast.SelectAst.Data
import dataprism.sharedast.{AstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform extends QueryPlatform { platform =>

  val sqlRenderer: AstRenderer

  trait SqlBinOpBase {
    def ast: SqlExpr.BinaryOperation
  }

  type BinOp[LHS, RHS, R] <: SqlBinOpBase
  enum SqlBinOp[LHS, RHS, R](op: SqlExpr.BinaryOperation) extends SqlBinOpBase {
    case Eq[A]()  extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Eq)
    case Neq[A]() extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Neq)
    case And      extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolAnd)
    case Or       extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolOr)

    override def ast: SqlExpr.BinaryOperation = op
  }
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlDbValueBase[A] {

    def ast: SqlExpr

    def hasGroupBy: Boolean

    def asSqlDbVal: Option[SqlDbValue[A]]
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
    case DbColumn(column: Column[A], id: UUID)
    case QueryColumn(queryName: String, fromName: String)
    case GroupBy(value: DbValue[A])
    case JoinNullable[A](value: DbValue[A])                                            extends SqlDbValue[Option[A]]
    case BinOp[A, B, R](lhs: DbValue[A], rhs: DbValue[B], op: platform.BinOp[A, B, R]) extends SqlDbValue[R]
    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue])

    override def ast: SqlExpr = this match
      case SqlDbValue.DbColumn(_, _)                   => throw new IllegalArgumentException("Value not tagged")
      case SqlDbValue.QueryColumn(queryName, fromName) => SqlExpr.QueryRef(fromName, queryName)
      case SqlDbValue.GroupBy(value)                   => value.ast
      case SqlDbValue.BinOp(lhs, rhs, op)              => SqlExpr.BinOp(lhs.ast, rhs.ast, op.ast)
      case SqlDbValue.JoinNullable(value)              => value.ast
      case SqlDbValue.Function(f, values)              => SqlExpr.FunctionCall(f, values.map(_.ast))
    end ast

    override def hasGroupBy: Boolean = this match
      case SqlDbValue.DbColumn(_, _)      => false
      case SqlDbValue.QueryColumn(_, _)   => false
      case SqlDbValue.GroupBy(_)          => true
      case SqlDbValue.BinOp(lhs, rhs, _)  => lhs.hasGroupBy || rhs.hasGroupBy
      case SqlDbValue.JoinNullable(value) => value.hasGroupBy
      case SqlDbValue.Function(_, values) => values.exists(_.hasGroupBy)
    end hasGroupBy

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)
  }
  extension [A](sqlDbValue: SqlDbValue[A]) def liftSqlDbValue: DbValue[A]

  extension [A](dbVal: DbValue[A])

    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Eq().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Neq().liftSqlBinOp).liftSqlDbValue

    @targetName("dbValAsGrouped") protected[platform] inline def asGrouped: Grouped[A] = dbVal

    @targetName("dbValAsMany") protected[platform] inline def dbValAsMany: Many[A] = dbVal

    @targetName("dbValAsAnyDbVal") protected[platform] def asAnyDbVal: AnyDbValue

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).liftSqlDbValue

    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).liftSqlDbValue

  trait SqlOrdSeqBase {
    def ast: Seq[SelectAst.OrderExpr]
  }

  type OrdSeq <: SqlOrdSeqBase

  opaque type Grouped[A] = DbValue[A]
  opaque type Many[A]    = DbValue[A]

  extension [A](grouped: Grouped[A])
    @targetName("groupedAsMany")
    inline def asMany: Many[A] = grouped

    @targetName("groupedAsDbValue") protected[platform] inline def asDbValue: DbValue[A] = grouped

  extension [A](many: Many[A]) @targetName("manyAsDbValue") protected[platform] inline def asDbValue: DbValue[A] = many

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
    ) extends SqlValueSource[[F[_]] =>> (A[Compose2[F, Option]], B[F])]
    case RightJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[F], B[Compose2[F, Option]])]
    case FullJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends SqlValueSource[[F[_]] =>> (A[Compose2[F, Option]], B[Compose2[F, Option]])]

    private def mapOptCompose[F[_[_]], A[_], B[_]](fa: F[Compose2[A, Option]])(f: A ~>: B)(
        using F: FunctorKC[F]
    ): F[Compose2[B, Option]] =
      fa.mapK([Z] => (v: A[Option[Z]]) => f[Option[Z]](v))

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

        new FunctorKC[[F[_]] =>> (lt[Compose2[F, Option]], rt[F])] {
          extension [X[_], C](fa: (lt[Compose2[X, Option]], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Compose2[Y, Option]], rt[Y]) =
              val l: lt[Compose2[X, Option]] = fa._1
              (mapOptCompose(fa._1)(f), fa._2.mapK(f))
        }
      case SqlValueSource.RightJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC
        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[Compose2[F, Option]])] {
          extension [X[_], C](fa: (lt[X], rt[Compose2[X, Option]]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Compose2[Y, Option]]) =
              (fa._1.mapK(f), mapOptCompose(fa._2)(f))
        }
      case SqlValueSource.FullJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC
        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[Compose2[F, Option]], rt[Compose2[F, Option]])] {
          extension [X[_], C](fa: (lt[Compose2[X, Option]], rt[Compose2[X, Option]]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Compose2[Y, Option]], rt[Compose2[Y, Option]]) =
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

    private def mapJoinNullable[A[_[_]]: FunctorKC](values: A[DbValue]): A[Compose2[DbValue, Option]] =
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
        import lhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.LeftOuterJoin.apply, (a, b) => (mapJoinNullable(a), b))
      case SqlValueSource.RightJoin(lhs: ValueSource[l], rhs: ValueSource[r], on) =>
        import rhs.given
        fromPartJoin(lhs, rhs, on, SelectAst.From.RightOuterJoin.apply, (a, b) => (a, mapJoinNullable(b)))
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

  trait SqlTaggedState {
    def queryNum: Int

    def columnNum: Int

    def withNewQueryNum(newQueryNum: Int): TaggedState

    def withNewColumnNum(newColumnNum: Int): TaggedState
  }
  type TaggedState <: SqlTaggedState
  protected def freshTaggedState: TaggedState

  type TagState[A] = State[TaggedState, A]

  trait SqlQueryBase[A[_[_]]] {

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B]

    def addInnerJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]]

    def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]]

    def addLeftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]]

    def addRightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]]

    def addFullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]]

    def addGroupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B]

    def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A]

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A]

    def addLimit(i: Int): Query[A]

    def addOffset(i: Int): Query[A]

    def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])]

    def selectAst: SelectAst = selectAstAndValues.runA(freshTaggedState).value._1

    def applicativeK: ApplicativeKC[A]
    given ApplicativeKC[A] = applicativeK

    def traverseK: TraverseKC[A]
    given TraverseKC[A] = traverseK
  }
  type Query[A[_[_]]] <: SqlQueryBase[A]

  sealed trait SqlQuery[A[_[_]]] extends SqlQueryBase[A] {

    def nested: Query[A] =
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromQuery(this.liftSqlQuery).liftSqlValueSource).liftSqlQuery

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] = nested.addFilter(f)

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
      nested.addMap(f)

    def addInnerJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]] = nested.addInnerJoin(that)(on)

    def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] = nested.addCrossJoin(that)

    def addLeftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = nested.addLeftJoin(that)(on)

    def addRightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = nested.addRightJoin(that)(on)

    def addFullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = nested.addFullJoin(that)(on)

    def addGroupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
      nested.addGroupBy(f)

    def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A] = nested.addHaving(f)

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = nested.addOrderBy(f)

    def addLimit(i: Int): Query[A] = nested.addLimit(i)

    def addOffset(i: Int): Query[A] = nested.addOffset(i)
  }
  object SqlQuery {
    private def tagValues[A[_[_]]](
        values: A[DbValue]
    )(using TraverseKC[A], ApplicativeKC[A]): TagState[(A[Const[String]], Seq[SelectAst.ExprWithAlias])] =
      State { st =>
        val columnNum = st.columnNum

        val columnNumState: State[Int, A[Const[String]]] =
          values.traverseK(
            [X] => (_: DbValue[X]) => State[Int, Const[String][X]]((acc: Int) => (acc + 1, s"x$acc"))
          )

        val (newColumnNum, columnAliases) = columnNumState.run(columnNum).value

        val exprWithAliases = values
          .tupledK(columnAliases)
          .foldMapK(
            [X] => (t: (DbValue[X], String)) => List(SelectAst.ExprWithAlias(t._1.ast, Some(t._2)))
          )

        (st.withNewColumnNum(newColumnNum), (columnAliases, exprWithAliases))
      }

    case class SqlQueryWithoutFrom[A[_[_]]](values: A[DbValue])(
        using val applicativeK: ApplicativeKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {
      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        tagValues(values).map { case (aliases, exprWithAliases) =>
          val selectAst = SelectAst(
            SelectAst.Data
              .SelectFrom(
                None,
                exprWithAliases,
                None,
                None,
                None,
                None
              ),
            SelectAst.OrderLimit(None, None, None)
          )

          (selectAst, aliases, values)
        }

      override def addMap[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        copy(f(values)).liftSqlQuery
    }

    type AppTravKC[A[_[_]]] = ApplicativeKC[A] with TraverseKC[A]

    private def innerJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] =
      new ApplicativeKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] {
        type F[X[_]] = (MA[X], MB[X])

        extension [A[_]](a: ValueK[A])
          def pure[C]: F[A] =
            (a.pure, a.pure)

        extension [A[_], C](fa: F[A])
          def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] =
            (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))

        extension [A[_], C](fa: F[A])
          override def mapK[B[_]](f: A ~>: B): F[B] =
            (AA.mapK(fa._1)(f), BA.mapK(fa._2)(f))

        extension [A[_], C](fa: F[A])
          def traverseK[G[_]: Applicative, B[_]](f: A ~>: Compose2[G, B]): G[F[B]] =
            val r1 = fa._1.traverseK(f)
            val r2 = fa._2.traverseK(f)

            r1.product(r2)

        extension [A[_], C](fa: F[A])
          def foldLeftK[B](b: B)(f: B => A ~>#: B): B =
            val r1 = fa._1.foldLeftK(b)(f)
            val r2 = fa._2.foldLeftK(r1)(f)
            r2
      }

    private def leftJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]]]

    private def rightJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]]]

    private def fullJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]]]

    case class SqlQueryFromStage[A[_[_]]](valueSource: ValueSource[A])(
        using val applicativeK: ApplicativeKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {

      override def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
        SqlQueryMapFilterHavingStage(valueSource).addFilter(f)

      override def addMap[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryMapFilterHavingStage(valueSource).addMap(f)

      override def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A] =
        SqlQueryMapFilterHavingStage(valueSource).addHaving(f)

      override def addGroupBy[B[_[_]]](
          f: A[Grouped] => B[DbValue]
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryFilterGroupByHavingStage[A, A](this.liftSqlQuery).addGroupBy(f)

      override def addInnerJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.InnerJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.CrossJoin(valueSource, ValueSource.getFromQuery(that)).liftSqlValueSource
        ).liftSqlQuery

      override def addLeftJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[LeftJoin[A, B]] =
        import that.given
        given AppTravKC[LeftJoin[A, B]] = leftJoinInstances

        copy(
          SqlValueSource.LeftJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addRightJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[RightJoin[A, B]] =
        import that.given
        given AppTravKC[RightJoin[A, B]] = rightJoinInstances

        copy(
          SqlValueSource.RightJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addFullJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[FullJoin[A, B]] =
        import that.given
        given AppTravKC[FullJoin[A, B]] = fullJoinInstances

        copy(
          SqlValueSource.FullJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        valueSource.fromPartAndValues.flatMap { case (from, values) =>
          tagValues(values).map { case (aliases, exprWithAliases) =>
            val selectAst = SelectAst(
              SelectAst.Data.SelectFrom(None, exprWithAliases, Some(from), None, None, None),
              SelectAst.OrderLimit(None, None, None)
            )

            (selectAst, aliases, values)
          }
        }
    }

    case class SqlQueryMapFilterHavingStage[A[_[_]], B[_[_]]](
        valueSource: ValueSource[A],
        map: A[DbValue] => B[DbValue] = identity[A[DbValue]],
        filter: Option[A[DbValue] => DbValue[Boolean]] = None,
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplicativeKC[A],
        FTA: TraverseKC[A],
        val applicativeK: ApplicativeKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {

      override def addFilter(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(map(values))
          filter.fold(newBool)(old => old(values) && newBool)

        copy(filter = Some(cond)).liftSqlQuery

      override def addMap[C[_[_]]](
          f: B[DbValue] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        copy(
          map = this.map.andThen(f),
          filter = this.filter
        ).liftSqlQuery

      override def addGroupBy[C[_[_]]](
          f: B[Grouped] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        SqlQueryFilterGroupByHavingStage[B, B](this.liftSqlQuery).addGroupBy(f)

      override def addHaving(f: B[Grouped] => DbValue[Boolean]): Query[B] =
        given FunctorKC[B] = applicativeK

        val cond = (values: A[DbValue]) =>
          val newBool = f(map(values).mapK([A] => (value: DbValue[A]) => value.asGrouped))
          filter.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addOrderBy(f: B[DbValue] => OrdSeq): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        valueSource.fromPartAndValues.flatMap { case (from, values) =>
          val mappedValues = map(values)
          tagValues(mappedValues).map { case (aliases, exprWithAliases) =>
            val selectAst = SelectAst(
              SelectAst.Data.SelectFrom(
                None,
                exprWithAliases,
                Some(from),
                filter.map(f => f(values).ast),
                None,
                having.map(f => f(values).ast)
              ),
              SelectAst.OrderLimit(None, None, None)
            )

            (selectAst, aliases, mappedValues)
          }
        }
    }

    case class SqlQueryFilterGroupByHavingStage[A[_[_]], B[_[_]]](
        query: Query[A],
        groupBy: Option[A[Grouped] => B[DbValue]] = None,
        filter: Option[A[DbValue] => DbValue[Boolean]] = None,
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(using val applicativeK: ApplicativeKC[B], val traverseK: TraverseKC[B])
        extends SqlQuery[B] {
      import query.given

      override def addFilter(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[Grouped]) =>
          val newBool = f(groupBy.fold(values.asInstanceOf[B[DbValue]])(f => f(values)))
          filter.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addGroupBy[C[_[_]]](
          f: B[Grouped] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        given FunctorKC[B] = applicativeK
        copy(groupBy =
          Some(
            groupBy.fold(f.asInstanceOf[A[DbValue] => C[DbValue]])(oldGroupBy =>
              oldGroupBy.andThen(bValues => f(bValues.mapK([X] => (value: DbValue[X]) => value.asGrouped)))
            )
          )
        ).liftSqlQuery

      override def addHaving(f: B[Grouped] => DbValue[Boolean]): Query[B] =
        given FunctorKC[B] = applicativeK

        val cond = (values: A[Grouped]) =>
          val newBool = f(
            groupBy
              .fold(values.asInstanceOf[B[DbValue]])(f => f(values))
              .mapK([A] => (value: DbValue[A]) => value.asGrouped)
          )
          having.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addOrderBy(f: B[DbValue] => OrdSeq): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        query.selectAstAndValues.flatMap { case (selectAst, _, values) =>
          val groupedValues = this.groupBy.fold(values.asInstanceOf[B[DbValue]])(f => f(values))

          tagValues(groupedValues).map { case (aliases, exprWithAliases) =>
            val newSelectAst = selectAst.copy(
              data = selectAst.data match
                case from: Data.SelectFrom =>
                  val filterAst = this.filter.map(f => f(values).ast)
                  val havingAst = this.having.map(f => f(values).ast)

                  def astAnd(lhs: Option[SqlExpr], rhs: Option[SqlExpr]): Option[SqlExpr] = (lhs, rhs) match
                    case (Some(lhs), Some(rhs)) => Some(SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BoolAnd))
                    case (Some(lhs), None)      => Some(lhs)
                    case (None, Some(rhs))      => Some(rhs)
                    case (None, None)           => None

                  val optGroupBys = this.groupBy.map { _ =>
                    groupedValues.foldMapK(
                      [X] => (value: DbValue[X]) => if value.hasGroupBy then List(value.ast) else Nil
                    )
                  }

                  from.copy(
                    selectExprs = exprWithAliases,
                    where = astAnd(from.where, filterAst),
                    groupBy = optGroupBys.map(SelectAst.GroupBy.apply),
                    having = astAnd(from.having, havingAst)
                  )

                case data: Data.SetOperatorData => data
            )

            (newSelectAst, aliases, groupedValues)
          }
        }
    }

    case class SqlQueryOrderedLimitOffsetStage[A[_[_]]](
        query: Query[A],
        orderBy: Option[A[DbValue] => OrdSeq] = None,
        limit: Option[Int] = None,
        offset: Option[Int] = None
    ) extends SqlQuery[A] {
      export query.{applicativeK, traverseK}

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = copy(orderBy = Some(f)).liftSqlQuery

      override def addLimit(i: Int): Query[A] = copy(limit = Some(i)).liftSqlQuery

      override def addOffset(i: Int): Query[A] = copy(offset = Some(i)).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        query.selectAstAndValues.map { case (selectAst, aliases, values) =>
          val orderLimit = selectAst.orderLimit

          val newOrderLimit =
            if orderLimit.isEmpty then
              SelectAst.OrderLimit(
                orderBy.map(f => SelectAst.OrderBy(f(values).ast)),
                if limit.isDefined || offset.isDefined then
                  Some(SelectAst.LimitOffset(limit, offset.getOrElse(0), withTies = false))
                else None,
                None
              )
            else orderLimit

          (selectAst.copy(orderLimit = newOrderLimit), aliases, values)
        }
    }
  }
  extension [A[_[_]]](sqlQuery: SqlQuery[A]) def liftSqlQuery: Query[A]

  extension (q: QueryCompanion)
    @targetName("queryCompanionFrom")
    def from[A[_[_]]](table: Table[A])(using TraverseKC[A]): Query[A] =
      import table.given
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromTable(table).liftSqlValueSource).liftSqlQuery
    end from

  extension [A[_[_]]](query: Query[A])

    @targetName("queryFilter")
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      query.addFilter(f)

    @targetName("queryMap")
    def map[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      query.addMap(f)

    @targetName("queryJoin")
    def join[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[InnerJoin[A, B]] =
      query.addInnerJoin(that)(on)

    @targetName("queryJoin")
    def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] =
      query.addCrossJoin(that)

    @targetName("queryLeftJoin")
    def leftJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[LeftJoin[A, B]] =
      query.addLeftJoin(that)(on)

    @targetName("queryRightJoin")
    def rightJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[RightJoin[A, B]] =
      query.addRightJoin(that)(on)

    @targetName("queryFullJoin")
    def fullJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[FullJoin[A, B]] =
      query.addFullJoin(that)(on)

    @targetName("queryGroupBy")
    def groupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      query.addGroupBy(f)

    @targetName("queryJoin") def join[B[_[_]]](that: Table[B])(
      on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[InnerJoin[A, B]] = query.join(Query.from(that))(on)

    @targetName("queryCrossJoin") def crossJoin[B[_[_]]](that: Table[B])(using TraverseKC[B]): Query[InnerJoin[A, B]] =
      query.crossJoin(Query.from(that))

    @targetName("queryLeftJoin") def leftJoin[B[_[_]]](that: Table[B])(
      on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[LeftJoin[A, B]] = query.leftJoin(Query.from(that))(on)

    @targetName("queryRightJoin") def rightJoin[B[_[_]]](that: Table[B])(
      on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[RightJoin[A, B]] = query.rightJoin(Query.from(that))(on)

    @targetName("queryFullJoin") def fullJoin[B[_[_]]](that: Table[B])(
      on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[FullJoin[A, B]] = query.fullJoin(Query.from(that))(on)

    @targetName("queryHaving")
    def having(f: A[Grouped] => DbValue[Boolean]): Query[A] = query.addHaving(f)

    @targetName("queryOrderBy")
    def orderBy(f: A[DbValue] => OrdSeq): Query[A] = query.addOrderBy(f)

    @targetName("queryLimit")
    def limit(n: Int): Query[A] = query.addLimit(n)

    @targetName("queryOffset")
    def offset(n: Int): Query[A] = query.addOffset(n)
  end extension
}
