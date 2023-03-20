package dataprism.platform.implementations

import scala.annotation.targetName
import scala.concurrent.Future
import scala.reflect.ClassTag

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.{AstRenderer, PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
class PostgresQueryPlatform extends SqlQueryPlatform { platform =>

  val sqlRenderer: PostgresAstRenderer = new PostgresAstRenderer

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op

  enum DbValue[A] extends SqlDbValueBase[A] {
    case SqlDbValue(value: platform.SqlDbValue[A])
    case ArrayOf(values: Seq[DbValue[A]], elemType: DbType[A], clazzTag: ClassTag[A]) extends DbValue[Seq[A]]

    def ast: TagState[SqlExpr] = this match
      case DbValue.SqlDbValue(value) => value.ast
      case DbValue.ArrayOf(values, _, _) =>
        values.toList
          .traverse(_.ast)
          .map(exprs => SqlExpr.Custom(exprs, args => sql"ARRAY[${args.intercalate(sql", ")}]"))
    end ast

    def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case DbValue.SqlDbValue(res) => Some(res)
      case _                       => None

    override def tpe: DbType[A] = this match
      case DbValue.SqlDbValue(value)      => value.tpe
      case DbValue.ArrayOf(_, tpe, clazz) => DbType.array(tpe)(using clazz)
    end tpe

    def singletonArray(using tag: ClassTag[A]): DbValue[Seq[A]] = DbValue.ArrayOf(Seq(this), tpe, tag)
  }

  override type AnyDbValue = DbValue[Any]

  extension [A](sqlDbValue: SqlDbValue[A]) def liftSqlDbValue: DbValue[A] = DbValue.SqlDbValue(sqlDbValue)

  extension [A](dbVal: DbValue[A])
    @targetName("dbValAsAnyDbVal")
    protected[platform] def asAnyDbVal: AnyDbValue = dbVal.asInstanceOf[AnyDbValue]

    @targetName("dbValAsc") def asc: Ord   = Ord.Asc(dbVal)
    @targetName("dbValDesc") def desc: Ord = Ord.Desc(dbVal)

  sealed trait OrdSeq extends SqlOrdSeqBase

  enum Ord extends OrdSeq {
    case Asc(value: DbValue[_])
    case Desc(value: DbValue[_])

    def ast: TagState[Seq[SelectAst.OrderExpr]] = this match
      case Ord.Asc(value)  => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Asc, None)))
      case Ord.Desc(value) => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Desc, None)))
  }

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq {
    def ast: TagState[Seq[SelectAst.OrderExpr]] = init.ast.flatMap(i => tail.ast.map(t => i ++ t))
  }

  extension (ordSeq: OrdSeq) @targetName("ordSeqAndThen") def andThen(ord: Ord): OrdSeq = MultiOrdSeq(ordSeq, ord)

  extension [A](many: Many[A])
    def arrayAgg(using ClassTag[A]): DbValue[Seq[A]] =
      SqlDbValue
        .Function[Seq[A]](
          SqlExpr.FunctionName.Custom("array_agg"),
          Seq(many.asDbValue.asAnyDbVal),
          DbType.array(many.asDbValue.tpe)
        )
        .liftSqlDbValue

  type ValueSource[A[_[_]]] = SqlValueSource[A]
  type ValueSourceCompanion = SqlValueSource.type
  val ValueSource: ValueSourceCompanion = SqlValueSource

  extension [A[_[_]]](sqlValueSource: SqlValueSource[A]) def liftSqlValueSource: ValueSource[A] = sqlValueSource

  private def anyDefined(options: Option[_]*): Boolean =
    options.exists(_.isDefined)

  extension (c: ValueSourceCompanion)
    @targetName("valueSourceGetFromQuery") def getFromQuery[A[_[_]]](query: Query[A]): ValueSource[A] =
      query match
        case baseQuery: SqlQuery.SqlQueryFromStage[A] => baseQuery.valueSource
        case _                                        => SqlValueSource.FromQuery(query)

  type Query[A[_[_]]]        = SqlQuery[A]
  type QueryGrouped[A[_[_]]] = SqlQueryGrouped[A]

  val Query: QueryCompanion = SqlQuery
  override type QueryCompanion = SqlQuery.type

  extension [A[_[_]]](sqlQuery: SqlQuery[A]) def liftSqlQuery: Query[A] = sqlQuery

  extension [A[_[_]]](sqlQueryGrouped: SqlQueryGrouped[A]) def liftSqlQueryGrouped: QueryGrouped[A] = sqlQueryGrouped

  case class TaggedState(queryNum: Int, columnNum: Int) extends SqlTaggedState {
    override def withNewQueryNum(newQueryNum: Int): TaggedState = copy(queryNum = newQueryNum)

    override def withNewColumnNum(newColumnNum: Int): TaggedState = copy(columnNum = newColumnNum)
  }
  protected def freshTaggedState: TaggedState = TaggedState(0, 0)

  sealed trait Operation[A]:
    def run(using db: Db): Future[A]
  object Operation:
    case class Select[Res[_[_]]](query: Query[Res]) extends Operation[QueryResult[Res[Id]]]:
      override def run(using db: Db): Future[QueryResult[Res[Id]]] =
        import query.given
        given FunctorKC[Res] = summon[ApplyKC[Res]]

        val astMeta = query.selectAstAndValues.runA(freshTaggedState).value
        db.runIntoRes(
          sqlRenderer.renderSelect(astMeta.ast),
          astMeta.values.mapK([Z] => (value: DbValue[Z]) => value.tpe)
        )
    end Select

    case class Delete[A[_[_]], B[_[_]]](
        from: Table[A],
        usingV: Option[Query[B]] = None,
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends Operation[Int]:
      given ApplyKC[B] = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

      given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

      override def run(using db: Db): Future[Int] =
        val q = usingV match
          case Some(usingQ) => Query.from(from).flatMap(a => usingQ.where(b => where(a, b)))
          case None         => Query.from(from).where(a => where(a, a.asInstanceOf[B[DbValue]]))

        db.run(
          sqlRenderer.renderDelete(
            q.selectAstAndValues.runA(freshTaggedState).value.ast,
            returning = false
          )
        )

      def returningK[C[_[_]]: ApplyKC: TraverseKC](
          f: (A[DbValue], B[DbValue]) => C[DbValue]
      ): DeleteReturning[A, B, C] = DeleteReturning(from, usingV, where, f)

      inline def returning[C](f: (A[DbValue], B[DbValue]) => C)(
          using MR: MapRes[DbValue, C]
      ): DeleteReturning[A, B, MR.K] =
        returningK((a, b) => MR.toK(f(a, b)))(using MR.applyKC, MR.traverseKC)
    object Delete:
      def from[A[_[_]]](from: Table[A]): DeleteFrom[A, A] =
        import from.given
        DeleteFrom(from)
    end Delete

    case class DeleteFrom[A[_[_]], B[_[_]]](from: Table[A], using: Option[Query[B]] = None) {

      def using[B1[_[_]]](query: Query[B1]): DeleteFrom[A, B1] = DeleteFrom(from, Some(query))

      def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Delete[A, B] = Delete(from, using, f)
    }

    case class DeleteReturning[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
        from: Table[A],
        usingV: Option[Query[B]] = None,
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        returning: (A[DbValue], B[DbValue]) => C[DbValue]
    ) extends Operation[QueryResult[C[Id]]]:
      given ApplyKC[B] = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

      given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

      override def run(using db: Db): Future[QueryResult[C[Id]]] =
        given FunctorKC[C] = summon[ApplyKC[C]]

        val q = usingV match
          case Some(usingQ) => Query.from(from).flatMap(a => usingQ.where(b => where(a, b)).mapK(b => returning(a, b)))
          case None =>
            Query
              .from(from)
              .where(a => where(a, a.asInstanceOf[B[DbValue]]))
              .mapK(a => returning(a, a.asInstanceOf[B[DbValue]]))

        val astMeta = q.selectAstAndValues.runA(freshTaggedState).value

        db.runIntoRes(
          sqlRenderer.renderDelete(
            astMeta.ast,
            returning = true
          ),
          astMeta.values.mapK([Z] => (value: DbValue[Z]) => value.tpe)
        )
    end DeleteReturning

    case class Insert[A[_[_]], B[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        values: Query[A],
        columnsToSet: [F[_]] => A[F] => B[F] = [F[_]] => (a: A[F]) => a,
        onConflict: Option[A[[Z] =>> Option[(DbValue[Z], DbValue[Z]) => DbValue[Z]]]] = None
    ) extends Operation[Int]:
      override def run(using db: Db): Future[Int] =
        import table.FA
        val ret = for
          computedValues <- values.addMap(columnsToSet[DbValue]).selectAstAndValues
          computedOnConflict <- onConflict.traverse { doColumns =>
            columnsToSet(doColumns)
              .map2Const(columnsToSet(table.columns)) {
                [Z] =>
                  (doCol: Option[(DbValue[Z], DbValue[Z]) => DbValue[Z]], column: Column[Z]) =>
                    doCol.traverse { f =>
                      f(
                        DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe)),
                        DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe))
                      ).ast.map(column.name -> _)
                  }
              }
              .traverseK[TagState, Const[Option[(SqlStr, SqlExpr)]]](FunctionK.identity)
          }
        yield db.run(
          sqlRenderer.renderInsert(
            table.name,
            columnsToSet(table.columns).foldMapK([Z] => (column: Column[Z]) => List(column.name)),
            computedValues.ast,
            computedOnConflict.toList.flatMap(_.toListK.flatten),
            Nil
          )
        )

        ret.runA(freshTaggedState).value

      def columnsToSet[B1[_[_]]: ApplyKC: TraverseKC](f: [F[_]] => A[F] => B1[F]): Insert[A, B1] =
        copy(columnsToSet = f)

      def onConflict(a: A[[Z] =>> Option[(DbValue[Z], DbValue[Z]) => DbValue[Z]]]): Insert[A, B] =
        copy(onConflict = Some(a))

      def onConflictUpdate: Insert[A, B] =
        import table.FA
        onConflict(table.columns.mapK([Z] => (_: Column[Z]) => Some((oldV: DbValue[Z], newV: DbValue[Z]) => newV)))

    object Insert:
      def into[A[_[_]]](table: Table[A]): InsertInto[A] = InsertInto(table)

      def values[A[_[_]]](table: Table[A], value: A[Id], values: Seq[A[Id]] = Nil): Insert[A, A] =
        Insert.into(table).values(Query.valuesOf(table, value, values))

    end Insert

    case class InsertInto[A[_[_]]](table: Table[A]):

      def values(query: Query[A]): Insert[A, A] =
        import table.given
        Insert(table, query)
    end InsertInto

    case class InsertReturning[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        values: Query[A],
        columnsToSet: [F[_]] => A[F] => B[F] = [F[_]] => (a: A[F]) => a,
        onConflict: Option[A[[Z] =>> Option[(DbValue[Z], DbValue[Z]) => DbValue[Z]]]] = None,
        returning: A[DbValue] => C[DbValue]
    ) extends Operation[QueryResult[C[Id]]]:
      override def run(using db: Db): Future[QueryResult[C[Id]]] =
        import table.FA
        val ret = for
          computedValues <- values.addMap(columnsToSet[DbValue]).selectAstAndValues
          computedOnConflict <- onConflict.traverse { doColumns =>
            columnsToSet(doColumns)
              .map2Const(columnsToSet(table.columns)) {
                [Z] =>
                  (doCol: Option[(DbValue[Z], DbValue[Z]) => DbValue[Z]], column: Column[Z]) =>
                    doCol.traverse { f =>
                      f(
                        DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe)),
                        DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe))
                      ).ast.map(column.name -> _)
                  }
              }
              .traverseK[TagState, Const[Option[(SqlStr, SqlExpr)]]](FunctionK.identity)
          }
          returningValues = returning(
            table.columns.mapK(
              [Z] =>
                (col: Column[Z]) => DbValue.SqlDbValue(SqlDbValue.QueryColumn(col.nameStr, table.tableName, col.tpe))
            )
          )
          computedReturning <-
            returningValues.traverseK[TagState, Const[SqlExpr]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)
        yield db.runIntoRes(
          sqlRenderer.renderInsert(
            table.name,
            columnsToSet(table.columns).foldMapK([Z] => (column: Column[Z]) => List(column.name)),
            computedValues.ast,
            computedOnConflict.toList.flatMap(_.toListK.flatten),
            computedReturning.toListK
          ),
          returningValues.mapK([Z] => (dbVal: DbValue[Z]) => dbVal.tpe)
        )

        ret.runA(freshTaggedState).value
    end InsertReturning

    case class Update[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        from: Option[Query[B]],
        setValues: (A[DbValue], B[DbValue]) => A[DbValue],
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        columnsToSet: [F[_]] => A[F] => C[F] = [F[_]] => (a: A[F]) => a
    ) extends Operation[Int]:

      def columnsToSet[C1[_[_]]: ApplyKC: TraverseKC](f: [F[_]] => A[F] => C1[F]): Update[A, B, C1] =
        copy(columnsToSet = f)

      override def run(using db: Db): Future[Int] =
        import table.given
        val query = from match
          case Some(fromQ) =>
            Query.from(table).addFlatMap(a => fromQ.where(b => where(a, b)).addMap(b => setValues(a, b)))
          case None =>
            Query
              .from(table)
              .where(a => where(a, a.asInstanceOf[B[DbValue]]))
              .addMap(a => setValues(a, a.asInstanceOf[B[DbValue]]))

        val ret =
          for meta <- query.addMap(columnsToSet[DbValue]).selectAstAndValues
          yield db.run(
            sqlRenderer.renderUpdate(
              columnsToSet(table.columns).foldMapK([Z] => (col: Column[Z]) => List(col.name)),
              meta.ast,
              Nil
            )
          )

        ret.runA(freshTaggedState).value

    object Update:
      def table[A[_[_]]](table: Table[A]): UpdateTable[A, A] = UpdateTable(table, None)

    end Update

    case class UpdateTable[A[_[_]], B[_[_]]](table: Table[A], from: Option[Query[B]] = None):

      def from[B1[_[_]]](fromQ: Query[B1]): UpdateTable[A, B1] = UpdateTable(table, Some(fromQ))

      def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableWhere[A, B] =
        UpdateTableWhere(table, from, where)

    case class UpdateTableWhere[A[_[_]], B[_[_]]](
        table: Table[A],
        from: Option[Query[B]],
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ):

      def values(setValues: (A[DbValue], B[DbValue]) => A[DbValue]): Update[A, B, A] =
        import table.given
        Update(table, from, setValues, where)

    case class UpdateReturning[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC, D[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        from: Option[Query[B]],
        setValues: (A[DbValue], B[DbValue]) => A[DbValue],
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        columnsToSet: [F[_]] => A[F] => D[F] = [F[_]] => (a: A[F]) => a,
        returning: (A[DbValue], B[DbValue]) => C[DbValue]
    ) extends Operation[QueryResult[C[Id]]]:

      override def run(using db: Db): Future[QueryResult[C[Id]]] =
        import table.given
        given ApplyKC[B]    = from.fold(table.FA.asInstanceOf[ApplyKC[B]])(_.applyK)
        given TraverseKC[B] = from.fold(table.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

        given ApplyKC[InnerJoin[A, B]] with {
          extension [X[_], E](fa: (A[X], B[X]))
            def mapK[Y[_]](f: X ~>: Y): (A[Y], B[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))

            def map2K[Y[_], Z[_]](fb: (A[Y], B[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (A[Z], B[Z]) =
              (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
        }
        given TraverseKC[InnerJoin[A, B]] with {
          extension [X[_], E](fa: InnerJoin[A, B][X])
            def foldLeftK[Y](b: Y)(f: Y => X ~>#: Y): Y =
              val b1 = fa._1.foldLeftK(b)(f)
              fa._2.foldLeftK(b)(f)

            def traverseK[G[_]: Applicative, Y[_]](f: X ~>: Compose2[G, Y]): G[InnerJoin[A, B][Y]] =
              fa._1.traverseK(f).product(fa._2.traverseK(f))
        }

        val bothQuery = from match
          case Some(fromQ) => Query.from(table).addFlatMap[InnerJoin[A, B]](a => fromQ.addMap(b => (a, b)))
          case None        => Query.from(table).addMap[InnerJoin[A, B]](a => (a, a.asInstanceOf[B[DbValue]]))

        extension [X[_[_]]: TraverseKC](x: X[DbValue])
          def collectAst: TagState[X[Const[SqlExpr]]] =
            x.traverseK[TagState, Const[SqlExpr]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)

        val ret = for
          bothMeta <- bothQuery.selectAstAndValues
          whereAst <- where.tupled(bothMeta.values).ast
          toSetAst <- columnsToSet(setValues.tupled(bothMeta.values)).collectAst
          returningValues = returning.tupled(bothMeta.values)
          returningAst <- returningValues.collectAst
        yield db.runIntoRes(
          sqlRenderer.renderUpdate(
            columnsToSet(table.columns).foldMapK([Z] => (col: Column[Z]) => List(col.name)),
            bothMeta.ast.copy(
              data = bothMeta.ast.data match
                case data: SelectAst.Data.SelectFrom =>
                  data.copy(
                    selectExprs = toSetAst.toListK.map(ast => SelectAst.ExprWithAlias(ast, None)),
                    where = Some(whereAst)
                  )
                case _ => throw new IllegalStateException("Expected SelectFrom ast")
            ),
            returningAst.toListK
          ),
          returningValues.mapK([Z] => (dbVal: DbValue[Z]) => dbVal.tpe)
        )

        ret.runA(freshTaggedState).value

  export Operation.*
}
