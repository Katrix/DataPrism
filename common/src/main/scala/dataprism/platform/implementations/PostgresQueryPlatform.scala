package dataprism.platform.implementations

import scala.annotation.targetName
import scala.concurrent.Future
import cats.Applicative
import cats.data.{NonEmptyList, State}
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.{AstRenderer, PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
class PostgresQueryPlatform extends SqlQueryPlatform { platform =>

  val sqlRenderer: PostgresAstRenderer = new PostgresAstRenderer

  override type UnaryOp[V, R] = SqlUnaryOp[V, R]

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R] = op

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op

  enum DbValue[A] extends SqlDbValueBase[A] {
    case SqlDbValue(value: platform.SqlDbValue[A])
    case ArrayOf(values: Seq[DbValue[A]], elemType: DbType[A], mapping: DbType.ArrayMapping[A]) extends DbValue[Seq[A]]

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
      case DbValue.ArrayOf(_, tpe, mapping) => DbType.array(tpe)(using mapping)
    end tpe

    def singletonArray(using mapping: DbType.ArrayMapping[A]): DbValue[Seq[A]] = DbValue.ArrayOf(Seq(this), tpe, mapping)

    override def liftDbValue: DbValue[A] = this

    override def asc: Ord = Ord.Asc(this)

    override def desc: Ord = Ord.Desc(this)

    override protected[platform] def asAnyDbVal: AnyDbValue = this.asInstanceOf[AnyDbValue]
  }

  override protected[platform] def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = DbValue.SqlDbValue(a)

  override type AnyDbValue = DbValue[Any]

  sealed trait OrdSeq extends SqlOrdSeqBase

  enum Ord extends OrdSeq {
    case Asc(value: DbValue[_])
    case Desc(value: DbValue[_])

    override def ast: TagState[Seq[SelectAst.OrderExpr]] = this match
      case Ord.Asc(value)  => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Asc, None)))
      case Ord.Desc(value) => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Desc, None)))

    override def andThen(ord: Ord): OrdSeq = MultiOrdSeq(this, ord)
  }

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq {
    override def ast: TagState[Seq[SelectAst.OrderExpr]] = init.ast.flatMap(i => tail.ast.map(t => i ++ t))

    override def andThen(ord: Ord): OrdSeq = MultiOrdSeq(this, ord)
  }

  extension [A](many: Many[A])
    def arrayAgg(using DbType.ArrayMapping[A]): DbValue[Seq[A]] =
      SqlDbValue
        .Function[Seq[A]](
          SqlExpr.FunctionName.Custom("array_agg"),
          Seq(many.asDbValue.asAnyDbVal),
          DbType.array(many.asDbValue.tpe)
        )
        .lift

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

    case class Insert[A[_[_]]](
        table: Table[A],
        values: Query[Insert.Optional[A]],
        conflictOn: A[Column] => List[Column[_]],
        onConflict: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]]
    ) extends Operation[Int]:
      override def run(using db: Db): Future[Int] =
        import table.given

        val ret = for
          computedValues <- values.selectAstAndValues
          computedOnConflict <- onConflict
            .map2Const(computedValues.values.tupledK(table.columns)) {
              [Z] =>
                (f: (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]], t: (Option[DbValue[Z]], Column[Z])) =>
                  val value  = t._1
                  val column = t._2

                  f(
                    DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                    value.map(_ => DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe)))
                  ).traverse(r => r.ast.map(column.name -> _))
            }
            .traverseK[TagState, Const[Option[(SqlStr, SqlExpr)]]](FunctionK.identity)
        yield db.run(
          sqlRenderer.renderInsert(
            table.name,
            table.columns
              .map2Const(computedValues.values)(
                [Z] => (column: Column[Z], opt: Option[DbValue[Z]]) => opt.map(_ => column.name)
              )
              .toListK
              .flatMap(_.toList),
            computedValues.ast,
            conflictOn(table.columns).map(_.name),
            computedOnConflict.toListK.flatMap(_.toList),
            Nil
          )
        )

        ret.runA(freshTaggedState).value

      def onConflict(on: A[Column] => NonEmptyList[Column[_]], a: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]]): Insert[A] =
        copy(conflictOn = on.andThen(_.toList), onConflict = a)

      def onConflictUpdate(on: A[Column] => NonEmptyList[Column[_]]): Insert[A] =
        import table.FA
        onConflict(on, table.columns.mapK([Z] => (_: Column[Z]) => (_: DbValue[Z], newV: Option[DbValue[Z]]) => newV))

      def returning[B[_[_]] : ApplyKC : TraverseKC](f: A[DbValue] => B[DbValue]): InsertReturning[A, B] =
        InsertReturning(table, values, conflictOn, onConflict, f)

    object Insert:
      def into[A[_[_]]](table: Table[A]): InsertInto[A] = InsertInto(table)

      def values[A[_[_]]](table: Table[A], value: A[Id], values: Seq[A[Id]] = Nil): Insert[A] =
        Insert.into(table).values(Query.valuesOf(table, value, values))

      type Optional[A[_[_]]] = [F[_]] =>> A[Compose2[Option, F]]

      given optValuesInstance[A[_[_]]](
          using FA: ApplyKC[A],
          FT: TraverseKC[A]
      ): ApplyKC[Optional[A]] with TraverseKC[Optional[A]] with {
        extension [B[_], D](fa: A[Compose2[Option, B]])
          def map2K[C[_], Z[_]](fb: A[Compose2[Option, C]])(f: [X] => (B[X], C[X]) => Z[X]): A[Compose2[Option, Z]] =
            FA.map2K(fa)(fb)([X] => (v1o: Option[B[X]], v2o: Option[C[X]]) => v1o.zip(v2o).map((v1, v2) => f(v1, v2)))

          def traverseK[G[_]: Applicative, C[_]](f: B ~>: Compose2[G, C]): G[A[Compose2[Option, C]]] =
            FT.traverseK(fa)([Z] => (vo: Option[B[Z]]) => vo.traverse[G, C[Z]](v => f(v)))

          def foldLeftK[C](b: C)(f: C => B ~>#: C): C =
            FT.foldLeftK(fa)(b)(b1 => [Z] => (vo: Option[B[Z]]) => vo.fold(b1)(v => f(b1)(v)))
      }

    end Insert

    case class InsertInto[A[_[_]]](table: Table[A]):

      def values(query: Query[A]): Insert[A] =
        import table.given
        given FunctorKC[A] = table.FA
        given (ApplyKC[Insert.Optional[A]] & TraverseKC[Insert.Optional[A]]) =
          Insert.optValuesInstance[A]

        Insert(
          table,
          query.mapK[[F[_]] =>> A[Compose2[Option, F]]](a => a.mapK([Z] => (dbVal: DbValue[Z]) => Some(dbVal))),
          _ => Nil,
          table.columns.mapK[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]](
            [Z] => (_: Column[Z]) => (_: DbValue[Z], _: Option[DbValue[Z]]) => None: Option[DbValue[Z]]
          )
        )

      def valuesWithoutSomeColumns(query: Query[[F[_]] =>> A[Compose2[Option, F]]]): Insert[A] =
        given FunctorKC[A] = table.FA
        Insert(
          table,
          query,
          _ => Nil,
          table.columns.mapK[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]](
            [Z] => (_: Column[Z]) => (_: DbValue[Z], _: Option[DbValue[Z]]) => None: Option[DbValue[Z]]
          )
        )
    end InsertInto

    case class InsertReturning[A[_[_]], C[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        values: Query[Insert.Optional[A]],
        conflictOn: A[Column] => List[Column[_]],
        onConflict: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]],
        returning: A[DbValue] => C[DbValue]
    ) extends Operation[QueryResult[C[Id]]]:
      override def run(using db: Db): Future[QueryResult[C[Id]]] =
        import table.given

        val ret = for
          computedValues <- values.selectAstAndValues
          computedOnConflict <- onConflict
            .map2Const(computedValues.values.tupledK(table.columns)) {
              [Z] =>
                (f: (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]], t: (Option[DbValue[Z]], Column[Z])) =>
                  val value  = t._1
                  val column = t._2

                  f(
                    DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                    value.map(_ => DbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe)))
                  ).traverse(r => r.ast.map(column.name -> _))
            }
            .traverseK[TagState, Const[Option[(SqlStr, SqlExpr)]]](FunctionK.identity)
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
            table.columns
              .map2Const(computedValues.values)(
                [Z] => (column: Column[Z], opt: Option[DbValue[Z]]) => opt.map(_ => column.name)
              )
              .toListK
              .flatMap(_.toList),
            computedValues.ast,
            conflictOn(table.columns).map(_.name),
            computedOnConflict.toListK.flatMap(_.toList),
            computedReturning.toListK
          ),
          returningValues.mapK([Z] => (dbVal: DbValue[Z]) => dbVal.tpe)
        )

        ret.runA(freshTaggedState).value
    end InsertReturning

    case class Update[A[_[_]], B[_[_]]](
        table: Table[A],
        from: Option[Query[B]],
        setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends Operation[Int]:

      def returning[C[_[_]]: ApplyKC: TraverseKC](f: (A[DbValue], B[DbValue]) => C[DbValue]): UpdateReturning[A, B, C] =
        UpdateReturning(table, from, setValues, where, f)

      override def run(using db: Db): Future[Int] =
        import table.given
        given (ApplyKC[Insert.Optional[A]] & TraverseKC[Insert.Optional[A]]) =
          Insert.optValuesInstance[A]

        val query = from match
          case Some(fromQ) =>
            Query.from(table).flatMap(a => fromQ.where(b => where(a, b)).mapK[Insert.Optional[A]](b => setValues(a, b)))
          case None =>
            Query
              .from(table)
              .where(a => where(a, a.asInstanceOf[B[DbValue]]))
              .mapK[Insert.Optional[A]](a => setValues(a, a.asInstanceOf[B[DbValue]]))

        val ret =
          for meta <- query.selectAstAndValues
          yield db.run(
            sqlRenderer.renderUpdate(
              table.columns
                .map2Const(meta.values)([Z] => (col: Column[Z], v: Option[DbValue[Z]]) => v.map(_ => col.name).toList)
                .toListK
                .flatten,
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

      def values(setValues: (A[DbValue], B[DbValue]) => A[DbValue]): Update[A, B] =
        import table.given
        given FunctorKC[A] = table.FA
        Update(
          table,
          from,
          (a, b) => setValues(a, b).mapK([Z] => (v: DbValue[Z]) => Some(v): Option[DbValue[Z]]),
          where
        )

      def someValues(setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]]): Update[A, B] =
        import table.given
        Update(table, from, setValues, where)

    case class UpdateReturning[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
        table: Table[A],
        from: Option[Query[B]],
        setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
        where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
        returning: (A[DbValue], B[DbValue]) => C[DbValue]
    ) extends Operation[QueryResult[C[Id]]]:

      override def run(using db: Db): Future[QueryResult[C[Id]]] =
        import table.given
        given ApplyKC[B]    = from.fold(table.FA.asInstanceOf[ApplyKC[B]])(_.applyK)
        given TraverseKC[B] = from.fold(table.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

        given (ApplyKC[Insert.Optional[A]] & TraverseKC[Insert.Optional[A]]) =
          Insert.optValuesInstance[A]

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
          case Some(fromQ) => Query.from(table).flatMap(a => fromQ.mapK[InnerJoin[A, B]](b => (a, b)))
          case None        => Query.from(table).mapK[InnerJoin[A, B]](a => (a, a.asInstanceOf[B[DbValue]]))

        val ret = for
          bothMeta <- bothQuery.selectAstAndValues
          whereAst <- where.tupled(bothMeta.values).ast
          toSet = setValues.tupled(bothMeta.values)
          toSetAst <- table.FT.traverseK(toSet)[TagState, Const[Option[SqlExpr]]](
            [Z] => (vo: Option[DbValue[Z]]) => vo.traverse(v => v.ast)
          )
          returningValues = returning.tupled(bothMeta.values)
          returningAst <- returningValues.traverseK[TagState, Const[SqlExpr]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)
        yield db.runIntoRes(
          sqlRenderer.renderUpdate(
            table.columns
              .map2Const(toSet)([Z] => (col: Column[Z], v: Option[DbValue[Z]]) => v.map(_ => col.name).toList)
              .toListK
              .flatten,
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
