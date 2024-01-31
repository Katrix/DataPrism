package dataprism.platform.implementations

import scala.annotation.targetName
import cats.Applicative
import cats.data.NonEmptyList
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.platform.sql.{SqlQueryPlatform, UnsafeSqlQueryPlatformFlatmap}
import dataprism.sharedast.{PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
trait PostgresQueryPlatform extends SqlQueryPlatform with UnsafeSqlQueryPlatformFlatmap { platform =>

  val sqlRenderer: PostgresAstRenderer[Codec] = new PostgresAstRenderer[Codec](AnsiTypes)
  type ArrayTypeArgs[A]
  protected def arrayType[A](elemType: Type[A])(using extraArrayTypeArgs: ArrayTypeArgs[A]): Type[Seq[A]]

  override type UnaryOp[V, R] = SqlUnaryOp[V, R]

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R] = op

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op

  override type CastType[A] = Type[A]

  extension [A](t: CastType[A])
    override def castTypeName: String  = t.name
    override def castTypeType: Type[A] = t

  type DbValue[A] = PostgresDbValue[A]
  enum PostgresDbValue[A] extends SqlDbValueBase[A]:
    case SqlDbValue(value: platform.SqlDbValue[A])
    case ArrayOf(values: Seq[DbValue[A]], elemType: Type[A], extraArrayTypeArgs: ArrayTypeArgs[A])
        extends DbValue[Seq[A]]

    def ast: TagState[SqlExpr[Codec]] = this match
      case PostgresDbValue.SqlDbValue(value) => value.ast
      case PostgresDbValue.ArrayOf(values, _, _) =>
        values.toList
          .traverse(_.ast)
          .map(exprs => SqlExpr.Custom(exprs, args => sql"ARRAY[${args.intercalate(sql", ")}]"))
    end ast

    def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case PostgresDbValue.SqlDbValue(res) => Some(res)
      case _                               => None

    override def tpe: Type[A] = this match
      case PostgresDbValue.SqlDbValue(value)                   => value.tpe
      case PostgresDbValue.ArrayOf(_, tpe, extraArrayTypeArgs) => arrayType(tpe)(using extraArrayTypeArgs)
    end tpe

    override def columnName(prefix: String): String = this match
      case PostgresDbValue.SqlDbValue(v)         => v.columnName(prefix)
      case PostgresDbValue.ArrayOf(values, _, _) => s"${values.headOption.fold(prefix)(_.columnName(prefix))}_array"

    def singletonArray(using extraArrayTypeArgs: ArrayTypeArgs[A]): DbValue[Seq[A]] =
      PostgresDbValue.ArrayOf(Seq(this), tpe, extraArrayTypeArgs)

    override def liftDbValue: DbValue[A] = this

    override def asc: Ord = Ord.Asc(this)

    override def desc: Ord = Ord.Desc(this)

    override def unsafeAsAnyDbVal: AnyDbValue = this.asInstanceOf[AnyDbValue]
  end PostgresDbValue

  type DbValueCompanion = SqlDbValueCompanion
  val DbValue: DbValueCompanion = new SqlDbValueCompanion {}

  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = PostgresDbValue.SqlDbValue(a)

  override type AnyDbValue = DbValue[Any]

  sealed trait OrdSeq extends SqlOrdSeqBase

  enum Ord extends OrdSeq:
    case Asc(value: DbValue[_])
    case Desc(value: DbValue[_])

    override def ast: TagState[Seq[SelectAst.OrderExpr[Codec]]] = this match
      case Ord.Asc(value)  => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Asc, None)))
      case Ord.Desc(value) => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Desc, None)))

    override def andThen(ord: Ord): OrdSeq = MultiOrdSeq(this, ord)
  end Ord

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq:
    override def ast: TagState[Seq[SelectAst.OrderExpr[Codec]]] = init.ast.flatMap(i => tail.ast.map(t => i ++ t))

    override def andThen(ord: Ord): OrdSeq = MultiOrdSeq(this, ord)
  end MultiOrdSeq

  extension [A](many: Many[A])
    def arrayAgg(using extraArrayTypeArgs: ArrayTypeArgs[A]): DbValue[Seq[A]] =
      SqlDbValue
        .Function[Seq[A]](
          SqlExpr.FunctionName.Custom("array_agg"),
          Seq(many.unsafeAsDbValue.unsafeAsAnyDbVal),
          arrayType(many.unsafeAsDbValue.tpe)
        )
        .lift

  type ValueSource[A[_[_]]] = SqlValueSource[A]
  type ValueSourceCompanion = SqlValueSource.type
  val ValueSource: ValueSourceCompanion = SqlValueSource

  extension [A[_[_]]](sqlValueSource: SqlValueSource[A]) def liftSqlValueSource: ValueSource[A] = sqlValueSource

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

  override type CaseCompanion = DefaultSqlCaseCompanion
  override val Case: DefaultSqlCaseCompanion = new DefaultSqlCaseCompanion {}

  case class TaggedState(queryNum: Int, columnNum: Int) extends SqlTaggedState:
    override def withNewQueryNum(newQueryNum: Int): TaggedState = copy(queryNum = newQueryNum)

    override def withNewColumnNum(newColumnNum: Int): TaggedState = copy(columnNum = newColumnNum)
  end TaggedState
  protected def freshTaggedState: TaggedState = TaggedState(0, 0)

  case class SelectOperation[Res[_[_]]](query: Query[Res])
      extends SqlSelectOperation[Res](query)
      with ResultOperation[Res](using query.applyK, query.traverseK)

  case class DeleteOperation[A[_[_]], B[_[_]]](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlDeleteOperation[A, B](from, usingV, where):
    def returningK[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    ): DeleteReturningOperation[A, B, C] = DeleteReturningOperation(from, usingV, where, f)

    inline def returning[C](f: (A[DbValue], B[DbValue]) => C)(
        using MR: MapRes[DbValue, C]
    ): DeleteReturningOperation[A, B, MR.K] =
      returningK((a, b) => MR.toK(f(a, b)))(using MR.applyKC, MR.traverseKC)

  case class DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], B[DbValue]) => C[DbValue]
  ) extends ResultOperation[C]:

    given ApplyKC[B] = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Codec], C[Type]) =
      given FunctorKC[C] = summon[ApplyKC[C]]

      val q = usingV match
        case Some(usingQ) => Query.from(from).flatMap(a => usingQ.where(b => where(a, b)).mapK(b => returning(a, b)))
        case None =>
          Query
            .from(from)
            .where(a => where(a, a.asInstanceOf[B[DbValue]]))
            .mapK(a => returning(a, a.asInstanceOf[B[DbValue]]))

      val astMeta = q.selectAstAndValues.runA(freshTaggedState).value

      (
        sqlRenderer.renderDelete(
          astMeta.ast,
          returning = true
        ),
        astMeta.values.mapK([Z] => (value: DbValue[Z]) => value.tpe)
      )
  end DeleteReturningOperation

  case class InsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B],
      conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, _]],
      onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
  ) extends SqlInsertOperation[A, B](table, columns, values):
    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import table.given
      import values.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(columns(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]], column: Column[Codec, Z]) =>
                f(
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe))
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Codec], SqlExpr[Codec])]]](FunctionK.identity)
      yield sqlRenderer.renderInsert(
        table.name,
        columns(table.columns).foldMapK([X] => (col: Column[Codec, X]) => List(col.name)),
        computedValues.ast,
        conflictOn(table.columns).map(_.name),
        computedOnConflict.toListK.flatMap(_.toList),
        Nil
      )

      (ret.runA(freshTaggedState).value, AnsiTypes.integer.notNull)

    def onConflict(
        on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]],
        a: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
    ): InsertOperation[A, B] =
      copy(conflictOn = on.andThen(_.toList), onConflict = a)

    def onConflictUpdate(on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]]): InsertOperation[A, B] =
      import values.given_ApplyKC_A
      onConflict(
        on,
        columns(table.columns).mapK(
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], newV: DbValue[Z]) => Some(newV): Option[DbValue[Z]]
        )
      )

    def returning[C[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => C[DbValue]): InsertReturningOperation[A, B, C] =
      InsertReturningOperation(table, columns, values, conflictOn, onConflict, f)

  case class InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B],
      conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, _]],
      onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]],
      returning: A[DbValue] => C[DbValue]
  ) extends ResultOperation[C]:

    override type Types = C[Type]

    override def sqlAndTypes: (SqlStr[Codec], C[Type]) =
      import table.given
      import values.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(columns(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]], column: Column[Codec, Z]) =>
                f(
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe))
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Codec], SqlExpr[Codec])]]](FunctionK.identity)
        returningValues = returning(
          table.columns.mapK(
            [Z] =>
              (col: Column[Codec, Z]) =>
                PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(col.nameStr, table.tableName, col.tpe))
          )
        )
        computedReturning <-
          returningValues.traverseK[TagState, Const[SqlExpr[Codec]]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)
      yield (
        sqlRenderer.renderInsert(
          table.name,
          columns(table.columns).foldMapK([X] => (col: Column[Codec, X]) => List(col.name)),
          computedValues.ast,
          conflictOn(table.columns).map(_.name),
          computedOnConflict.toListK.flatMap(_.toList),
          computedReturning.toListK
        ),
        returningValues.mapK([Z] => (dbVal: DbValue[Z]) => dbVal.tpe)
      )

      ret.runA(freshTaggedState).value
  end InsertReturningOperation

  case class UpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B, C](table, columns, from, setValues, where):
    def returning[D[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], C[DbValue]) => D[DbValue]
    ): UpdateReturningOperation[A, B, C, D] =
      UpdateReturningOperation(table, columns, from, setValues, where, f)

  case class UpdateReturningOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]], D[_[_]]: ApplyKC: TraverseKC](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], C[DbValue]) => D[DbValue]
  ) extends ResultOperation[D]:

    override def sqlAndTypes: (SqlStr[Codec], D[Type]) = {
      import table.given
      given ApplyKC[C] = from.fold(table.FA.asInstanceOf[ApplyKC[C]])(_.applyK)

      given TraverseKC[C] = from.fold(table.FT.asInstanceOf[TraverseKC[C]])(_.traverseK)

      given ApplyKC[InnerJoin[A, C]] with {
        extension [X[_], E](fa: (A[X], C[X]))
          def mapK[Y[_]](f: X :~>: Y): (A[Y], C[Y]) =
            (fa._1.mapK(f), fa._2.mapK(f))

          def map2K[Y[_], Z[_]](fb: (A[Y], C[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (A[Z], C[Z]) =
            (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
      }
      given TraverseKC[InnerJoin[A, C]] with {
        extension [X[_], E](fa: InnerJoin[A, C][X])
          def foldLeftK[Y](b: Y)(f: Y => X :~>#: Y): Y =
            val b1 = fa._1.foldLeftK(b)(f)
            fa._2.foldLeftK(b1)(f)

          def foldRightK[Y](b: Y)(f: X :~>#: (Y => Y)): Y =
            val b1 = fa._2.foldRightK(b)(f)
            fa._1.foldRightK(b1)(f)

          def traverseK[G[_]: Applicative, Y[_]](f: X :~>: Compose2[G, Y]): G[InnerJoin[A, C][Y]] =
            Applicative[G].product(
              fa._1.traverseK(f),
              fa._2.traverseK(f)
            )
      }

      val bothQuery = from match
        case Some(fromQ) => Query.from(table).flatMap(a => fromQ.mapK[InnerJoin[A, C]](b => (a, b)))
        case None        => Query.from(table).mapK[InnerJoin[A, C]](a => (a, a.asInstanceOf[C[DbValue]]))

      val ret = for
        bothMeta <- bothQuery.selectAstAndValues
        whereAst <- where.tupled(bothMeta.values).ast
        toSet = setValues.tupled(bothMeta.values)
        toSetAst <- toSet.traverseK[TagState, Const[SqlExpr[Codec]]](
          [Z] => (v: DbValue[Z]) => v.ast
        )
        returningValues = returning.tupled(bothMeta.values)
        returningAst <- returningValues.traverseK[TagState, Const[SqlExpr[Codec]]](
          [Z] => (v: DbValue[Z]) => v.ast
        )
      yield (
        sqlRenderer.renderUpdate(
          columns(table.columns).foldMapK([Z] => (col: Column[Codec, Z]) => List(col.name)),
          bothMeta.ast match
            case data: SelectAst.SelectFrom[Codec] =>
              data.copy(
                selectExprs = toSetAst.toListK.map(ast => SelectAst.ExprWithAlias(ast, None)),
                where = Some(whereAst)
              )
            case _ => throw new IllegalStateException("Expected SelectFrom ast")
          ,
          returningAst.toListK
        ),
        returningValues.mapK([Z] => (dbVal: DbValue[Z]) => dbVal.tpe)
      )

      ret.runA(freshTaggedState).value
    }
  end UpdateReturningOperation

  trait DeleteCompanion extends SqlDeleteCompanion:
    override def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A] = DeleteFrom(from)

  case class DeleteFrom[A[_[_]]](from: Table[Codec, A]) extends SqlDeleteFrom[A]:
    def using[B[_[_]]](query: Query[B]): DeleteFromUsing[A, B] = DeleteFromUsing(from, query)

    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A] = DeleteOperation(from, None, (a, _) => f(a))
  end DeleteFrom

  case class DeleteFromUsing[A[_[_]], B[_[_]]](from: Table[Codec, A], using: Query[B]) extends SqlDeleteFromUsing[A, B]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B] =
      DeleteOperation(from, Some(`using`), f)
  end DeleteFromUsing

  trait InsertCompanion extends SqlInsertCompanion:
    override def into[A[_[_]]](table: Table[Codec, A]): InsertInto[A] = InsertInto(table)

  case class InsertInto[A[_[_]]](table: Table[Codec, A]) extends SqlInsertInto[A]:

    def valuesFromQuery(query: Query[A]): InsertOperation[A, A] =
      import table.given
      InsertOperation(
        table,
        identity,
        query,
        _ => Nil,
        table.columns.mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      )

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B] =
      import table.given
      import query.given_ApplyKC_A
      InsertOperation(
        table,
        columns,
        query,
        _ => Nil,
        columns(table.columns).mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      )
  end InsertInto

  trait UpdateCompanion extends SqlUpdateCompanion:
    override def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A] = UpdateTable(table)

  case class UpdateTable[A[_[_]]](table: Table[Codec, A]) extends SqlUpdateTable[A]:

    def from[B[_[_]]](fromQ: Query[B]): UpdateTableFrom[A, B] = UpdateTableFrom(table, fromQ)

    def where(where: A[DbValue] => DbValue[Boolean]): UpdateTableWhere[A] =
      UpdateTableWhere(table, where)
  end UpdateTable

  case class UpdateTableFrom[A[_[_]], B[_[_]]](table: Table[Codec, A], from: Query[B]) extends SqlUpdateTableFrom[A, B]:
    override def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableFromWhere[A, B] =
      UpdateTableFromWhere(table, from, where)

  case class UpdateTableWhere[A[_[_]]](
      table: Table[Codec, A],
      where: A[DbValue] => DbValue[Boolean]
  ) extends SqlUpdateTableWhere[A]:

    def values(setValues: A[DbValue] => A[DbValue]): UpdateOperation[A, A, A] =
      import table.given
      given FunctorKC[A] = table.FA

      UpdateOperation(
        table,
        identity,
        None,
        (a, _) => setValues(a),
        (a, _) => where(a)
      )

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: A[DbValue] => B[DbValue]
    ): UpdateOperation[A, B, A] = UpdateOperation(table, columns, None, (a, _) => setValues(a), (a, _) => where(a))

  case class UpdateTableFromWhere[A[_[_]], C[_[_]]](
      table: Table[Codec, A],
      from: Query[C],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateTableFromWhere[A, C]:

    def values(setValues: (A[DbValue], C[DbValue]) => A[DbValue]): UpdateOperation[A, A, C] =
      import table.given
      given FunctorKC[A] = table.FA

      UpdateOperation(
        table,
        identity,
        Some(from),
        (a, b) => setValues(a, b),
        where
      )

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: (A[DbValue], C[DbValue]) => B[DbValue]
    ): UpdateOperation[A, B, C] = UpdateOperation(table, columns, Some(from), setValues, where)

  object Operation extends OperationCompanion
  trait OperationCompanion extends SqlOperationCompanion:
    override def Select[Res[_[_]]](query: SqlQuery[Res]): SelectOperation[Res] = SelectOperation(query)

    object Delete extends DeleteCompanion

    object Insert extends InsertCompanion

    object Update extends UpdateCompanion
  end OperationCompanion

  export Operation.*
}
