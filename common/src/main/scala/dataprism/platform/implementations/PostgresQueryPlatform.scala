package dataprism.platform.implementations

import scala.annotation.targetName

import cats.Applicative
import cats.data.NonEmptyList
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.{PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
trait PostgresQueryPlatform extends SqlQueryPlatform { platform =>

  val sqlRenderer: PostgresAstRenderer[Type] = new PostgresAstRenderer[Type](AnsiTypes)
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

    def ast: TagState[SqlExpr[Type]] = this match
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

    override def ast: TagState[Seq[SelectAst.OrderExpr[Type]]] = this match
      case Ord.Asc(value)  => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Asc, None)))
      case Ord.Desc(value) => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Desc, None)))

    override def andThen(ord: Ord): OrdSeq = MultiOrdSeq(this, ord)
  end Ord

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq:
    override def ast: TagState[Seq[SelectAst.OrderExpr[Type]]] = init.ast.flatMap(i => tail.ast.map(t => i ++ t))

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
      from: Table[A, Type],
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
      from: Table[A, Type],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], B[DbValue]) => C[DbValue]
  ) extends ResultOperation[C]:

    given ApplyKC[B] = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Type], C[Type]) =
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

  case class InsertOperation[A[_[_]]](
      table: Table[A, Type],
      values: Query[Optional[A]],
      conflictOn: A[[Z] =>> Column[Z, Type]] => List[Column[_, Type]],
      onConflict: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]]
  ) extends SqlInsertOperation[A](table, values):
    override def sqlAndTypes: (SqlStr[Type], Type[Int]) =
      import table.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(computedValues.values.tupledK(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]], t: (Option[DbValue[Z]], Column[Z, Type])) =>
                val value  = t._1
                val column = t._2

                f(
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                  value.map(_ =>
                    PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe))
                  )
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Type], SqlExpr[Type])]]](FunctionK.identity)
      yield sqlRenderer.renderInsert(
        table.name,
        table.columns
          .map2Const(computedValues.values)(
            [Z] => (column: Column[Z, Type], opt: Option[DbValue[Z]]) => opt.map(_ => column.name)
          )
          .toListK
          .flatMap(_.toList),
        computedValues.ast,
        conflictOn(table.columns).map(_.name),
        computedOnConflict.toListK.flatMap(_.toList),
        Nil
      )

      (ret.runA(freshTaggedState).value, AnsiTypes.integer)

    def onConflict(
        on: A[[Z] =>> Column[Z, Type]] => NonEmptyList[Column[_, Type]],
        a: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]]
    ): InsertOperation[A] =
      copy(conflictOn = on.andThen(_.toList), onConflict = a)

    def onConflictUpdate(on: A[[Z] =>> Column[Z, Type]] => NonEmptyList[Column[_, Type]]): InsertOperation[A] =
      import table.FA
      onConflict(
        on,
        table.columns.mapK([Z] => (_: Column[Z, Type]) => (_: DbValue[Z], newV: Option[DbValue[Z]]) => newV)
      )

    def returning[B[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => B[DbValue]): InsertReturningOperation[A, B] =
      InsertReturningOperation(table, values, conflictOn, onConflict, f)

  case class InsertReturningOperation[A[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      table: Table[A, Type],
      values: Query[Optional[A]],
      conflictOn: A[[Z] =>> Column[Z, Type]] => List[Column[_, Type]],
      onConflict: A[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]],
      returning: A[DbValue] => C[DbValue]
  ) extends ResultOperation[C]:

    override type Types = C[Type]

    override def sqlAndTypes: (SqlStr[Type], C[Type]) =
      import table.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(computedValues.values.tupledK(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]], t: (Option[DbValue[Z]], Column[Z, Type])) =>
                val value  = t._1
                val column = t._2

                f(
                  PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe)),
                  value.map(_ =>
                    PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe))
                  )
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Type], SqlExpr[Type])]]](FunctionK.identity)
        returningValues = returning(
          table.columns.mapK(
            [Z] =>
              (col: Column[Z, Type]) =>
                PostgresDbValue.SqlDbValue(SqlDbValue.QueryColumn(col.nameStr, table.tableName, col.tpe))
          )
        )
        computedReturning <-
          returningValues.traverseK[TagState, Const[SqlExpr[Type]]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)
      yield (
        sqlRenderer.renderInsert(
          table.name,
          table.columns
            .map2Const(computedValues.values)(
              [Z] => (column: Column[Z, Type], opt: Option[DbValue[Z]]) => opt.map(_ => column.name)
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
  end InsertReturningOperation

  case class UpdateOperation[A[_[_]], B[_[_]]](
      table: Table[A, Type],
      from: Option[Query[B]],
      setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B](table, from, setValues, where):
    def returning[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    ): UpdateReturningOperation[A, B, C] =
      UpdateReturningOperation(table, from, setValues, where, f)

  case class UpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      table: Table[A, Type],
      from: Option[Query[B]],
      setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], B[DbValue]) => C[DbValue]
  ) extends ResultOperation[C]:

    override def sqlAndTypes: (SqlStr[Type], C[Type]) = {
      import table.given
      given ApplyKC[B] = from.fold(table.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

      given TraverseKC[B] = from.fold(table.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

      given (ApplyKC[Optional[A]] & TraverseKC[Optional[A]]) =
        optValuesInstance[A]

      given ApplyKC[InnerJoin[A, B]] with {
        extension [X[_], E](fa: (A[X], B[X]))
          def mapK[Y[_]](f: X :~>: Y): (A[Y], B[Y]) =
            (fa._1.mapK(f), fa._2.mapK(f))

          def map2K[Y[_], Z[_]](fb: (A[Y], B[Y]))(f: [W] => (X[W], Y[W]) => Z[W]): (A[Z], B[Z]) =
            (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))
      }
      given TraverseKC[InnerJoin[A, B]] with {
        extension [X[_], E](fa: InnerJoin[A, B][X])
          def foldLeftK[Y](b: Y)(f: Y => X :~>#: Y): Y =
            val b1 = fa._1.foldLeftK(b)(f)
            fa._2.foldLeftK(b1)(f)

          def foldRightK[Y](b: Y)(f: X :~>#: (Y => Y)): Y =
            val b1 = fa._2.foldRightK(b)(f)
            fa._1.foldRightK(b1)(f)

          def traverseK[G[_]: Applicative, Y[_]](f: X :~>: Compose2[G, Y]): G[InnerJoin[A, B][Y]] =
            Applicative[G].product(
              fa._1.traverseK(f),
              fa._2.traverseK(f)
            )
      }

      val bothQuery = from match
        case Some(fromQ) => Query.from(table).flatMap(a => fromQ.mapK[InnerJoin[A, B]](b => (a, b)))
        case None        => Query.from(table).mapK[InnerJoin[A, B]](a => (a, a.asInstanceOf[B[DbValue]]))

      val ret = for
        bothMeta <- bothQuery.selectAstAndValues
        whereAst <- where.tupled(bothMeta.values).ast
        toSet = setValues.tupled(bothMeta.values)
        toSetAst <- table.FT.traverseK(toSet)[TagState, Const[Option[SqlExpr[Type]]]](
          [Z] => (vo: Option[DbValue[Z]]) => vo.traverse(v => v.ast)
        )
        returningValues = returning.tupled(bothMeta.values)
        returningAst <- returningValues.traverseK[TagState, Const[SqlExpr[Type]]](
          [Z] => (dbVal: DbValue[Z]) => dbVal.ast
        )
      yield (
        sqlRenderer.renderUpdate(
          table.columns
            .map2Const(toSet)([Z] => (col: Column[Z, Type], v: Option[DbValue[Z]]) => v.map(_ => col.name).toList)
            .toListK
            .flatten,
          bothMeta.ast match
            case data: SelectAst.SelectFrom[Type] =>
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
    override def from[A[_[_]]](from: Table[A, Type]): DeleteFrom[A, A] = DeleteFrom(from)

  case class DeleteFrom[A[_[_]], B[_[_]]](from: Table[A, Type], using: Option[Query[B]] = None)
      extends SqlDeleteFrom[A, B](from, using):
    def using[B1[_[_]]](query: Query[B1]): DeleteFrom[A, B1] = DeleteFrom(from, Some(query))

    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B] = DeleteOperation(from, using, f)
  end DeleteFrom

  trait InsertCompanion extends SqlInsertCompanion:
    override def into[A[_[_]]](table: Table[A, Type]): InsertInto[A] = InsertInto(table)

  case class InsertInto[A[_[_]]](table: Table[A, Type]) extends SqlInsertInto[A]:

    def values(query: Query[A]): InsertOperation[A] =
      import table.given
      given FunctorKC[A] = table.FA

      given (ApplyKC[Optional[A]] & TraverseKC[Optional[A]]) =
        optValuesInstance[A]

      InsertOperation(
        table,
        query.mapK[[F[_]] =>> A[Compose2[Option, F]]](a => a.mapK([Z] => (dbVal: DbValue[Z]) => Some(dbVal))),
        _ => Nil,
        table.columns.mapK[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Z, Type]) => (_: DbValue[Z], _: Option[DbValue[Z]]) => None: Option[DbValue[Z]]
        )
      )

    def valuesWithoutSomeColumns(query: Query[[F[_]] =>> A[Compose2[Option, F]]]): InsertOperation[A] =
      given FunctorKC[A] = table.FA

      InsertOperation(
        table,
        query,
        _ => Nil,
        table.columns.mapK[[Z] =>> (DbValue[Z], Option[DbValue[Z]]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Z, Type]) => (_: DbValue[Z], _: Option[DbValue[Z]]) => None: Option[DbValue[Z]]
        )
      )
  end InsertInto

  trait UpdateCompanion extends SqlUpdateCompanion:
    override def table[A[_[_]]](table: Table[A, Type]): UpdateTable[A, A] = UpdateTable(table)

  case class UpdateTable[A[_[_]], B[_[_]]](table: Table[A, Type], from: Option[Query[B]] = None)
      extends SqlUpdateTable[A, B]:

    def from[B1[_[_]]](fromQ: Query[B1]): UpdateTable[A, B1] = UpdateTable(table, Some(fromQ))

    def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableWhere[A, B] =
      UpdateTableWhere(table, from, where)
  end UpdateTable

  case class UpdateTableWhere[A[_[_]], B[_[_]]](
      table: Table[A, Type],
      from: Option[Query[B]],
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateTableWhere[A, B]:

    def values(setValues: (A[DbValue], B[DbValue]) => A[DbValue]): UpdateOperation[A, B] =
      import table.given
      given FunctorKC[A] = table.FA

      UpdateOperation(
        table,
        from,
        (a, b) => setValues(a, b).mapK([Z] => (v: DbValue[Z]) => Some(v): Option[DbValue[Z]]),
        where
      )

    def someValues(setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]]): UpdateOperation[A, B] =
      UpdateOperation(table, from, setValues, where)

  object Operation extends OperationCompanion
  trait OperationCompanion extends SqlOperationCompanion:
    override def Select[Res[_[_]]](query: SqlQuery[Res]): SelectOperation[Res] = SelectOperation(query)

    object Delete extends DeleteCompanion

    object Insert extends InsertCompanion

    object Update extends UpdateCompanion
  end OperationCompanion

  export Operation.*
}
