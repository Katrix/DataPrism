package dataprism.platform.implementations

import scala.annotation.targetName

import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.{MySqlAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait MySqlQueryPlatform extends SqlQueryPlatform { platform =>

  val sqlRenderer: MySqlAstRenderer[Codec] = new MySqlAstRenderer[Codec](AnsiTypes)

  override type UnaryOp[V, R] = SqlUnaryOp[V, R]

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R] = op

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op

  type DbValue[A] = MySqlDbValue[A]
  enum MySqlDbValue[A] extends SqlDbValueBase[A]:
    case SqlDbValue(value: platform.SqlDbValue[A])

    override def ast: TagState[SqlExpr[Codec]] = this match
      case MySqlDbValue.SqlDbValue(v) => v.ast

    override def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case MySqlDbValue.SqlDbValue(v) => Some(v)

    override def tpe: Type[A] = this match
      case MySqlDbValue.SqlDbValue(v) => v.tpe

    override def columnName(prefix: String): String = this match
      case MySqlDbValue.SqlDbValue(v) => v.columnName(prefix)

    override def unsafeAsAnyDbVal: DbValue[Any] = this.asInstanceOf[DbValue[Any]]
    override def liftDbValue: DbValue[A]        = this
    override def asc: Ord                       = Ord.Asc(this)
    override def desc: Ord                      = Ord.Desc(this)
  end MySqlDbValue

  type DbValueCompanion = SqlDbValueCompanion
  val DbValue: DbValueCompanion = new SqlDbValueCompanion {}

  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = MySqlDbValue.SqlDbValue(a)

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
  ) extends SqlDeleteOperation[A, B](from, usingV, where)

  case class InsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B]
  ) extends SqlInsertOperation[A, B](table, columns, values)

  case class UpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B, C](table, columns, from, setValues, where)

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
      InsertOperation(table, identity, query)

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B] =
      InsertOperation(table, columns, query)
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
