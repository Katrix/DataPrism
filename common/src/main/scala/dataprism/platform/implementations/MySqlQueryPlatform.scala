package dataprism.platform.implementations

import scala.annotation.targetName
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.{MySqlAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait MySqlQueryPlatform extends SqlQueryPlatform { platform =>

  val sqlRenderer: MySqlAstRenderer[Type] = new MySqlAstRenderer[Type](AnsiTypes)

  override type UnaryOp[V, R] = SqlUnaryOp[V, R]

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R] = op

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op
  
  enum DbValue[A] extends SqlDbValueBase[A]:
    case SqlDbValue(value: platform.SqlDbValue[A])

    override def ast: TagState[SqlExpr[Type]] = this match
      case DbValue.SqlDbValue(v) => v.ast

    override def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case DbValue.SqlDbValue(v) => Some(v)

    override def tpe: Type[A] = this match
      case DbValue.SqlDbValue(v) => v.tpe

    override def unsafeAsAnyDbVal: DbValue[Any] = this.asInstanceOf[DbValue[Any]]
    override def liftDbValue: DbValue[A] = this
    override def asc: Ord = Ord.Asc(this)
    override def desc: Ord = Ord.Desc(this)


  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = DbValue.SqlDbValue(a)

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
  ) extends SqlDeleteOperation[A, B](from, usingV, where)

  case class InsertOperation[A[_[_]]](table: Table[A, Type], values: Query[Optional[A]])
      extends SqlInsertOperation[A](table, values)

  case class UpdateOperation[A[_[_]], B[_[_]]](
      table: Table[A, Type],
      from: Option[Query[B]],
      setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B](table, from, setValues, where)

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
        query.mapK[[F[_]] =>> A[Compose2[Option, F]]](a => a.mapK([Z] => (dbVal: DbValue[Z]) => Some(dbVal)))
      )

    def valuesWithoutSomeColumns(query: Query[[F[_]] =>> A[Compose2[Option, F]]]): InsertOperation[A] =
      given FunctorKC[A] = table.FA
      InsertOperation(table, query)
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
