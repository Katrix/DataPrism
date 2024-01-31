package dataprism.platform.sql

import dataprism.sharedast.SelectAst

import scala.annotation.targetName

trait DefaultCompleteSqlQueryPlatform extends SqlQueryPlatform {

  override type UnaryOp[V, R] = SqlUnaryOp[V, R]
  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R] = op

  override type BinOp[LHS, RHS, R] = SqlBinOp[LHS, RHS, R]
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R] = op

  type DbValueCompanion = SqlDbValueCompanion
  val DbValue: DbValueCompanion = new SqlDbValueCompanion {}

  override type AnyDbValue = DbValue[Any]

  sealed trait OrdSeq extends SqlOrdSeqBase
  enum Ord extends OrdSeq:
    case Asc(value: AnyDbValue)
    case Desc(value: AnyDbValue)

    override def ast: TagState[Seq[SelectAst.OrderExpr[Codec]]] = this match
      case Ord.Asc(value) => value.ast.map(expr => Seq(SelectAst.OrderExpr(expr, SelectAst.OrderDir.Asc, None)))
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
        case _ => SqlValueSource.FromQuery(query)

  type Query[A[_[_]]] = SqlQuery[A]
  type QueryGrouped[A[_[_]]] = SqlQueryGrouped[A]

  val Query: QueryCompanion = new SqlQueryCompanion {}
  override type QueryCompanion = SqlQueryCompanion

  extension [A[_[_]]](sqlQuery: SqlQuery[A]) def liftSqlQuery: Query[A] = sqlQuery

  extension [A[_[_]]](sqlQueryGrouped: SqlQueryGrouped[A]) def liftSqlQueryGrouped: QueryGrouped[A] = sqlQueryGrouped

  override type CaseCompanion = DefaultSqlCaseCompanion
  override val Case: DefaultSqlCaseCompanion = new DefaultSqlCaseCompanion {}

  case class TaggedState(queryNum: Int, columnNum: Int) extends SqlTaggedState:
    override def withNewQueryNum(newQueryNum: Int): TaggedState = copy(queryNum = newQueryNum)

    override def withNewColumnNum(newColumnNum: Int): TaggedState = copy(columnNum = newColumnNum)
  end TaggedState

  protected def freshTaggedState: TaggedState = TaggedState(0, 0)

}
