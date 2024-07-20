package dataprism.platform.sql

import scala.annotation.targetName

import dataprism.sharedast.{SelectAst, SqlExpr}

trait DefaultCompleteSql extends SqlQueryPlatform {

  type DbValueCompanion = SqlDbValueCompanion
  val DbValue: DbValueCompanion = new SqlDbValueCompanionImpl {}

  override type AnyDbValue = DbValue[Any]

  type Impl <: DefaultCompleteImpl
  trait DefaultCompleteImpl extends SqlBaseImpl, SqlDbValueImpl, SqlQueriesImpl:
    override def asc[A](v: DbValue[A]): Ord                       = Ord.Asc(v.asAnyDbVal)
    override def desc[A](v: DbValue[A]): Ord                      = Ord.Desc(v.asAnyDbVal)
    override def unsafeAsAnyDbVal[A](v: DbValue[A]): DbValue[Any] = v.asInstanceOf[DbValue[Any]]

  sealed trait OrdSeq extends SqlOrdSeqBase
  enum Ord extends OrdSeq:
    case Asc(value: AnyDbValue)
    case Desc(value: AnyDbValue)

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
  type ValueSourceCompanion = SqlValueSourceCompanionImpl
  object ValueSource extends SqlValueSourceCompanionImpl

  extension [A[_[_]]](sqlValueSource: SqlValueSource[A]) def liftSqlValueSource: ValueSource[A] = sqlValueSource

  type Query[A[_[_]]]        = SqlQuery[A]
  type QueryGrouped[A[_[_]]] = SqlQueryGrouped[A]

  val Query: QueryCompanion = new SqlQueryCompanionImpl {}
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
