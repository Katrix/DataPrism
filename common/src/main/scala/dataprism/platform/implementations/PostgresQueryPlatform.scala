package dataprism.platform.implementations

import scala.annotation.targetName
import scala.concurrent.Future

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
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
    case ArrayOf(values: Seq[DbValue[A]]) extends DbValue[Seq[A]]

    def ast: SqlExpr = this match
      case DbValue.SqlDbValue(value) => value.ast
      case DbValue.ArrayOf(values) =>
        SqlExpr.Custom(values.map(_.ast), args => sql"ARRAY[${args.intercalate(sql", ")}]")
    end ast

    def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case DbValue.SqlDbValue(res) => Some(res)
      case _                       => None

    def singletonArray: DbValue[Seq[A]] = DbValue.ArrayOf(Seq(this))
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

    def ast: Seq[SelectAst.OrderExpr] = this match
      case Ord.Asc(value)  => Seq(SelectAst.OrderExpr(value.ast, SelectAst.OrderDir.Asc, None))
      case Ord.Desc(value) => Seq(SelectAst.OrderExpr(value.ast, SelectAst.OrderDir.Desc, None))
  }

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq {
    def ast: Seq[SelectAst.OrderExpr] = init.ast ++ tail.ast
  }

  extension (ordSeq: OrdSeq) @targetName("ordSeqAndThen") def andThen(ord: Ord): OrdSeq = MultiOrdSeq(ordSeq, ord)

  extension [A](many: Many[A])
    def arrayAgg: DbValue[Seq[A]] =
      SqlDbValue
        .Function[Seq[A]](SqlExpr.FunctionName.Custom("array_agg"), Seq(many.asDbValue.asAnyDbVal))
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

  extension [A[_[_]]](sqlQueryGrouped: SqlQueryGrouped[A])
    def liftSqlQueryGrouped: QueryGrouped[A] = sqlQueryGrouped

  case class TaggedState(queryNum: Int, columnNum: Int) extends SqlTaggedState {
    override def withNewQueryNum(newQueryNum: Int): TaggedState = copy(queryNum = newQueryNum)

    override def withNewColumnNum(newColumnNum: Int): TaggedState = copy(columnNum = newColumnNum)
  }
  protected def freshTaggedState: TaggedState = TaggedState(0, 0)

  def select[Res[_[_]]](
      query: Query[Res]
  )(using db: Db, dbTypes: Res[DbType], FA: ApplyKC[Res], FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]] =
    db.runIntoRes(sqlRenderer.renderSelect(query.selectAstAndValues.runA(TaggedState(0, 0)).value._1))(
      using dbTypes,
      FA,
      FT
    )
}
