package dataprism.platform.sql

import cats.data.State
import cats.syntax.all.*
import dataprism.platform.MapRes
import dataprism.platform.sql.value.SqlDbValuesBase
import dataprism.sharedast.{MergeAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlMergeOperations extends SqlOperationsBase, SqlDbValuesBase { platform =>

  class SqlMergeCompanion:
    def into[A[_[_]]](table: Table[Codec, A]): SqlMergeInto[A] = new SqlMergeInto(table)

  class SqlMergeInto[A[_[_]]](protected val table: Table[Codec, A]):
    def using[B[_[_]]](dataSource: Query[B]): SqlMergeIntoUsing[A, B] = new SqlMergeIntoUsing(table, dataSource)

  class SqlMergeIntoUsing[A[_[_]], B[_[_]]](protected val table: Table[Codec, A], protected val dataSource: Query[B]):
    def on(joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean]): SqlMergeIntoUsingOn[A, B] =
      new SqlMergeIntoUsingOn(table, dataSource, joinCondition)

  class SqlMergeIntoUsingOn[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ):
    def whenMatched: SqlMergeMatchedKeyword[A, B] =
      new SqlMergeMatchedKeyword(table, dataSource, joinCondition, Nil, None)
    def whenNotMatched: SqlMergeNotMatchedKeyword[A, B] =
      new SqlMergeNotMatchedKeyword(table, dataSource, joinCondition, Nil, None)

  enum SqlMergeMatch[A[_[_]], B[_[_]]]:
    case MatchUpdate[A1[_[_]], B1[_[_]], C[_[_]]](
        cond: Option[(A1[DbValue], B1[DbValue]) => DbValue[Boolean]],
        columns: A1[[X] =>> Column[Codec, X]] => C[[X] =>> Column[Codec, X]],
        setValues: (A1[DbValue], B1[DbValue]) => C[DbValue]
    )(using val CA: ApplyKC[C], val CT: TraverseKC[C]) extends SqlMergeMatch[A1, B1]
    case MatchDelete(cond: Option[(A[DbValue], B[DbValue]) => DbValue[Boolean]])
    case NotMatchInsert[A1[_[_]], B1[_[_]], C[_[_]]](
        cond: Option[B1[DbValue] => DbValue[Boolean]],
        columns: A1[[X] =>> Column[Codec, X]] => C[[X] =>> Column[Codec, X]],
        setValues: B1[DbValue] => C[DbValue]
    )(using val CA: ApplyKC[C], val CT: TraverseKC[C]) extends SqlMergeMatch[A1, B1]

  class SqlMergeMatchedKeyword[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val whens: Seq[SqlMergeMatch[A, B]],
      protected val cond: Option[(A[DbValue], B[DbValue]) => DbValue[Boolean]]
  ):
    def and(cond: (A[DbValue], B[DbValue]) => DbValue[Boolean]): SqlMergeMatchedKeyword[A, B] =
      new SqlMergeMatchedKeyword(
        table,
        dataSource,
        joinCondition,
        whens,
        Some(this.cond.fold(cond)(oldCond => (a, b) => oldCond(a, b) && cond(a, b)))
      )

    def thenUpdate: SqlMergeUpdateKeyword[A, B] =
      new SqlMergeUpdateKeyword(table, dataSource, joinCondition, whens, cond)

    def thenDelete: SqlMergeOperation[A, B] =
      new SqlMergeOperation(table, dataSource, joinCondition, whens :+ SqlMergeMatch.MatchDelete(cond))

  class SqlMergeUpdateKeyword[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val whens: Seq[SqlMergeMatch[A, B]],
      protected val cond: Option[(A[DbValue], B[DbValue]) => DbValue[Boolean]]
  ):
    def values(setValues: (A[DbValue], B[DbValue]) => A[DbValue]): SqlMergeOperation[A, B] =
      import table.given
      new SqlMergeOperation(
        table,
        dataSource,
        joinCondition,
        whens :+ SqlMergeMatch.MatchUpdate(cond, identity, setValues)
      )

    def valuesInColumnsK[C[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => C[[X] =>> Column[Codec, X]]
    )(
        setValues: (A[DbValue], B[DbValue]) => C[DbValue]
    ): SqlMergeOperation[A, B] =
      new SqlMergeOperation(
        table,
        dataSource,
        joinCondition,
        whens :+ SqlMergeMatch.MatchUpdate(cond, columns, setValues)
      )

    inline def valuesInColumns[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(setValues: (A[DbValue], B[DbValue]) => mr.K[DbValue]): SqlMergeOperation[A, B] =
      valuesInColumnsK(a => mr.toK(columns(a)))((a, b) => setValues(a, b))(using mr.applyKC, mr.traverseKC)

  class SqlMergeNotMatchedKeyword[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val whens: Seq[SqlMergeMatch[A, B]],
      protected val cond: Option[B[DbValue] => DbValue[Boolean]]
  ):
    def and(cond: B[DbValue] => DbValue[Boolean]): SqlMergeNotMatchedKeyword[A, B] =
      new SqlMergeNotMatchedKeyword(
        table,
        dataSource,
        joinCondition,
        whens,
        Some(this.cond.fold(cond)(oldCond => a => oldCond(a) && cond(a)))
      )

    def thenInsert: SqlMergeInsertKeyword[A, B] =
      new SqlMergeInsertKeyword(table, dataSource, joinCondition, whens, cond)

  class SqlMergeInsertKeyword[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val whens: Seq[SqlMergeMatch[A, B]],
      protected val cond: Option[B[DbValue] => DbValue[Boolean]]
  ):

    def values(setValues: B[DbValue] => A[DbValue]): SqlMergeOperation[A, B] =
      import table.given
      new SqlMergeOperation(
        table,
        dataSource,
        joinCondition,
        whens :+ SqlMergeMatch.NotMatchInsert(cond, identity, setValues)
      )

    def valuesInColumnsK[C[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => C[[X] =>> Column[Codec, X]]
    )(
        setValues: B[DbValue] => C[DbValue]
    ): SqlMergeOperation[A, B] =
      new SqlMergeOperation(
        table,
        dataSource,
        joinCondition,
        whens :+ SqlMergeMatch.NotMatchInsert(cond, columns, setValues)
      )

    inline def valuesInColumns[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(setValues: B[DbValue] => mr.K[DbValue]): SqlMergeOperation[A, B] =
      valuesInColumnsK(a => mr.toK(columns(a)))(b => setValues(b))(using mr.applyKC, mr.traverseKC)

  class SqlMergeOperation[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val dataSource: Query[B],
      protected val joinCondition: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val whens: Seq[SqlMergeMatch[A, B]]
  ) extends IntOperation:

    def whenMatched: SqlMergeMatchedKeyword[A, B] =
      new SqlMergeMatchedKeyword(table, dataSource, joinCondition, whens, None)

    def whenNotMatched: SqlMergeNotMatchedKeyword[A, B] =
      new SqlMergeNotMatchedKeyword(table, dataSource, joinCondition, whens, None)

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      val valuesQuery = Query.from(table).join(dataSource)(joinCondition)
      import valuesQuery.given

      val st = for
        astMetadata <- valuesQuery.selectAstAndValues
        whenAsts <- whens.traverse:
          case m: SqlMergeMatch.MatchUpdate[a, b, c] =>
            import m.given
            val usedColumns = m.CT.foldMapK[[X] =>> Column[Codec, X], Nothing](m.columns(table.columns))(
              [X] => (col: Column[Codec, X]) => List(col.name)
            )

            val newValues = m.setValues.tupled(astMetadata.values)

            for
              condAst <- m.cond.traverse(f => f.tupled(astMetadata.values).ast)
              values  <- m.CT.traverseConst[DbValue, Nothing](newValues)([Z] => (v: DbValue[Z]) => v.ast).map(_.toListK)
            yield MergeAst.When(not = false, condAst, MergeAst.WhenOperation.Update(usedColumns, values))

          case SqlMergeMatch.MatchDelete(cond) =>
            cond
              .traverse(f => f.tupled(astMetadata.values).ast)
              .map(condAst => MergeAst.When(not = false, condAst, MergeAst.WhenOperation.Delete()))

          case m: SqlMergeMatch.NotMatchInsert[a, b, c] =>
            import m.given
            val usedColumns = m.CT.foldMapK[[X] =>> Column[Codec, X], Nothing](m.columns(table.columns))(
              [X] => (col: Column[Codec, X]) => List(col.name)
            )
            val bValues = astMetadata.values._2

            val newValues = m.setValues(bValues)

            for
              condAst <- m.cond.traverse(f => f(bValues).ast)
              values  <- m.CT.traverseConst[DbValue, Nothing](newValues)([Z] => (v: DbValue[Z]) => v.ast).map(_.toListK)
            yield MergeAst.When(not = true, condAst, MergeAst.WhenOperation.Insert(usedColumns, values))
      yield MergeAst(astMetadata.ast, whenAsts)

      (sqlRenderer.renderMerge(st.runA(freshTaggedState).value), AnsiTypes.integer)
  end SqlMergeOperation

  type OperationCompanion <: SqlOperationCompanion & SqlMergeOperationsCompanion
  trait SqlMergeOperationsCompanion {
    val Merge: SqlMergeCompanion = new SqlMergeCompanion
  }

  type Api <: SqlOperationApi & SqlMergeApi & SqlQueryApi & SqlDbValueApi & QueryApi
  trait SqlMergeApi {
    inline def Merge: platform.SqlMergeCompanion = Operation.Merge
  }
}
