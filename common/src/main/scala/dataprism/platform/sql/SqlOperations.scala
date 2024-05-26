package dataprism.platform.sql

import cats.data.{NonEmptyList, NonEmptySeq}
import cats.syntax.all.*
import cats.{Applicative, Functor, MonadThrow}
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlOperations extends SqlOperationsBase { platform: SqlQueryPlatform =>

  class SqlSelectCompanionImpl extends SqlSelectCompanion:
    def apply[Res[_[_]]](query: Query[Res]): SelectOperation[Res] = new SqlSelectOperationImpl(query).lift

  given sqlSelectCompanionLift: Lift[SqlSelectCompanionImpl, SelectCompanion]

  class SqlSelectOperationImpl[Res[_[_]]](protected val query: Query[Res])
      extends SqlSelectOperation[Res],
        ResultOperation[Res](using query.applyK, query.traverseK):

    override def sqlAndTypes: (SqlStr[Codec], Res[Type]) =
      import query.given

      given FunctorKC[Res] = summon[ApplyKC[Res]]
      val astMeta          = query.selectAstAndValues.runA(freshTaggedState).value
      (
        sqlRenderer.renderSelectStatement(astMeta.ast),
        astMeta.values.mapK([Z] => (value: DbValue[Z]) => value.tpe)
      )
  end SqlSelectOperationImpl

  given sqlSelectOperationLift[A[_[_]]]: Lift[SqlSelectOperationImpl[A], SelectOperation[A]]

  protected def generateDeleteAlias: Boolean = true

  class SqlDeleteOperationImpl[A[_[_]], B[_[_]]](
      protected val from: Table[Codec, A],
      protected val usingV: Option[Query[B]] = None,
      protected val where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlDeleteOperation[A, B]:
    given ApplyKC[B]    = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)
    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import from.given
      val fromQ =
        SqlQuery
          .SqlQueryFromStage(SqlValueSource.FromTable(from, generateDeleteAlias).liftSqlValueSource)
          .liftSqlQuery

      // Code duplicated for not needing casts
      val ast = usingV match
        case Some(usingQ) =>
          fromQ
            .crossJoin(usingQ)
            .filter((a, b) => where(a, b))
            .selectAstAndValues
            .runA(freshTaggedState)
            .value
            .ast
        case None =>
          fromQ
            .where(a => where(a, a.asInstanceOf[B[DbValue]]))
            .selectAstAndValues
            .runA(freshTaggedState)
            .value
            .ast

      (
        sqlRenderer.renderDelete(ast, returning = false),
        AnsiTypes.integer.notNull
      )
    end sqlAndTypes

    override def returningK[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    )(using DeleteReturningCapability): DeleteReturningOperation[A, B, C] =
      new SqlDeleteReturningOperationImpl(from, usingV, where, f).lift
  end SqlDeleteOperationImpl

  given sqlDeleteOperationLift[A[_[_]], B[_[_]]]: Lift[SqlDeleteOperationImpl[A, B], DeleteOperation[A, B]]

  class SqlDeleteReturningOperationImpl[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      protected val from: Table[Codec, A],
      protected val usingV: Option[Query[B]] = None,
      protected val where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      protected val returning: (A[DbValue], B[DbValue]) => C[DbValue]
  ) extends SqlDeleteReturningOperation[A, B, C]:

    given ApplyKC[B] = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)

    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Codec], C[Type]) =
      given FunctorKC[C] = summon[ApplyKC[C]]
      import from.given
      val fromQ =
        SqlQuery
          .SqlQueryFromStage(SqlValueSource.FromTable(from, generateDeleteAlias).liftSqlValueSource)
          .liftSqlQuery

      val q = usingV match
        case Some(usingQ) =>
          fromQ.crossJoin(usingQ).where((a, b) => where(a, b)).mapK((a, b) => returning(a, b))
        case None =>
          fromQ
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
  end SqlDeleteReturningOperationImpl

  given sqlDeleteReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]: Lift[SqlDeleteReturningOperationImpl[A, B, C], DeleteReturningOperation[A, B, C]]

  class SqlDeleteCompanionImpl extends SqlDeleteCompanion:
    def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A] = SqlDeleteFromImpl(from).lift
  end SqlDeleteCompanionImpl

  given sqlDeleteCompanionLift: Lift[SqlDeleteCompanionImpl, DeleteCompanion]

  class SqlDeleteFromImpl[A[_[_]]](protected val from: Table[Codec, A]) extends SqlDeleteFrom[A]:
    def using[B[_[_]]](query: Query[B])(using DeleteUsingCapability): DeleteFromUsing[A, B] =
      SqlDeleteFromUsingImpl(from, query).lift
    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A] =
      SqlDeleteOperationImpl[A, A](from, None, (a, _) => f(a)).lift
  end SqlDeleteFromImpl

  given sqlDeleteFromLift[A[_[_]]]: Lift[SqlDeleteFromImpl[A], DeleteFrom[A]]

  class SqlDeleteFromUsingImpl[A[_[_]], B[_[_]]](protected val from: Table[Codec, A], val query: Query[B])
      extends SqlDeleteFromUsing[A, B]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B] =
      SqlDeleteOperationImpl(from, Some(query), f).lift
  end SqlDeleteFromUsingImpl

  given sqlDeleteFromUsingLift[A[_[_]], B[_[_]]]: Lift[SqlDeleteFromUsingImpl[A, B], DeleteFromUsing[A, B]]

  class SqlInsertOperationImpl[A[_[_]], B[_[_]]](
      protected val table: Table[Codec, A],
      protected val columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      protected val values: Query[B],
      protected val conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, ?]],
      protected val onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
  ) extends SqlInsertOperation[A, B]:
    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import values.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(columns(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]], column: Column[Codec, Z]) =>
                f(
                  SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe).lift,
                  SqlDbValue.QueryColumn(column.nameStr, "excluded", column.tpe).lift
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
        on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, ?]],
        a: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
    )(using InsertOnConflictCapability): InsertOperation[A, B] =
      SqlInsertOperationImpl(table, columns, values, on(_).toList, a).lift

    def onConflictUpdate(on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, ?]])(
        using InsertOnConflictCapability
    ): InsertOperation[A, B] =
      import values.given_ApplyKC_A
      onConflict(
        on,
        columns(table.columns).mapK(
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], newV: DbValue[Z]) => Some(newV): Option[DbValue[Z]]
        )
      )

    def returning[C[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => C[DbValue])(
        using InsertReturningCapability
    ): InsertReturningOperation[A, B, C] =
      SqlInsertReturningOperationImpl(table, columns, values, conflictOn, onConflict, f).lift
  end SqlInsertOperationImpl

  given sqlInsertOperationLift[A[_[_]], B[_[_]]]: Lift[SqlInsertOperationImpl[A, B], InsertOperation[A, B]]

  class SqlInsertReturningOperationImpl[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      protected val table: Table[Codec, A],
      protected val columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      protected val values: Query[B],
      protected val conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, ?]],
      protected val onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]],
      protected val returning: A[DbValue] => C[DbValue]
  ) extends SqlInsertReturningOperation[A, B, C]:

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
                  SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe).lift,
                  SqlDbValue.QueryColumn(column.nameStr, "excluded", column.tpe).lift
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Codec], SqlExpr[Codec])]]](FunctionK.identity)
        returningValues = returning(
          table.columns.mapK(
            [Z] => (col: Column[Codec, Z]) => SqlDbValue.QueryColumn(col.nameStr, table.tableName, col.tpe).lift
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
  end SqlInsertReturningOperationImpl

  given sqlInsertReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]: Lift[SqlInsertReturningOperationImpl[A, B, C], InsertReturningOperation[A, B, C]]

  class SqlInsertCompanionImpl extends SqlInsertCompanion:
    def into[A[_[_]]](table: Table[Codec, A]): InsertInto[A] = SqlInsertIntoImpl(table).lift
  end SqlInsertCompanionImpl

  given sqlInsertCompanionLift: Lift[SqlInsertCompanionImpl, InsertCompanion]

  class SqlInsertIntoImpl[A[_[_]]](protected val table: Table[Codec, A]) extends SqlInsertInto[A]:

    override def valuesInColumnsFromQueryK[B[_[_]]](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        query: Query[B]
    ): InsertOperation[A, B] =
      import query.given_ApplyKC_A
      SqlInsertOperationImpl[A, B](
        table,
        columns,
        query,
        _ => Nil,
        columns(table.columns).mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      ).lift

    override def valuesFromQuery(query: Query[A]): InsertOperation[A, A] =
      import table.given
      SqlInsertOperationImpl(
        table,
        identity,
        query,
        _ => Nil,
        table.columns.mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      ).lift

    override def values(value: A[Id], values: A[Id]*): InsertOperation[A, A] =
      valuesFromQuery(Query.valuesOf(table, value, values*))

    override def valuesBatch(value: A[Id], values: A[Id]*)(using DistributiveKC[A]): InsertOperation[A, A] =
      valuesFromQuery(Query.valuesOfBatch(table, value, values*))

    override def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B] =
      valuesInColumnsFromQueryK(columns)(
        Query.valuesK[B](columns(table.columns).mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values*)
      )

    override def valuesInColumnsKBatch[B[_[_]]: ApplyKC: TraverseKC: DistributiveKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B] =
      valuesInColumnsFromQueryK(columns)(
        Query.valuesKBatch[B](columns(table.columns).mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values*)
      )

    override def valuesInColumns[T](
        columns: A[[X] =>> Column[Codec, X]] => T
    )(using mr: MapRes[[X] =>> Column[Codec, X], T])(value: mr.K[Id], values: mr.K[Id]*): InsertOperation[A, mr.K] =
      given FunctorKC[mr.K] = mr.applyKC

      valuesInColumnsFromQueryK(a => mr.toK(columns(a)))(
        Query.valuesK[mr.K](
          mr.toK(columns(table.columns)).mapK([Z] => (col: Column[Codec, Z]) => col.tpe),
          value,
          values*
        )(using mr.applyKC, mr.traverseKC)
      )

    override def valuesInColumnsKBatch[T](
        columns: A[[X] =>> Column[Codec, X]] => T
    )(using mr: MapRes[[X] =>> Column[Codec, X], T])(value: mr.K[Id], values: mr.K[Id]*)(
        using D: DistributiveKC[mr.K]
    ): InsertOperation[A, mr.K] =
      given FunctorKC[mr.K] = mr.applyKC

      valuesInColumnsFromQueryK(a => mr.toK(columns(a)))(
        Query.valuesKBatch[mr.K](
          mr.toK(columns(table.columns)).mapK([Z] => (col: Column[Codec, Z]) => col.tpe),
          value,
          values*
        )(using mr.applyKC, mr.traverseKC, D)
      )
  end SqlInsertIntoImpl

  given sqlInsertIntoLift[A[_[_]]]: Lift[SqlInsertIntoImpl[A], InsertInto[A]]

  protected def generateUpdateAlias: Boolean = true

  protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res

  class SqlUpdateOperationImpl[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      protected val table: Table[Codec, A],
      protected val columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      protected val from: Option[Query[C]],
      protected val setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      protected val where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B, C]:

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import table.given
      val tableQ =
        SqlQuery
          .SqlQueryFromStage(SqlValueSource.FromTable(table, generateDeleteAlias).liftSqlValueSource)
          .liftSqlQuery

      val query = from match
        case Some(fromQ) =>
          tableQ.crossJoin(fromQ).filter((a, b) => where(a, b)).mapK((a, b) => setValues(a, b))
        case None =>
          tableQ
            .where(a => where(a, a.asInstanceOf[C[DbValue]]))
            .mapK(a => setValues(a, a.asInstanceOf[C[DbValue]]))

      val ret =
        for meta <- query.selectAstAndValues
        yield sqlRenderer.renderUpdate(
          columns(table.columns).foldMapK([Z] => (col: Column[Codec, Z]) => List(col.name)),
          meta.ast,
          Nil
        )

      (ret.runA(freshTaggedState).value, AnsiTypes.integer.notNull)
    end sqlAndTypes

    def returningK[D[_[_]]: ApplyKC: TraverseKC](
        f: MapUpdateReturning[A[DbValue], C[DbValue], D[DbValue]]
    )(using UpdateReturningCapability): UpdateReturningOperation[A, B, C, D] =
      SqlUpdateReturningOperationImpl(table, columns, from, setValues, where, contramapUpdateReturning(f)).lift

    def returning[T](
        f: MapUpdateReturning[A[DbValue], C[DbValue], T]
    )(using mr: MapRes[DbValue, T], cap: UpdateReturningCapability): UpdateReturningOperation[A, B, C, mr.K] =
      given ApplyKC[mr.K]    = mr.applyKC
      given TraverseKC[mr.K] = mr.traverseKC
      SqlUpdateReturningOperationImpl(
        table,
        columns,
        from,
        setValues,
        where,
        (a, b) => mr.toK(contramapUpdateReturning(f)(a, b))
      ).lift
  end SqlUpdateOperationImpl

  given sqlUpdateOperationLift[A[_[_]], B[_[_]], C[_[_]]]: Lift[SqlUpdateOperationImpl[A, B, C], UpdateOperation[A, B, C]]

  class SqlUpdateReturningOperationImpl[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]], D[_[_]]: ApplyKC: TraverseKC](
      protected val table: Table[Codec, A],
      protected val columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      protected val from: Option[Query[C]],
      protected val setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      protected val where: (A[DbValue], C[DbValue]) => DbValue[Boolean],
      protected val returning: (A[DbValue], C[DbValue]) => D[DbValue]
  ) extends SqlUpdateReturningOperation[A, B, C, D]:

    override def sqlAndTypes: (SqlStr[Codec], D[Type]) = {
      import table.given
      val tableQ =
        SqlQuery
          .SqlQueryFromStage(SqlValueSource.FromTable(table, generateDeleteAlias).liftSqlValueSource)
          .liftSqlQuery

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
        case Some(fromQ) => tableQ.crossJoin(fromQ)
        case None        => tableQ.mapK[InnerJoin[A, C]](a => (a, a.asInstanceOf[C[DbValue]]))

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
  end SqlUpdateReturningOperationImpl

  given sqlUpdateReturningOperationLift[A[_[_]], B[_[_]], C[_[_]], D[_[_]]]: Lift[SqlUpdateReturningOperationImpl[A, B, C, D], UpdateReturningOperation[A, B, C, D]]

  class SqlUpdateCompanionImpl extends SqlUpdateCompanion:
    def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A] = SqlUpdateTableImpl(table).lift

  given sqlUpdateCompanionLift: Lift[SqlUpdateCompanionImpl, UpdateCompanion]

  class SqlUpdateTableImpl[A[_[_]]](protected val table: Table[Codec, A]) extends SqlUpdateTable[A]:
    def from[B[_[_]]](fromQ: Query[B])(using UpdateFromCapability): UpdateTableFrom[A, B] =
      SqlUpdateTableFromImpl(table, fromQ).lift

    def where(where: A[DbValue] => DbValue[Boolean]): UpdateTableWhere[A] = SqlUpdateTableWhereImpl(table, where).lift

  given sqlUpdateTableLift[A[_[_]]]: Lift[SqlUpdateTableImpl[A], UpdateTable[A]]

  class SqlUpdateTableFromImpl[A[_[_]], C[_[_]]](protected val table: Table[Codec, A], protected val from: Query[C])
      extends SqlUpdateTableFrom[A, C]:
    def where(where: (A[DbValue], C[DbValue]) => DbValue[Boolean]): UpdateTableFromWhere[A, C] =
      SqlUpdateTableFromWhereImpl(table, from, where).lift

  given sqlUpdateTableFromLift[A[_[_]], C[_[_]]]: Lift[SqlUpdateTableFromImpl[A, C], UpdateTableFrom[A, C]]

  class SqlUpdateTableWhereImpl[A[_[_]]](
      protected val table: Table[Codec, A],
      protected val where: A[DbValue] => DbValue[Boolean]
  ) extends SqlUpdateTableWhere[A]:
    def values(setValues: A[DbValue] => A[DbValue]): UpdateOperation[A, A, A] =
      import table.given
      SqlUpdateOperationImpl[A, A, A](table, identity, None, (a, _) => setValues(a), (a, _) => where(a)).lift

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: A[DbValue] => B[DbValue]
    ): UpdateOperation[A, B, A] =
      SqlUpdateOperationImpl[A, B, A](table, columns, None, (a, _) => setValues(a), (a, _) => where(a)).lift

  given sqlUpdateTableWhereLift[A[_[_]]]: Lift[SqlUpdateTableWhereImpl[A], UpdateTableWhere[A]]

  class SqlUpdateTableFromWhereImpl[A[_[_]], C[_[_]]](
      protected val table: Table[Codec, A],
      protected val from: Query[C],
      protected val where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateTableFromWhere[A, C]:
    def values(setValues: (A[DbValue], C[DbValue]) => A[DbValue]): UpdateOperation[A, A, C] =
      import table.given
      SqlUpdateOperationImpl[A, A, C](table, identity, Some(from), setValues, where).lift

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: (A[DbValue], C[DbValue]) => B[DbValue]
    ): UpdateOperation[A, B, C] = SqlUpdateOperationImpl[A, B, C](table, columns, Some(from), setValues, where).lift

  given sqlUpdateTableFromWhereLift[A[_[_]], C[_[_]]]: Lift[SqlUpdateTableFromWhereImpl[A, C], UpdateTableFromWhere[A, C]]

  trait SqlOperationCompanionImpl extends SqlOperationCompanion:
    val Select: SelectCompanion = new SqlSelectCompanionImpl().lift
    val Delete: DeleteCompanion = new SqlDeleteCompanionImpl().lift
    val Insert: InsertCompanion = new SqlInsertCompanionImpl().lift
    val Update: UpdateCompanion = new SqlUpdateCompanionImpl().lift
  end SqlOperationCompanionImpl

  trait SqlCompileImpl extends SqlCompile:
    protected def simple[A[_[_]]: ApplyKC: TraverseKC, B](types: A[Type])(f: A[DbValue] => B)(
        doReplacement: (B, Map[Object, Seq[Any]]) => B
    ): A[Id] => B =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] =
        types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues =
        tpesWithIdentifiers.mapK(
          [Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).lift
        )
      val b = f(dbValues)

      (values: A[Id]) => {
        val replacements =
          values
            .map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, Seq(v: Any)))
            .toListK
            .toMap
        doReplacement(b, replacements)
      }

    def rawK[A[_[_]]: ApplyKC: TraverseKC](types: A[Type])(f: A[DbValue] => SqlStr[Codec]): A[Id] => SqlStr[Codec] =
      simple(types)(f)(_.compileWithValues(_))

    def operationK[A[_[_]]: ApplyKC: TraverseKC, B, F[_]: MonadThrow](types: A[Type])(f: A[DbValue] => Operation[B])(
        using db: Db[F, Codec]
    ): A[Id] => F[B] =
      simple(types)(f.andThen(op => (op, op.sqlAndTypes))) { case ((op, (rawSqlStr, resultTypes)), replacements) =>
        (op, (rawSqlStr.compileWithValues(replacements), resultTypes))
      }.andThen { case (op, (sqlStr, resultTypes)) =>
        op.runWithSqlAndTypes(sqlStr, resultTypes.asInstanceOf[op.Types])
      }
}
