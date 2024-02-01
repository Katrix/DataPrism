package dataprism.platform.sql

import cats.data.{NonEmptyList, NonEmptySeq}
import cats.syntax.all.*
import cats.{Applicative, Functor}
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformOperation { platform: SqlQueryPlatform =>

  sealed trait Operation[A]:
    type Types

    def sqlAndTypes: (SqlStr[Codec], Types)

    def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Types)(using db: Db[F, Codec]): F[A]

    def run[F[_]](using Db[F, Codec]): F[A] =
      val (sqlStr, types) = sqlAndTypes
      runWithSqlAndTypes(sqlStr, types)
  end Operation

  trait IntOperation extends Operation[Int]:
    type Types = Type[Int]

    override def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Type[Int])(using db: Db[F, Codec]): F[Int] =
      db.run(sqlStr)

  trait ResultOperation[Res[_[_]]](
      using val resApplyK: ApplyKC[Res],
      val resTraverseK: TraverseKC[Res]
  ) extends Operation[Seq[Res[Id]]]:
    type Types = Res[Type]

    private def runWithMinMax[F[_]](minRows: Int, maxRows: Int)(using db: Db[F, Codec]): F[Seq[Res[Id]]] =
      val (sqlStr, types) = sqlAndTypes
      db.runIntoRes(sqlStr, types.mapK([X] => (tpe: Type[X]) => tpe.codec), minRows, maxRows)

    override def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Res[Type])(
        using db: Db[F, Codec]
    ): F[Seq[Res[Id]]] =
      db.runIntoRes(sqlStr, types.mapK([X] => (tpe: Type[X]) => tpe.codec))

    def runOne[F[_]: Functor](using db: Db[F, Codec]): F[Res[Id]] = runWithMinMax(1, 1).map(_.head)

    def runMaybeOne[F[_]: Functor](using db: Db[F, Codec]): F[Option[Res[Id]]] = runWithMinMax(0, 1).map(_.headOption)

    def runOneOrMore[F[_]: Functor](using db: Db[F, Codec]): F[NonEmptySeq[Res[Id]]] =
      runWithMinMax(1, -1).map(s => NonEmptySeq.fromSeqUnsafe(s))

  type SelectOperation[Res[_[_]]] <: SqlSelectOperation[Res]

  type SelectCompanion <: SqlSelectCompanion

  trait SqlSelectCompanion:
    def apply[Res[_[_]]](query: Query[Res]): SelectOperation[Res]

  trait SqlSelectOperation[Res[_[_]]](query: Query[Res]) extends ResultOperation[Res]:
    override def sqlAndTypes: (SqlStr[Codec], Res[Type]) =
      import query.given

      given FunctorKC[Res] = summon[ApplyKC[Res]]
      val astMeta          = query.selectAstAndValues.runA(freshTaggedState).value
      (
        sqlRenderer.renderSelect(astMeta.ast),
        astMeta.values.mapK([Z] => (value: DbValue[Z]) => value.tpe)
      )
  end SqlSelectOperation

  trait DeleteReturningCapability

  type DeleteOperation[A[_[_]], B[_[_]]] <: SqlDeleteOperation[A, B]
  trait SqlDeleteOperation[A[_[_]], B[_[_]]](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends IntOperation:
    given ApplyKC[B]    = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)
    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      // Code duplicated for not needing casts
      val ast = usingV match
        case Some(usingQ) =>
          Query
            .from(from)
            .crossJoin(usingQ)
            .filter((a, b) => where(a, b))
            .selectAstAndValues
            .runA(freshTaggedState)
            .value
            .ast
        case None =>
          Query
            .from(from)
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

    def returningK[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    )(using DeleteReturningCapability): DeleteReturningOperation[A, B, C]

    inline def returning[C](f: (A[DbValue], B[DbValue]) => C)(
        using MR: MapRes[DbValue, C]
    )(using cap: DeleteReturningCapability): DeleteReturningOperation[A, B, MR.K] =
      returningK((a, b) => MR.toK(f(a, b)))(using MR.applyKC, MR.traverseKC, cap)
  end SqlDeleteOperation

  type DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlDeleteReturningOperation[A, B, C]
  trait SqlDeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
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
        case Some(usingQ) =>
          Query.from(from).crossJoin(usingQ).where((a, b) => where(a, b)).mapK((a, b) => returning(a, b))
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
  end SqlDeleteReturningOperation

  type DeleteCompanion <: SqlDeleteCompanion
  trait SqlDeleteCompanion:
    def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A]
  end SqlDeleteCompanion

  trait DeleteUsingCapability

  type DeleteFrom[A[_[_]]] <: SqlDeleteFrom[A]
  trait SqlDeleteFrom[A[_[_]]]:
    def using[B[_[_]]](query: Query[B])(using DeleteUsingCapability): DeleteFromUsing[A, B]
    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A]
  end SqlDeleteFrom

  type DeleteFromUsing[A[_[_]], B[_[_]]] <: SqlDeleteFromUsing[A, B]
  trait SqlDeleteFromUsing[A[_[_]], B[_[_]]]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B]
  end SqlDeleteFromUsing

  type InsertOperation[A[_[_]], B[_[_]]] <: SqlInsertOperation[A, B]

  trait InsertOnConflictCapability
  trait InsertReturningCapability

  trait SqlInsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B],
      conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, _]],
      onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
  ) extends IntOperation:
    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import values.given

      val ret = for
        computedValues <- values.selectAstAndValues
        computedOnConflict <- onConflict
          .map2Const(columns(table.columns)) {
            [Z] =>
              (f: (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]], column: Column[Codec, Z]) =>
                f(
                  SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe).liftDbValue,
                  SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe).liftDbValue
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
    )(using InsertOnConflictCapability): InsertOperation[A, B]

    def onConflictUpdate(on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]])(
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
    ): InsertReturningOperation[A, B, C]
  end SqlInsertOperation

  type InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlInsertReturningOperation[A, B, C]

  trait SqlInsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
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
                  SqlDbValue.QueryColumn(column.nameStr, table.tableName, column.tpe).liftDbValue,
                  SqlDbValue.QueryColumn(column.nameStr, "EXCLUDED", column.tpe).liftDbValue
                ).traverse(r => r.ast.map(column.name -> _))
          }
          .traverseK[TagState, Const[Option[(SqlStr[Codec], SqlExpr[Codec])]]](FunctionK.identity)
        returningValues = returning(
          table.columns.mapK(
            [Z] => (col: Column[Codec, Z]) => SqlDbValue.QueryColumn(col.nameStr, table.tableName, col.tpe).liftDbValue
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
  end SqlInsertReturningOperation

  type InsertCompanion <: SqlInsertCompanion
  trait SqlInsertCompanion:
    def into[A[_[_]]](table: Table[Codec, A]): InsertInto[A]
  end SqlInsertCompanion

  type InsertInto[A[_[_]]] <: SqlInsertInto[A]
  trait SqlInsertInto[A[_[_]]]:
    protected def table: Table[Codec, A]

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B]

    inline def valuesInColumnsFromQuery[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(query: Query[mr.K]): InsertOperation[A, mr.K] =
      valuesInColumnsFromQueryK(a => mr.toK(columns(a)))(query)

    def valuesFromQuery(query: Query[A]): InsertOperation[A, A]

    def values(value: A[Id], values: A[Id]*): InsertOperation[A, A] =
      valuesFromQuery(Query.valuesOf(table, value, values*))

    def valuesBatch(value: A[Id], values: A[Id]*)(using DistributiveKC[A]): InsertOperation[A, A] =
      valuesFromQuery(Query.valuesOfBatch(table, value, values*))

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B] =
      valuesInColumnsFromQueryK(columns)(
        Query.valuesK[B](columns(table.columns).mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values*)
      )

    def valuesInColumnsKBatch[B[_[_]]: ApplyKC: TraverseKC: DistributiveKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B] =
      valuesInColumnsFromQueryK(columns)(
        Query.valuesKBatch[B](columns(table.columns).mapK([Z] => (col: Column[Codec, Z]) => col.tpe), value, values*)
      )

    def valuesInColumns[T](
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

    def valuesInColumnsKBatch[T](
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
  end SqlInsertInto

  trait UpdateReturningCapability

  type UpdateOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlUpdateOperation[A, B, C]
  trait SqlUpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends IntOperation:

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import table.given

      val query = from match
        case Some(fromQ) =>
          Query.from(table).crossJoin(fromQ).filter((a, b) => where(a, b)).mapK((a, b) => setValues(a, b))
        case None =>
          Query
            .from(table)
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

    def returning[D[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], C[DbValue]) => D[DbValue]
    )(using UpdateReturningCapability): UpdateReturningOperation[A, B, C, D]
  end SqlUpdateOperation

  type UpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]], D[_[_]]] <: SqlUpdateReturningOperation[A, B, C, D]

  trait SqlUpdateReturningOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]], D[_[_]]: ApplyKC: TraverseKC](
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
        case Some(fromQ) => Query.from(table).crossJoin(fromQ)
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
  end SqlUpdateReturningOperation

  type UpdateCompanion <: SqlUpdateCompanion
  trait SqlUpdateCompanion:
    def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A]

  trait UpdateFromCapability

  type UpdateTable[A[_[_]]] <: SqlUpdateTable[A]
  trait SqlUpdateTable[A[_[_]]]:
    def from[B[_[_]]](fromQ: Query[B])(using UpdateFromCapability): UpdateTableFrom[A, B]

    def where(where: A[DbValue] => DbValue[Boolean]): UpdateTableWhere[A]

  type UpdateTableFrom[A[_[_]], B[_[_]]] <: SqlUpdateTableFrom[A, B]
  trait SqlUpdateTableFrom[A[_[_]], B[_[_]]]:
    def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableFromWhere[A, B]

  type UpdateTableWhere[A[_[_]]] <: SqlUpdateTableWhere[A]
  trait SqlUpdateTableWhere[A[_[_]]]:
    def values(setValues: A[DbValue] => A[DbValue]): UpdateOperation[A, A, A]

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: A[DbValue] => B[DbValue]
    ): UpdateOperation[A, B, A]

    inline def valuesInColumns[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(setValues: A[DbValue] => mr.K[DbValue]): UpdateOperation[A, mr.K, A] =
      valuesInColumnsK(a => mr.toK(columns(a)))(a => setValues(a))(using mr.applyKC, mr.traverseKC)

  type UpdateTableFromWhere[A[_[_]], C[_[_]]] <: SqlUpdateTableFromWhere[A, C]
  trait SqlUpdateTableFromWhere[A[_[_]], C[_[_]]]:
    def values(setValues: (A[DbValue], C[DbValue]) => A[DbValue]): UpdateOperation[A, A, C]

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: (A[DbValue], C[DbValue]) => B[DbValue]
    ): UpdateOperation[A, B, C]

    inline def valuesInColumns[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(setValues: (A[DbValue], C[DbValue]) => mr.K[DbValue]): UpdateOperation[A, mr.K, C] =
      valuesInColumnsK(a => mr.toK(columns(a)))((a, b) => setValues(a, b))(using mr.applyKC, mr.traverseKC)

  val Operation: OperationCompanion
  type OperationCompanion <: SqlOperationCompanion
  trait SqlOperationCompanion:

    val Select: SelectCompanion

    val Delete: DeleteCompanion

    val Insert: InsertCompanion

    val Update: UpdateCompanion

  end SqlOperationCompanion

  val Compile: Compile
  type Compile <: SqlCompile

  trait SqlCompile:
    protected def simple[A[_[_]]: ApplyKC: TraverseKC, B](types: A[Type])(f: A[DbValue] => B)(
        doReplacement: (B, Map[Object, Seq[Any]]) => B
    ): A[Id] => B =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] =
        types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues =
        tpesWithIdentifiers.mapK(
          [Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue
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

    inline def raw[A](types: A)(using res: MapRes[Type, A])(
        f: res.K[DbValue] => SqlStr[Codec]
    ): res.K[Id] => SqlStr[Codec] =
      rawK(res.toK(types))(f)(using res.applyKC, res.traverseKC)

    def operationK[A[_[_]]: ApplyKC: TraverseKC, B, F[_]](types: A[Type])(f: A[DbValue] => Operation[B])(
        using db: Db[F, Codec]
    ): A[Id] => F[B] =
      simple(types)(f.andThen(op => (op, op.sqlAndTypes))) { case ((op, (rawSqlStr, resultTypes)), replacements) =>
        (op, (rawSqlStr.compileWithValues(replacements), resultTypes))
      }.andThen { case (op, (sqlStr, resultTypes)) =>
        op.runWithSqlAndTypes(sqlStr, resultTypes.asInstanceOf[op.Types])
      }

    inline def operation[A, B, F[_]](types: A)(using res: MapRes[Type, A])(
        f: res.K[DbValue] => Operation[B]
    )(
        using db: Db[F, Codec]
    ): res.K[Id] => F[B] =
      operationK(res.toK(types))(f)(using res.applyKC, res.traverseKC, db)

  type Api <: SqlOperationApi
  trait SqlOperationApi {
    export platform.{IntOperation, ResultOperation}

    inline def Select: platform.SelectCompanion       = platform.Operation.Select
    inline def Delete: platform.DeleteCompanion       = platform.Operation.Delete
    inline def Insert: platform.InsertCompanion       = platform.Operation.Insert
    inline def Update: platform.UpdateCompanion       = platform.Operation.Update
    inline def Operation: platform.OperationCompanion = platform.Operation

    type Compile = platform.Compile
    inline def Compile: platform.Compile = platform.Compile

    type SelectOperation[A[_[_]]] = platform.SelectOperation[A]

    type DeleteFrom[A[_[_]]]               = platform.DeleteFrom[A]
    type DeleteFromUsing[A[_[_]], B[_[_]]] = platform.DeleteFromUsing[A, B]
    type DeleteOperation[A[_[_]], B[_[_]]] = platform.DeleteOperation[A, B]

    type InsertInto[A[_[_]]]               = platform.InsertInto[A]
    type InsertOperation[A[_[_]], B[_[_]]] = platform.InsertOperation[A, B]

    type UpdateTable[A[_[_]]]                       = platform.UpdateTable[A]
    type UpdateTableFrom[A[_[_]], B[_[_]]]          = platform.UpdateTableFrom[A, B]
    type UpdateTableWhere[A[_[_]]]                  = platform.UpdateTableWhere[A]
    type UpdateTableFromWhere[A[_[_]], C[_[_]]]     = platform.UpdateTableFromWhere[A, C]
    type UpdateOperation[A[_[_]], B[_[_]], C[_[_]]] = platform.UpdateOperation[A, B, C]
  }
}
