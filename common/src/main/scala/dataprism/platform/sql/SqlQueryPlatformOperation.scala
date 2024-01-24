package dataprism.platform.sql

import cats.Functor
import cats.data.NonEmptySeq
import cats.syntax.all.*
import dataprism.platform.base.MapRes
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

  type DeleteOperation[A[_[_]], B[_[_]]] <: SqlDeleteOperation[A, B]
  trait SqlDeleteOperation[A[_[_]], B[_[_]]](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends IntOperation:
    given ApplyKC[B]    = usingV.fold(from.FA.asInstanceOf[ApplyKC[B]])(_.applyK)
    given TraverseKC[B] = usingV.fold(from.FT.asInstanceOf[TraverseKC[B]])(_.traverseK)

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      val q = usingV match
        case Some(usingQ) => Query.from(from).flatMap(a => usingQ.where(b => where(a, b)))
        case None         => Query.from(from).where(a => where(a, a.asInstanceOf[B[DbValue]])).asInstanceOf[Query[B]]

      (
        sqlRenderer.renderDelete(
          q.selectAstAndValues.runA(freshTaggedState).value.ast,
          returning = false
        ),
        AnsiTypes.integer.notNull
      )
  end SqlDeleteOperation

  type DeleteCompanion <: SqlDeleteCompanion
  trait SqlDeleteCompanion:
    def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A]
  end SqlDeleteCompanion

  type DeleteFrom[A[_[_]]] <: SqlDeleteFrom[A]
  trait SqlDeleteFrom[A[_[_]]]:
    def using[B[_[_]]](query: Query[B]): DeleteFromUsing[A, B]
    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A]
  end SqlDeleteFrom

  type DeleteFromUsing[A[_[_]], B[_[_]]] <: SqlDeleteFromUsing[A, B]
  trait SqlDeleteFromUsing[A[_[_]], B[_[_]]]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B]
  end SqlDeleteFromUsing

  type InsertOperation[A[_[_]], B[_[_]]] <: SqlInsertOperation[A, B]

  trait SqlInsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B]
  ) extends IntOperation:
    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import values.given

      val ret = values.selectAstAndValues.map { computedValues =>
        sqlRenderer.renderInsert(
          table.name,
          columns(table.columns).foldMapK([X] => (col: Column[Codec, X]) => List(col.name)),
          computedValues.ast,
          Nil,
          Nil,
          Nil
        )
      }

      (ret.runA(freshTaggedState).value, AnsiTypes.integer.notNull)

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
          Query.from(table).flatMap(a => fromQ.where(b => where(a, b)).mapK(b => setValues(a, b)))
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

  type UpdateCompanion <: SqlUpdateCompanion
  trait SqlUpdateCompanion:
    def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A]

  type UpdateTable[A[_[_]]] <: SqlUpdateTable[A]
  trait SqlUpdateTable[A[_[_]]]:
    def from[B[_[_]]](fromQ: Query[B]): UpdateTableFrom[A, B]

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

    def Select[Res[_[_]]](query: Query[Res]): SelectOperation[Res]

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
}
