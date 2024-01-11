package dataprism.platform.sql

import cats.Applicative
import cats.data.NonEmptyList
import cats.syntax.all.*
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
  ) extends Operation[QueryResult[Res[Id]]]:
    type Types = Res[Type]

    override def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Res[Type])(
        using db: Db[F, Codec]
    ): F[QueryResult[Res[Id]]] =
      db.runIntoRes(sqlStr, types.mapK([X] => (tpe: Type[X]) => tpe.codec))

  type SelectOperation[Res[_[_]]] <: SqlSelectOperation[Res]

  trait SqlSelectOperation[Res[_[_]]](query: Query[Res]) extends ResultOperation[Res]:
    override def sqlAndTypes: (SqlStr[Codec], Res[Type]) =
      import query.given

      given FunctorKC[Res] = summon[ApplyKC[Res]]

      val astMeta = query.selectAstAndValues.runA(freshTaggedState).value
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
    def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A, A]
  end SqlDeleteCompanion

  type DeleteFrom[A[_[_]], B[_[_]]] <: SqlDeleteFrom[A, B]
  trait SqlDeleteFrom[A[_[_]], B[_[_]]](from: Table[Codec, A], using: Option[Query[B]] = None):
    def using[B1[_[_]]](query: Query[B1]): DeleteFrom[A, B1]
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B]
  end SqlDeleteFrom

  type InsertOperation[A[_[_]]] <: SqlInsertOperation[A]

  type Optional[A[_[_]]] = [F[_]] =>> A[Compose2[Option, F]]

  given optValuesInstance[A[_[_]]](
      using FA: ApplyKC[A],
      FT: TraverseKC[A]
  ): ApplyKC[Optional[A]] with TraverseKC[Optional[A]] with {
    extension [B[_], D](fa: A[Compose2[Option, B]])
      def map2K[C[_], Z[_]](fb: A[Compose2[Option, C]])(f: [X] => (B[X], C[X]) => Z[X]): A[Compose2[Option, Z]] =
        FA.map2K(fa)(fb)([X] => (v1o: Option[B[X]], v2o: Option[C[X]]) => v1o.zip(v2o).map((v1, v2) => f(v1, v2)))

      def traverseK[G[_]: Applicative, C[_]](f: B :~>: Compose2[G, C]): G[A[Compose2[Option, C]]] =
        FT.traverseK(fa)([Z] => (vo: Option[B[Z]]) => vo.traverse[G, C[Z]](v => f(v)))

      def foldLeftK[C](b: C)(f: C => B :~>#: C): C =
        FT.foldLeftK(fa)(b)(b1 => [Z] => (vo: Option[B[Z]]) => vo.fold(b1)(v => f(b1)(v)))

      def foldRightK[C](b: C)(f: B :~>#: (C => C)): C =
        FT.foldRightK(fa)(b)([Z] => (vo: Option[B[Z]]) => (b1: C) => vo.fold(b1)(v => f(v)(b1)))
  }

  trait SqlInsertOperation[A[_[_]]](
      table: Table[Codec, A],
      values: Query[Optional[A]]
  ) extends IntOperation:
    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =
      import table.given

      val ret = values.selectAstAndValues.map { computedValues =>
        sqlRenderer.renderInsert(
          table.name,
          table.columns
            .map2Const(computedValues.values)(
              [Z] => (column: Column[Codec, Z], opt: Option[DbValue[Z]]) => opt.map(_ => column.name)
            )
            .toListK
            .flatMap(_.toList),
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

    def values[A[_[_]]](table: Table[Codec, A], value: A[Id], values: Seq[A[Id]] = Nil): InsertOperation[A] =
      into(table).values(Query.valuesOf(table, value, values))
  end SqlInsertCompanion

  type InsertInto[A[_[_]]] <: SqlInsertInto[A]
  trait SqlInsertInto[A[_[_]]]:

    def values(query: Query[A]): InsertOperation[A]

    def valuesWithoutSomeColumns(query: Query[[F[_]] =>> A[Compose2[Option, F]]]): InsertOperation[A]
  end SqlInsertInto

  type UpdateOperation[A[_[_]], B[_[_]]] <: SqlUpdateOperation[A, B]
  trait SqlUpdateOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      from: Option[Query[B]],
      setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]],
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends IntOperation:

    override def sqlAndTypes: (SqlStr[Codec], Type[Int]) =

      import table.given
      given (ApplyKC[Optional[A]] & TraverseKC[Optional[A]]) = optValuesInstance[A]

      val query = from match
        case Some(fromQ) =>
          Query.from(table).flatMap(a => fromQ.where(b => where(a, b)).mapK[Optional[A]](b => setValues(a, b)))
        case None =>
          Query
            .from(table)
            .where(a => where(a, a.asInstanceOf[B[DbValue]]))
            .mapK[Optional[A]](a => setValues(a, a.asInstanceOf[B[DbValue]]))

      val ret =
        for meta <- query.selectAstAndValues
        yield sqlRenderer.renderUpdate(
          table.columns
            .map2Const(meta.values)(
              [Z] => (col: Column[Codec, Z], v: Option[DbValue[Z]]) => v.map(_ => col.name).toList
            )
            .toListK
            .flatten,
          meta.ast,
          Nil
        )

      (ret.runA(freshTaggedState).value, AnsiTypes.integer.notNull)

  type UpdateCompanion <: SqlUpdateCompanion
  trait SqlUpdateCompanion:
    def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A, A]

  type UpdateTable[A[_[_]], B[_[_]]] <: SqlUpdateTable[A, B]
  trait SqlUpdateTable[A[_[_]], B[_[_]]]:
    def from[B1[_[_]]](fromQ: Query[B1]): UpdateTable[A, B1]

    def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableWhere[A, B]

  type UpdateTableWhere[A[_[_]], B[_[_]]] <: SqlUpdateTableWhere[A, B]
  trait SqlUpdateTableWhere[A[_[_]], B[_[_]]]:
    def values(setValues: (A[DbValue], B[DbValue]) => A[DbValue]): UpdateOperation[A, B]

    def someValues(setValues: (A[DbValue], B[DbValue]) => A[Compose2[Option, DbValue]]): UpdateOperation[A, B]

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
        doReplacement: (B, Map[Object, Any]) => B
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
          values.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
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
