package dataprism.platform.sql

import cats.MonadThrow
import cats.data.{NonEmptyList, NonEmptySeq}
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sql.*
import perspective.*

trait SqlOperationsBase extends SqlQueryPlatformBase, SqlQueriesBase { platform =>

  sealed trait Operation[A]:
    type Types

    def sqlAndTypes: (SqlStr[Codec], Types)

    def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Types)(using db: Db[F, Codec])(using MonadThrow[F]): F[A]

    def run[F[_]](using Db[F, Codec])(using MonadThrow[F]): F[A] =
      val (sqlStr, types) = sqlAndTypes
      runWithSqlAndTypes(sqlStr, types)
  end Operation

  trait IntOperation extends Operation[Int]:
    type Types = Type[Int]

    override def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Type[Int])(using db: Db[F, Codec])(
        using MonadThrow[F]
    ): F[Int] =
      MonadThrow[F].map(db.runBatch(sqlStr))(_.sum)
  end IntOperation

  trait ResultOperation[Res[_[_]]](
      using val resApplyK: ApplyKC[Res],
      val resTraverseK: TraverseKC[Res]
  ) extends Operation[Seq[Res[Id]]]:
    type Types = Res[Type]

    private def runWithMinMax[F[_]](minRows: Int, maxRows: Int)(using db: Db[F, Codec])(
        using MonadThrow[F]
    ): F[Seq[Res[Id]]] =
      val (sqlStr, types) = sqlAndTypes
      db.runIntoRes(sqlStr, types.mapK([X] => (tpe: Type[X]) => tpe.codec), minRows, maxRows)

    override def runWithSqlAndTypes[F[_]](sqlStr: SqlStr[Codec], types: Res[Type])(
        using db: Db[F, Codec]
    )(using MonadThrow[F]): F[Seq[Res[Id]]] =
      db.runIntoRes(sqlStr, types.mapK([X] => (tpe: Type[X]) => tpe.codec))

    def runOne[F[_]](using db: Db[F, Codec])(using MonadThrow[F]): F[Res[Id]] = runWithMinMax(1, 1).map(_.head)

    def runMaybeOne[F[_]](using db: Db[F, Codec])(using MonadThrow[F]): F[Option[Res[Id]]] =
      runWithMinMax(0, 1).map(_.headOption)

    def runOneOrMore[F[_]](using db: Db[F, Codec])(using MonadThrow[F]): F[NonEmptySeq[Res[Id]]] =
      runWithMinMax(1, -1).map(s => NonEmptySeq.fromSeqUnsafe(s))
  end ResultOperation

  trait SqlSelectOperation[Res[_[_]]] extends ResultOperation[Res]
  type SelectOperation[Res[_[_]]] <: SqlSelectOperation[Res]

  type SelectCompanion <: SqlSelectCompanion
  trait SqlSelectCompanion:
    def apply[Res[_[_]]](query: Query[Res]): SelectOperation[Res]

  trait DeleteReturningCapability

  type DeleteOperation[A[_[_]], B[_[_]]] <: SqlDeleteOperation[A, B]
  trait SqlDeleteOperation[A[_[_]], B[_[_]]] extends IntOperation:
    def returningK[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    )(using DeleteReturningCapability): DeleteReturningOperation[A, B, C]

    inline def returning[C](f: (A[DbValue], B[DbValue]) => C)(
        using MR: MapRes[DbValue, C]
    )(using cap: DeleteReturningCapability): DeleteReturningOperation[A, B, MR.K] =
      returningK((a, b) => MR.toK(f(a, b)))(using MR.applyKC, MR.traverseKC, cap)

  trait SqlDeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]] extends ResultOperation[C]
  type DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlDeleteReturningOperation[A, B, C]

  type DeleteCompanion <: SqlDeleteCompanion
  trait SqlDeleteCompanion:
    def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A]

  trait DeleteUsingCapability

  type DeleteFrom[A[_[_]]] <: SqlDeleteFrom[A]
  trait SqlDeleteFrom[A[_[_]]]:
    def using[B[_[_]]](query: Query[B])(using DeleteUsingCapability): DeleteFromUsing[A, B]
    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A]

  type DeleteFromUsing[A[_[_]], B[_[_]]] <: SqlDeleteFromUsing[A, B]
  trait SqlDeleteFromUsing[A[_[_]], B[_[_]]]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B]

  type InsertOperation[A[_[_]], B[_[_]]] <: SqlInsertOperation[A, B]

  trait InsertOnConflictCapability
  trait InsertReturningCapability

  trait SqlInsertOperation[A[_[_]], B[_[_]]] extends IntOperation:

    def onConflict(
        on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]],
        a: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
    )(using InsertOnConflictCapability): InsertOperation[A, B]

    def onConflictUpdate(on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]])(
        using InsertOnConflictCapability
    ): InsertOperation[A, B]

    def returning[C[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => C[DbValue])(
        using InsertReturningCapability
    ): InsertReturningOperation[A, B, C]
  end SqlInsertOperation

  trait SqlInsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]] extends ResultOperation[C]
  type InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlInsertReturningOperation[A, B, C]

  type InsertCompanion <: SqlInsertCompanion
  trait SqlInsertCompanion:
    def into[A[_[_]]](table: Table[Codec, A]): InsertInto[A]

  type InsertInto[A[_[_]]] <: SqlInsertInto[A]
  trait SqlInsertInto[A[_[_]]]:

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B]

    inline def valuesInColumnsFromQuery[T](columns: A[[X] =>> Column[Codec, X]] => T)(
        using mr: MapRes[[X] =>> Column[Codec, X], T]
    )(query: Query[mr.K]): InsertOperation[A, mr.K] =
      valuesInColumnsFromQueryK(a => mr.toK(columns(a)))(query)

    def valuesFromQuery(query: Query[A]): InsertOperation[A, A]

    def values(value: A[Id], values: A[Id]*): InsertOperation[A, A]

    def valuesBatch(value: A[Id], values: A[Id]*)(using DistributiveKC[A]): InsertOperation[A, A]

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B]

    def valuesInColumnsKBatch[B[_[_]]: ApplyKC: TraverseKC: DistributiveKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(value: B[Id], values: B[Id]*): InsertOperation[A, B]

    def valuesInColumns[T](
        columns: A[[X] =>> Column[Codec, X]] => T
    )(using mr: MapRes[[X] =>> Column[Codec, X], T])(value: mr.K[Id], values: mr.K[Id]*): InsertOperation[A, mr.K]

    def valuesInColumnsKBatch[T](
        columns: A[[X] =>> Column[Codec, X]] => T
    )(using mr: MapRes[[X] =>> Column[Codec, X], T])(value: mr.K[Id], values: mr.K[Id]*)(
        using D: DistributiveKC[mr.K]
    ): InsertOperation[A, mr.K]
  end SqlInsertInto

  trait UpdateReturningCapability

  type MapUpdateReturning[Table, From, Res]

  type UpdateOperation[A[_[_]], B[_[_]], C[_[_]]] <: SqlUpdateOperation[A, B, C]
  trait SqlUpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]] extends IntOperation:

    def returningK[D[_[_]]: ApplyKC: TraverseKC](
        f: MapUpdateReturning[A[DbValue], C[DbValue], D[DbValue]]
    )(using UpdateReturningCapability): UpdateReturningOperation[A, B, C, D]

    def returning[T](
        f: MapUpdateReturning[A[DbValue], C[DbValue], T]
    )(using mr: MapRes[DbValue, T], cap: UpdateReturningCapability): UpdateReturningOperation[A, B, C, mr.K]

  trait SqlUpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]], D[_[_]]] extends ResultOperation[D]
  type UpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]], D[_[_]]] <: SqlUpdateReturningOperation[A, B, C, D]

  type UpdateCompanion <: SqlUpdateCompanion
  trait SqlUpdateCompanion:
    def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A]

  trait UpdateFromCapability

  type UpdateTable[A[_[_]]] <: SqlUpdateTable[A]
  trait SqlUpdateTable[A[_[_]]]:
    def from[B[_[_]]](fromQ: Query[B])(using UpdateFromCapability): UpdateTableFrom[A, B]

    def where(where: A[DbValue] => DbValue[Boolean]): UpdateTableWhere[A]

  type UpdateTableFrom[A[_[_]], C[_[_]]] <: SqlUpdateTableFrom[A, C]
  trait SqlUpdateTableFrom[A[_[_]], C[_[_]]]:
    def where(where: (A[DbValue], C[DbValue]) => DbValue[Boolean]): UpdateTableFromWhere[A, C]

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
    def rawK[A[_[_]]: ApplyKC: TraverseKC](types: A[Type])(f: A[DbValue] => SqlStr[Codec]): A[Id] => SqlStr[Codec]

    inline def raw[A](types: A)(using res: MapRes[Type, A])(
        f: res.K[DbValue] => SqlStr[Codec]
    ): res.K[Id] => SqlStr[Codec] =
      rawK(res.toK(types))(f)(using res.applyKC, res.traverseKC)

    def operationK[A[_[_]]: ApplyKC: TraverseKC, B, F[_]: MonadThrow](types: A[Type])(f: A[DbValue] => Operation[B])(
        using db: Db[F, Codec]
    ): A[Id] => F[B]

    inline def operation[A, B, F[_]: MonadThrow](types: A)(using res: MapRes[Type, A])(
        f: res.K[DbValue] => Operation[B]
    )(
        using db: Db[F, Codec]
    ): res.K[Id] => F[B] =
      operationK(res.toK(types))(f)(using res.applyKC, res.traverseKC, summon[MonadThrow[F]], db)

  type Api <: SqlOperationApi & SqlQueryApi & SqlDbValueApi & QueryApi
  trait SqlOperationApi {
    export platform.{IntOperation, ResultOperation}

    inline def Select: platform.SelectCompanion       = platform.Operation.Select
    inline def Delete: platform.DeleteCompanion       = platform.Operation.Delete
    inline def Insert: platform.InsertCompanion       = platform.Operation.Insert
    inline def Update: platform.UpdateCompanion       = platform.Operation.Update
    inline def Operation: platform.OperationCompanion = platform.Operation

    type Operation[A] = platform.Operation[A]

    type Compile = platform.Compile
    inline def Compile: platform.Compile = platform.Compile

    type SelectOperation[A[_[_]]] = platform.SelectOperation[A]

    type DeleteFrom[A[_[_]]]                                 = platform.DeleteFrom[A]
    type DeleteFromUsing[A[_[_]], B[_[_]]]                   = platform.DeleteFromUsing[A, B]
    type DeleteOperation[A[_[_]], B[_[_]]]                   = platform.DeleteOperation[A, B]
    type DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]] = platform.DeleteReturningOperation[A, B, C]

    type InsertInto[A[_[_]]]                                 = platform.InsertInto[A]
    type InsertOperation[A[_[_]], B[_[_]]]                   = platform.InsertOperation[A, B]
    type InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]] = platform.InsertReturningOperation[A, B, C]

    type UpdateTable[A[_[_]]]                                         = platform.UpdateTable[A]
    type UpdateTableFrom[A[_[_]], B[_[_]]]                            = platform.UpdateTableFrom[A, B]
    type UpdateTableWhere[A[_[_]]]                                    = platform.UpdateTableWhere[A]
    type UpdateTableFromWhere[A[_[_]], C[_[_]]]                       = platform.UpdateTableFromWhere[A, C]
    type UpdateOperation[A[_[_]], B[_[_]], C[_[_]]]                   = platform.UpdateOperation[A, B, C]
    type UpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]], D[_[_]]] = platform.UpdateReturningOperation[A, B, C, D]
  }
}
