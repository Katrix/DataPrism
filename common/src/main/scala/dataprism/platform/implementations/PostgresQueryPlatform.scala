package dataprism.platform.implementations

import cats.data.NonEmptyList
import cats.syntax.all.*
import dataprism.platform.sql.{DefaultCompleteSqlQueryPlatform, UnsafeSqlQueryPlatformFlatmap}
import dataprism.sharedast.{PostgresAstRenderer, SqlExpr}
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
trait PostgresQueryPlatform extends DefaultCompleteSqlQueryPlatform with UnsafeSqlQueryPlatformFlatmap { platform =>

  override type InFilterCapability = Unit
  override type InMapCapability = Unit
  override type InJoinConditionCapability = Unit
  override type InGroupByCapability = Unit
  override type InHavingCapability = Unit
  override type InOrderByCapability = Unit

  override protected val InFilterCapability: Unit = ()
  override protected val InMapCapability: Unit = ()
  override protected val InJoinConditionCapability: Unit = ()
  override protected val InGroupByCapability: Unit = ()
  override protected val InHavingCapability: Unit = ()
  override protected val InOrderByCapability: Unit = ()

  given DeleteUsingCapability with {}
  given UpdateFromCapability with {}
  given DeleteReturningCapability with {}
  given InsertReturningCapability with {}
  given UpdateReturningCapability with {}
  given InsertOnConflictCapability with {}

  type Api <: PostgresApi
  trait PostgresApi extends QueryApi with SqlDbValueApi with SqlOperationApi with SqlQueryApi {
    export platform.{given DeleteUsingCapability, given UpdateFromCapability}
  }

  val sqlRenderer: PostgresAstRenderer[Codec] = new PostgresAstRenderer[Codec](AnsiTypes)
  type ArrayTypeArgs[A]
  protected def arrayType[A](elemType: Type[A])(using extraArrayTypeArgs: ArrayTypeArgs[A]): Type[Seq[A]]

  override type CastType[A] = Type[A]

  extension [A](t: CastType[A])
    override def castTypeName: String  = t.name
    override def castTypeType: Type[A] = t

  type DbValue[A] = PostgresDbValue[A]
  enum PostgresDbValue[A] extends SqlDbValueBase[A]:
    case SqlDbValue(value: platform.SqlDbValue[A])
    case ArrayOf(values: Seq[DbValue[A]], elemType: Type[A], extraArrayTypeArgs: ArrayTypeArgs[A])
        extends DbValue[Seq[A]]

    def ast: TagState[SqlExpr[Codec]] = this match
      case PostgresDbValue.SqlDbValue(value) => value.ast
      case PostgresDbValue.ArrayOf(values, _, _) =>
        values.toList
          .traverse(_.ast)
          .map(exprs => SqlExpr.Custom(exprs, args => sql"ARRAY[${args.intercalate(sql", ")}]"))
    end ast

    def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case PostgresDbValue.SqlDbValue(res) => Some(res)
      case _                               => None

    override def tpe: Type[A] = this match
      case PostgresDbValue.SqlDbValue(value)                   => value.tpe
      case PostgresDbValue.ArrayOf(_, tpe, extraArrayTypeArgs) => arrayType(tpe)(using extraArrayTypeArgs)
    end tpe

    override def columnName(prefix: String): String = this match
      case PostgresDbValue.SqlDbValue(v)         => v.columnName(prefix)
      case PostgresDbValue.ArrayOf(values, _, _) => s"${values.headOption.fold(prefix)(_.columnName(prefix))}_array"

    def singletonArray(using extraArrayTypeArgs: ArrayTypeArgs[A]): DbValue[Seq[A]] =
      PostgresDbValue.ArrayOf(Seq(this), tpe, extraArrayTypeArgs)

    override def liftDbValue: DbValue[A] = this

    override def asc: Ord = Ord.Asc(this.unsafeAsAnyDbVal)

    override def desc: Ord = Ord.Desc(this.unsafeAsAnyDbVal)

    override def unsafeAsAnyDbVal: AnyDbValue = this.asInstanceOf[AnyDbValue]
  end PostgresDbValue

  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = PostgresDbValue.SqlDbValue(a)

  case class SelectOperation[Res[_[_]]](query: Query[Res])
      extends SqlSelectOperation[Res](query)
      with ResultOperation[Res](using query.applyK, query.traverseK)

  case class DeleteOperation[A[_[_]], B[_[_]]](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlDeleteOperation[A, B](from, usingV, where):
    def returningK[C[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], B[DbValue]) => C[DbValue]
    )(using DeleteReturningCapability): DeleteReturningOperation[A, B, C] = DeleteReturningOperation(from, usingV, where, f)

  case class DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], B[DbValue]) => C[DbValue]
  ) extends SqlDeleteReturningOperation[A, B, C](from, usingV, where, returning)

  case class InsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B],
      conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, _]],
      onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
  ) extends SqlInsertOperation[A, B](table, columns, values, conflictOn, onConflict):

    def onConflict(
        on: A[[Z] =>> Column[Codec, Z]] => NonEmptyList[Column[Codec, _]],
        a: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]]
    )(using InsertOnConflictCapability): InsertOperation[A, B] =
      copy(conflictOn = on.andThen(_.toList), onConflict = a)

    def returning[C[_[_]]: ApplyKC: TraverseKC](f: A[DbValue] => C[DbValue])(using InsertReturningCapability): InsertReturningOperation[A, B, C] =
      InsertReturningOperation(table, columns, values, conflictOn, onConflict, f)

  case class InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]: ApplyKC: TraverseKC](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B],
      conflictOn: A[[Z] =>> Column[Codec, Z]] => List[Column[Codec, _]],
      onConflict: B[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]],
      returning: A[DbValue] => C[DbValue]
  ) extends SqlInsertReturningOperation(table, columns, values, conflictOn, onConflict, returning)

  case class UpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B, C](table, columns, from, setValues, where):
    def returning[D[_[_]]: ApplyKC: TraverseKC](
        f: (A[DbValue], C[DbValue]) => D[DbValue]
    )(using UpdateReturningCapability): UpdateReturningOperation[A, B, C, D] =
      UpdateReturningOperation(table, columns, from, setValues, where, f)

  case class UpdateReturningOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]], D[_[_]]: ApplyKC: TraverseKC](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean],
      returning: (A[DbValue], C[DbValue]) => D[DbValue]
  ) extends SqlUpdateReturningOperation(table, columns, from, setValues, where, returning)

  trait SelectCompanion extends SqlSelectCompanion:
    override def apply[Res[_[_]]](query: SqlQuery[Res]): SelectOperation[Res] = SelectOperation(query)

  trait DeleteCompanion extends SqlDeleteCompanion:
    override def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A] = DeleteFrom(from)

  case class DeleteFrom[A[_[_]]](from: Table[Codec, A]) extends SqlDeleteFrom[A]:
    def using[B[_[_]]](query: Query[B])(using DeleteUsingCapability): DeleteFromUsing[A, B] = DeleteFromUsing(from, query)

    def where(f: A[DbValue] => DbValue[Boolean]): DeleteOperation[A, A] = DeleteOperation(from, None, (a, _) => f(a))
  end DeleteFrom

  case class DeleteFromUsing[A[_[_]], B[_[_]]](from: Table[Codec, A], using: Query[B]) extends SqlDeleteFromUsing[A, B]:
    def where(f: (A[DbValue], B[DbValue]) => DbValue[Boolean]): DeleteOperation[A, B] =
      DeleteOperation(from, Some(`using`), f)
  end DeleteFromUsing

  trait InsertCompanion extends SqlInsertCompanion:
    override def into[A[_[_]]](table: Table[Codec, A]): InsertInto[A] = InsertInto(table)

  case class InsertInto[A[_[_]]](table: Table[Codec, A]) extends SqlInsertInto[A]:

    def valuesFromQuery(query: Query[A]): InsertOperation[A, A] =
      import table.given
      InsertOperation(
        table,
        identity,
        query,
        _ => Nil,
        table.columns.mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      )

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B] =
      import query.given_ApplyKC_A
      InsertOperation(
        table,
        columns,
        query,
        _ => Nil,
        columns(table.columns).mapK[[Z] =>> (DbValue[Z], DbValue[Z]) => Option[DbValue[Z]]](
          [Z] => (_: Column[Codec, Z]) => (_: DbValue[Z], _: DbValue[Z]) => None: Option[DbValue[Z]]
        )
      )
  end InsertInto

  trait UpdateCompanion extends SqlUpdateCompanion:
    override def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A] = UpdateTable(table)

  case class UpdateTable[A[_[_]]](table: Table[Codec, A]) extends SqlUpdateTable[A]:

    def from[B[_[_]]](fromQ: Query[B])(using UpdateFromCapability): UpdateTableFrom[A, B] = UpdateTableFrom(table, fromQ)

    def where(where: A[DbValue] => DbValue[Boolean]): UpdateTableWhere[A] =
      UpdateTableWhere(table, where)
  end UpdateTable

  case class UpdateTableFrom[A[_[_]], B[_[_]]](table: Table[Codec, A], from: Query[B]) extends SqlUpdateTableFrom[A, B]:
    override def where(where: (A[DbValue], B[DbValue]) => DbValue[Boolean]): UpdateTableFromWhere[A, B] =
      UpdateTableFromWhere(table, from, where)

  case class UpdateTableWhere[A[_[_]]](
      table: Table[Codec, A],
      where: A[DbValue] => DbValue[Boolean]
  ) extends SqlUpdateTableWhere[A]:

    def values(setValues: A[DbValue] => A[DbValue]): UpdateOperation[A, A, A] =
      import table.given
      given FunctorKC[A] = table.FA

      UpdateOperation(
        table,
        identity,
        None,
        (a, _) => setValues(a),
        (a, _) => where(a)
      )

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: A[DbValue] => B[DbValue]
    ): UpdateOperation[A, B, A] = UpdateOperation(table, columns, None, (a, _) => setValues(a), (a, _) => where(a))

  case class UpdateTableFromWhere[A[_[_]], C[_[_]]](
      table: Table[Codec, A],
      from: Query[C],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateTableFromWhere[A, C]:

    def values(setValues: (A[DbValue], C[DbValue]) => A[DbValue]): UpdateOperation[A, A, C] =
      import table.given
      given FunctorKC[A] = table.FA

      UpdateOperation(
        table,
        identity,
        Some(from),
        (a, b) => setValues(a, b),
        where
      )

    def valuesInColumnsK[B[_[_]]: ApplyKC: TraverseKC](
        columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]]
    )(
        setValues: (A[DbValue], C[DbValue]) => B[DbValue]
    ): UpdateOperation[A, B, C] = UpdateOperation(table, columns, Some(from), setValues, where)

  object Operation extends OperationCompanion
  trait OperationCompanion extends SqlOperationCompanion:
    object Select extends SelectCompanion

    object Delete extends DeleteCompanion

    object Insert extends InsertCompanion

    object Update extends UpdateCompanion
  end OperationCompanion

  export Operation.*
}
