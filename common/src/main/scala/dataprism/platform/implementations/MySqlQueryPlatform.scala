package dataprism.platform.implementations

import dataprism.platform.sql.{DefaultCompleteSqlQueryPlatform, UnsafeSqlQueryPlatformFlatmap}
import dataprism.sharedast.{MySqlAstRenderer, SqlExpr}
import dataprism.sql.*
import perspective.*

trait MySqlQueryPlatform extends DefaultCompleteSqlQueryPlatform with UnsafeSqlQueryPlatformFlatmap { platform =>

  override type InFilterCapability        = Unit
  override type InMapCapability           = Unit
  override type InJoinConditionCapability = Unit
  override type InGroupByCapability       = Unit
  override type InHavingCapability        = Unit
  override type InOrderByCapability       = Unit

  override protected val InFilterCapability: Unit        = ()
  override protected val InMapCapability: Unit           = ()
  override protected val InJoinConditionCapability: Unit = ()
  override protected val InGroupByCapability: Unit       = ()
  override protected val InHavingCapability: Unit        = ()
  override protected val InOrderByCapability: Unit       = ()

  given DeleteUsingCapability with {}
  given UpdateFromCapability with  {}

  type Api <: MySqlApi
  trait MySqlApi extends QueryApi with SqlDbValueApi with SqlOperationApi with SqlQueryApi {
    export platform.{given DeleteUsingCapability, given UpdateFromCapability}
  }

  val sqlRenderer: MySqlAstRenderer[Codec] = new MySqlAstRenderer[Codec](AnsiTypes)

  type DbValue[A] = MySqlDbValue[A]
  enum MySqlDbValue[A] extends SqlDbValueBase[A]:
    case SqlDbValue(value: platform.SqlDbValue[A])

    override def ast: TagState[SqlExpr[Codec]] = this match
      case MySqlDbValue.SqlDbValue(v) => v.ast

    override def asSqlDbVal: Option[platform.SqlDbValue[A]] = this match
      case MySqlDbValue.SqlDbValue(v) => Some(v)

    override def tpe: Type[A] = this match
      case MySqlDbValue.SqlDbValue(v) => v.tpe

    override def columnName(prefix: String): String = this match
      case MySqlDbValue.SqlDbValue(v) => v.columnName(prefix)

    override def unsafeAsAnyDbVal: DbValue[Any] = this.asInstanceOf[DbValue[Any]]
    override def liftDbValue: DbValue[A]        = this
    override def asc: Ord                       = Ord.Asc(this.unsafeAsAnyDbVal)
    override def desc: Ord                      = Ord.Desc(this.unsafeAsAnyDbVal)
  end MySqlDbValue

  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] =
    new Lift[SqlDbValue[A], DbValue[A]]:
      extension (a: SqlDbValue[A]) def lift: DbValue[A] = MySqlDbValue.SqlDbValue(a)

  case class SelectOperation[Res[_[_]]](query: Query[Res])
      extends SqlSelectOperation[Res](query)
      with ResultOperation[Res](using query.applyK, query.traverseK)

  case class DeleteOperation[A[_[_]], B[_[_]]](
      from: Table[Codec, A],
      usingV: Option[Query[B]] = None,
      where: (A[DbValue], B[DbValue]) => DbValue[Boolean]
  ) extends SqlDeleteOperation[A, B](from, usingV, where)

  case class InsertOperation[A[_[_]], B[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      values: Query[B]
  ) extends SqlInsertOperation[A, B](table, columns, values)

  case class UpdateOperation[A[_[_]], B[_[_]]: ApplyKC: TraverseKC, C[_[_]]](
      table: Table[Codec, A],
      columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]],
      from: Option[Query[C]],
      setValues: (A[DbValue], C[DbValue]) => B[DbValue],
      where: (A[DbValue], C[DbValue]) => DbValue[Boolean]
  ) extends SqlUpdateOperation[A, B, C](table, columns, from, setValues, where)

  trait DeleteCompanion extends SqlDeleteCompanion:
    override def from[A[_[_]]](from: Table[Codec, A]): DeleteFrom[A] = DeleteFrom(from)

  case class DeleteFrom[A[_[_]]](from: Table[Codec, A]) extends SqlDeleteFrom[A]:
    def using[B[_[_]]](query: Query[B])(using DeleteUsingCapability): DeleteFromUsing[A, B] =
      DeleteFromUsing(from, query)

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
      InsertOperation(table, identity, query)

    def valuesInColumnsFromQueryK[B[_[_]]](columns: A[[X] =>> Column[Codec, X]] => B[[X] =>> Column[Codec, X]])(
        query: Query[B]
    ): InsertOperation[A, B] =
      InsertOperation(table, columns, query)
  end InsertInto

  trait UpdateCompanion extends SqlUpdateCompanion:
    override def table[A[_[_]]](table: Table[Codec, A]): UpdateTable[A] = UpdateTable(table)

  case class UpdateTable[A[_[_]]](table: Table[Codec, A]) extends SqlUpdateTable[A]:

    def from[B[_[_]]](fromQ: Query[B])(using UpdateFromCapability): UpdateTableFrom[A, B] =
      UpdateTableFrom(table, fromQ)

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
    override def Select[Res[_[_]]](query: SqlQuery[Res]): SelectOperation[Res] = SelectOperation(query)

    object Delete extends DeleteCompanion

    object Insert extends InsertCompanion

    object Update extends UpdateCompanion
  end OperationCompanion

  export Operation.*
}
