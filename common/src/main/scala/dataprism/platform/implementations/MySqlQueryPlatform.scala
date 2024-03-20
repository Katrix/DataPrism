package dataprism.platform.implementations

import dataprism.platform.sql.DefaultCompleteSqlQueryPlatform
import dataprism.sharedast.{MySqlAstRenderer, SqlExpr}

trait MySqlQueryPlatform extends DefaultCompleteSqlQueryPlatform { platform =>

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

  given DeleteUsingCapability with  {}

  override type MapUpdateReturning[Table, From, Res] = (Table, From) => Res
  override protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res = f

  type Api <: MySqlApi
  trait MySqlApi extends QueryApi with SqlDbValueApi with SqlOperationApi with SqlQueryApi {
    export platform.{given DeleteUsingCapability, given LateralJoinCapability}
  }

  type DbMath = SqlDbMath
  object DbMath extends SqlDbMath

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

  override type SelectOperation[A[_[_]]] = SqlSelectOperation[A]
  override type SelectCompanion          = SqlSelectCompanion

  override type DeleteOperation[A[_[_]], B[_[_]]]                   = SqlDeleteOperation[A, B]
  override type DeleteReturningOperation[A[_[_]], B[_[_]], C[_[_]]] = SqlDeleteReturningOperation[A, B, C]
  override type DeleteCompanion                                     = SqlDeleteCompanion
  override type DeleteFrom[A[_[_]]]                                 = SqlDeleteFrom[A]
  override type DeleteFromUsing[A[_[_]], B[_[_]]]                   = SqlDeleteFromUsing[A, B]

  override type InsertOperation[A[_[_]], B[_[_]]]                   = SqlInsertOperation[A, B]
  override type InsertReturningOperation[A[_[_]], B[_[_]], C[_[_]]] = SqlInsertReturningOperation[A, B, C]
  override type InsertCompanion                                     = SqlInsertCompanion
  override type InsertInto[A[_[_]]]                                 = SqlInsertInto[A]

  override type UpdateOperation[A[_[_]], B[_[_]], C[_[_]]]                   = SqlUpdateOperation[A, B, C]
  override type UpdateReturningOperation[A[_[_]], B[_[_]], C[_[_]], D[_[_]]] = SqlUpdateReturningOperation[A, B, C, D]
  override type UpdateCompanion                                              = SqlUpdateCompanion
  override type UpdateTable[A[_[_]]]                                         = SqlUpdateTable[A]
  override type UpdateTableFrom[A[_[_]], C[_[_]]]                            = SqlUpdateTableFrom[A, C]
  override type UpdateTableWhere[A[_[_]]]                                    = SqlUpdateTableWhere[A]
  override type UpdateTableFromWhere[A[_[_]], C[_[_]]]                       = SqlUpdateTableFromWhere[A, C]

  override protected def sqlSelectCompanionLift: Lift[SqlSelectCompanion, SqlSelectCompanion] = Lift.identity

  override protected def sqlSelectOperationLift[A[_[_]]]: Lift[SqlSelectOperation[A], SqlSelectOperation[A]] =
    Lift.identity

  override protected def sqlDeleteOperationLift[A[_[_]], B[_[_]]]
      : Lift[SqlDeleteOperation[A, B], SqlDeleteOperation[A, B]] = Lift.identity

  override protected def sqlDeleteReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlDeleteReturningOperation[A, B, C], SqlDeleteReturningOperation[A, B, C]] = Lift.identity

  override protected def sqlDeleteCompanionLift: Lift[SqlDeleteCompanion, SqlDeleteCompanion] = Lift.identity

  override protected def sqlDeleteFromLift[A[_[_]]]: Lift[SqlDeleteFrom[A], SqlDeleteFrom[A]] = Lift.identity

  override protected def sqlDeleteFromUsingLift[A[_[_]], B[_[_]]]
      : Lift[SqlDeleteFromUsing[A, B], SqlDeleteFromUsing[A, B]] = Lift.identity

  override protected def sqlInsertOperationLift[A[_[_]], B[_[_]]]
      : Lift[SqlInsertOperation[A, B], SqlInsertOperation[A, B]] = Lift.identity

  override protected def sqlInsertReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlInsertReturningOperation[A, B, C], SqlInsertReturningOperation[A, B, C]] = Lift.identity

  override protected def sqlInsertCompanionLift: Lift[SqlInsertCompanion, SqlInsertCompanion] = Lift.identity

  override protected def sqlInsertIntoLift[A[_[_]]]: Lift[SqlInsertInto[A], SqlInsertInto[A]] = Lift.identity

  override protected def sqlUpdateOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlUpdateOperation[A, B, C], SqlUpdateOperation[A, B, C]] = Lift.identity

  override protected def sqlUpdateReturningOperationLift[A[_[_]], B[_[_]], C[_[_]], D[_[_]]]
      : Lift[SqlUpdateReturningOperation[A, B, C, D], SqlUpdateReturningOperation[A, B, C, D]] = Lift.identity

  override protected def sqlUpdateCompanionLift: Lift[SqlUpdateCompanion, SqlUpdateCompanion] = Lift.identity

  override protected def sqlUpdateTableLift[A[_[_]]]: Lift[SqlUpdateTable[A], SqlUpdateTable[A]] = Lift.identity

  override protected def sqlUpdateTableFromLift[A[_[_]], C[_[_]]]
      : Lift[SqlUpdateTableFrom[A, C], SqlUpdateTableFrom[A, C]] = Lift.identity

  override protected def sqlUpdateTableWhereLift[A[_[_]]]: Lift[SqlUpdateTableWhere[A], SqlUpdateTableWhere[A]] =
    Lift.identity

  override protected def sqlUpdateTableFromWhereLift[A[_[_]], C[_[_]]]
      : Lift[SqlUpdateTableFromWhere[A, C], SqlUpdateTableFromWhere[A, C]] = Lift.identity

  type OperationCompanion = SqlOperationCompanion
  object Operation extends SqlOperationCompanion
}
