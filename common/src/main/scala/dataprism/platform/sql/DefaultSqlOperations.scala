package dataprism.platform.sql

trait DefaultSqlOperations extends SqlOperations { platform: SqlQueryPlatform =>

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

  override given sqlSelectCompanionLift: Lift[SqlSelectCompanionImpl, SqlSelectCompanion]                = Lift.subtype
  override given sqlSelectOperationLift[A[_[_]]]: Lift[SqlSelectOperationImpl[A], SqlSelectOperation[A]] = Lift.subtype
  override given sqlDeleteOperationLift[A[_[_]], B[_[_]]]
      : Lift[SqlDeleteOperationImpl[A, B], SqlDeleteOperation[A, B]] = Lift.subtype
  override given sqlDeleteReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlDeleteReturningOperationImpl[A, B, C], SqlDeleteReturningOperation[A, B, C]] = Lift.subtype
  override given sqlDeleteCompanionLift: Lift[SqlDeleteCompanionImpl, SqlDeleteCompanion] = Lift.subtype
  override given sqlDeleteFromLift[A[_[_]]]: Lift[SqlDeleteFromImpl[A], SqlDeleteFrom[A]] = Lift.subtype
  override given sqlDeleteFromUsingLift[A[_[_]], B[_[_]]]
      : Lift[SqlDeleteFromUsingImpl[A, B], SqlDeleteFromUsing[A, B]] = Lift.subtype
  override given sqlInsertOperationLift[A[_[_]], B[_[_]]]
      : Lift[SqlInsertOperationImpl[A, B], SqlInsertOperation[A, B]] = Lift.subtype
  override given sqlInsertReturningOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlInsertReturningOperationImpl[A, B, C], SqlInsertReturningOperation[A, B, C]] = Lift.subtype
  override given sqlInsertCompanionLift: Lift[SqlInsertCompanionImpl, SqlInsertCompanion] = Lift.subtype
  override given sqlInsertIntoLift[A[_[_]]]: Lift[SqlInsertIntoImpl[A], SqlInsertInto[A]] = Lift.subtype
  override given sqlUpdateOperationLift[A[_[_]], B[_[_]], C[_[_]]]
      : Lift[SqlUpdateOperationImpl[A, B, C], SqlUpdateOperation[A, B, C]] = Lift.subtype
  override given sqlUpdateReturningOperationLift[A[_[_]], B[_[_]], C[_[_]], D[_[_]]]
      : Lift[SqlUpdateReturningOperationImpl[A, B, C, D], SqlUpdateReturningOperation[A, B, C, D]] = Lift.subtype
  override given sqlUpdateCompanionLift: Lift[SqlUpdateCompanionImpl, SqlUpdateCompanion]    = Lift.subtype
  override given sqlUpdateTableLift[A[_[_]]]: Lift[SqlUpdateTableImpl[A], SqlUpdateTable[A]] = Lift.subtype
  override given sqlUpdateTableFromLift[A[_[_]], C[_[_]]]
      : Lift[SqlUpdateTableFromImpl[A, C], SqlUpdateTableFrom[A, C]] = Lift.subtype
  override given sqlUpdateTableWhereLift[A[_[_]]]: Lift[SqlUpdateTableWhereImpl[A], SqlUpdateTableWhere[A]] =
    Lift.subtype
  override given sqlUpdateTableFromWhereLift[A[_[_]], C[_[_]]]
      : Lift[SqlUpdateTableFromWhereImpl[A, C], SqlUpdateTableFromWhere[A, C]] = Lift.subtype
}
