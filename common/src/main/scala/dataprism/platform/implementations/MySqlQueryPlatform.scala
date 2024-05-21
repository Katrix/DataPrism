package dataprism.platform.implementations

import dataprism.platform.sql.{DefaultCompleteSqlQueryPlatform, DefaultOperationSqlQueryPlatform}

trait MySqlQueryPlatform extends DefaultCompleteSqlQueryPlatform, DefaultOperationSqlQueryPlatform { platform =>

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
  
  //TODO: Support MySql variant
  //given UpdateFromCapability with {}

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

  type DbValue[A] = SqlDbValue[A]
  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] = Lift.subtype
}
