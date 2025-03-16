package dataprism.platform.sql.implementations

import dataprism.platform.sql.value.{SqlArrays, SqlBitwiseOps, SqlHyperbolicTrigFunctions, SqlTrigFunctions}
import dataprism.platform.sql.{DefaultCompleteSql, DefaultSqlOperations, SqlMergeOperations}
import dataprism.sharedast.H2AstRenderer
import dataprism.sql.SqlNull

trait H2Platform
    extends DefaultCompleteSql,
      DefaultSqlOperations,
      SqlMergeOperations,
      SqlBitwiseOps,
      SqlTrigFunctions,
      SqlHyperbolicTrigFunctions,
      SqlArrays {
  platform =>

  override type CastType[A] = Type[A]

  extension [A](t: CastType[A])
    override def castTypeName: String  = t.name
    override def castTypeType: Type[A] = t

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

  given ExceptCapability with    {}
  given IntersectCapability with {}

  given DistinctOnCapability with {}

  given SqlStringLpadCapability with {}
  given SqlStringRpadCapability with {}

  given SqlStringTrimLeadingCapability with  {}
  given SqlStringTrimTrailingCapability with {}

  given SqlStringLeftCapability with  {}
  given SqlStringRightCapability with {}

  given SqlStringMd5Capability with    {}
  given SqlStringSha256Capability with {}

  given SqlStringRepeatCapability with {}

  override type MapUpdateReturning[Table, From, Res] = (Table, From) => Res
  override protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res = f

  given bitwiseByte: SqlBitwise[Byte]                = SqlBitwise.defaultInstance
  given bitwiseOptByte: SqlBitwise[Byte | SqlNull]   = SqlBitwise.defaultInstance
  given bitwiseShort: SqlBitwise[Short]              = SqlBitwise.defaultInstance
  given bitwiseOptShort: SqlBitwise[Short | SqlNull] = SqlBitwise.defaultInstance
  given bitwiseInt: SqlBitwise[Int]                  = SqlBitwise.defaultInstance
  given bitwiseOptInt: SqlBitwise[Int | SqlNull]     = SqlBitwise.defaultInstance

  type OperationCompanion = SqlOperationCompanion & SqlMergeOperationsCompanion
  object Operation extends SqlOperationCompanionImpl, SqlMergeOperationsCompanion

  type Api <: H2Api
  trait H2Api
      extends QueryApi,
        SqlDbValueApi,
        SqlDbValueImplApi,
        SqlBitwiseApi,
        SqlStringApi,
        SqlOperationApi,
        SqlMergeApi,
        SqlQueryApi,
        SqlArraysApi {
    export platform.given
  }

  lazy val sqlRenderer: H2AstRenderer[Codec] =
    new H2AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)

  type DbMath = SimpleSqlDbMath & SqlTrigMath & SqlHyperbolicTrigMath
  object DbMath extends SimpleSqlDbMath, SqlTrigMath, SqlHyperbolicTrigMath

  type DbValue[A] = SqlDbValue[A]
  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] = Lift.subtype
}
