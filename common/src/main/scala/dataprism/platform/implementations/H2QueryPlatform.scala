package dataprism.platform.implementations

import dataprism.platform.sql.{DefaultCompleteSqlQueryPlatform, DefaultOperationSqlQueryPlatform}
import dataprism.sharedast.H2AstRenderer

trait H2QueryPlatform extends DefaultCompleteSqlQueryPlatform, DefaultOperationSqlQueryPlatform {
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

  override type MapUpdateReturning[Table, From, Res] = (Table, From) => Res
  override protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res = f

  type Api <: H2Api

  trait H2Api extends QueryApi with SqlDbValueApi with SqlOperationApi with SqlQueryApi

  lazy val sqlRenderer: H2AstRenderer[Codec] =
    new H2AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)

  type DbMath = SqlDbMath

  object DbMath extends SqlDbMath

  type DbValue[A] = SqlDbValue[A]
  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] = Lift.subtype
}
