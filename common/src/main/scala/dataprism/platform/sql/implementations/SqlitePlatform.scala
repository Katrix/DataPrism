package dataprism.platform.sql.implementations

import dataprism.platform.sql.value.SqlBitwiseOps
import dataprism.platform.sql.{DefaultCompleteSql, DefaultSqlOperations}
import dataprism.sharedast.{SqlExpr, SqliteAstRenderer}

trait SqlitePlatform extends DefaultCompleteSql, DefaultSqlOperations, SqlBitwiseOps { platform =>

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

  given DeleteReturningCapability with {}
  given InsertReturningCapability with {}
  given UpdateReturningCapability with {}

  given InsertOnConflictCapability with {}
  given UpdateFromCapability with       {}
  given FullJoinCapability with         {}

  given ExceptCapability with    {}
  given IntersectCapability with {}

  override protected def generateDeleteAlias: Boolean = false
  override protected def generateUpdateAlias: Boolean = false

  override type MapUpdateReturning[Table, _, Res] = Table => Res
  override protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res = (a, _) => f(a)
  
  given bitwiseByte: SqlBitwise[Byte] = SqlBitwise.defaultInstance
  given bitwiseOptByte: SqlBitwise[Option[Byte]] = SqlBitwise.defaultInstance
  given bitwiseShort: SqlBitwise[Short] = SqlBitwise.defaultInstance
  given bitwiseOptShort: SqlBitwise[Option[Short]] = SqlBitwise.defaultInstance
  given bitwiseInt: SqlBitwise[Int] = SqlBitwise.defaultInstance
  given bitwiseOptInt: SqlBitwise[Option[Int]] = SqlBitwise.defaultInstance

  type Api <: SqliteApi
  trait SqliteApi extends QueryApi, SqlDbValueApi, SqlDbValueImplApi, SqlBitwiseApi, SqlOperationApi, SqlQueryApi {
    export platform.given
  }
  
  lazy val sqlRenderer: SqliteAstRenderer[Codec] =
    new SqliteAstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)

  type DbMath = SimpleSqlDbMath
  object DbMath extends SimpleSqlDbMath

  type DbValue[A] = SqlDbValue[A]
  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] = Lift.subtype
}
