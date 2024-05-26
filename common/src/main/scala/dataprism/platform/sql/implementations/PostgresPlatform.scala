package dataprism.platform.sql.implementations

import cats.syntax.all.*
import dataprism.platform.sql.{DefaultCompleteSql, DefaultSqlOperations}
import dataprism.sharedast.{PostgresAstRenderer, SqlExpr}
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection, ScalaUnusedSymbol
trait PostgresPlatform extends DefaultCompleteSql, DefaultSqlOperations { platform =>

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

  given DeleteUsingCapability with      {}
  given UpdateFromCapability with       {}
  given DeleteReturningCapability with  {}
  given InsertReturningCapability with  {}
  given UpdateReturningCapability with  {}
  given InsertOnConflictCapability with {}
  given LateralJoinCapability with      {}
  given IntersectAllCapability with     {}
  given ExceptAllCapability with        {}
  given FullJoinCapability with         {}

  given ExceptCapability with {}
  given IntersectCapability with {}

  override type MapUpdateReturning[Table, From, Res] = (Table, From) => Res
  override protected def contramapUpdateReturning[Table, From, Res](
      f: MapUpdateReturning[Table, From, Res]
  ): (Table, From) => Res = f

  type Api <: PostgresApi
  trait PostgresApi extends QueryApi, SqlDbValueApi, SqlDbValueImplApi, SqlOperationApi, SqlQueryApi {
    export platform.{
      given DeleteReturningCapability,
      given DeleteUsingCapability,
      given InsertOnConflictCapability,
      given InsertReturningCapability,
      given LateralJoinCapability,
      given UpdateFromCapability,
      given UpdateReturningCapability
    }
  }

  lazy val sqlRenderer: PostgresAstRenderer[Codec] =
    new PostgresAstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
  type ArrayTypeArgs[A]
  protected def arrayType[A](elemType: Type[A])(using extraArrayTypeArgs: ArrayTypeArgs[A]): Type[Seq[A]]

  override type CastType[A] = Type[A]

  extension [A](t: CastType[A])
    override def castTypeName: String  = t.name
    override def castTypeType: Type[A] = t

  type DbMath = SimpleSqlDbMath
  object DbMath extends SimpleSqlDbMath

  type DbValue[A] = SqlDbValue[A]
  override protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]] = Lift.subtype
}
