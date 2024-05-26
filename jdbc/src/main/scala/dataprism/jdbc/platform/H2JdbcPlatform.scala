package dataprism.jdbc.platform

import dataprism.jdbc.sql.{JdbcCodec, H2JdbcTypes}
import dataprism.platform.sql.implementations.H2Platform
import dataprism.sql.AnsiTypes

import scala.annotation.targetName

trait H2JdbcPlatform extends H2Platform {

  type Api <: H2Api

  //override type ArrayTypeArgs[A] = Nothing // H2JdbcTypes.ArrayMapping[A]
  override type Codec[A]          = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  //override protected def arrayType[A](elemType: Type[A])(
  //    using extraArrayTypeArgs: ArrayTypeArgs[A]
  //): Type[Seq[A]] = 
  //  ??? // H2JdbcTypes.array(elemType).notNull

  override val AnsiTypes: AnsiTypes[JdbcCodec] = H2JdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
object H2JdbcPlatform extends H2JdbcPlatform {
  override type Api = H2Api
  object Api extends H2Api

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
