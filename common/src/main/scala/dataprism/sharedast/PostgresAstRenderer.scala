package dataprism.sharedast

import dataprism.sql.*
import cats.syntax.all.*

//noinspection SqlNoDataSourceInspection
class PostgresAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String) extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderBinaryOp(lhs: SqlExpr[Codec], rhs: SqlExpr[Codec], op: SqlExpr.BinaryOperation): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} # ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderPreparedArgument(arg: SqlExpr.PreparedArgument[Codec]): SqlStr[Codec] =
    SqlStr(s"(?::${arg.arg.codec.name})", Seq(arg.arg))

  override protected def renderSelectValues(values: SelectAst.Values[Codec]): SqlStr[Codec] =
    val res = spaceConcat(
      sql"VALUES ",
      values.valueExprs.map(renderRow).intercalate(sql", ")
    )
    if values.alias.isDefined || values.columnAliases.isDefined then
      spaceConcat(
        sql"SELECT * FROM ($res)",
        sql"AS ${SqlStr.const(quote(values.alias.getOrElse("v")))}",
        values.columnAliases.fold(sql"")(as => sql"(${as.map(s => SqlStr.const(quote(s))).intercalate(sql", ")})")
      )
    else res
}
