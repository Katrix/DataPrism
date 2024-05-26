package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderFunctionCall(
      call: SqlExpr.FunctionName,
      args: Seq[SqlExpr[Codec]],
      tpe: String
  ): SqlStr[Codec] =
    inline def rendered                         = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr[Codec] = sql"${SqlStr.const(f)}($rendered)"

    inline def tpeSql = SqlStr.const(tpe)

    call match
      case SqlExpr.FunctionName.Log =>
        sql"log(${args.map(renderExpr).map(s => sql"($s)::NUMERIC").intercalate(sql", ")})::$tpeSql"
      case SqlExpr.FunctionName.Ln    => sql"${normal("ln")}::$tpeSql"
      case SqlExpr.FunctionName.Log10 => sql"${normal("log10")}::$tpeSql"
      case SqlExpr.FunctionName.Sign  => sql"${normal("sign")}::$tpeSql"
      case SqlExpr.FunctionName.Exp   => sql"${normal("exp")}::$tpeSql"
      case SqlExpr.FunctionName.Pow   => sql"${normal("power")}::$tpeSql"

      case SqlExpr.FunctionName.Abs     => sql"${normal("abs")}::$tpeSql"
      case SqlExpr.FunctionName.Floor   => sql"${normal("floor")}::$tpeSql"
      case SqlExpr.FunctionName.Ceiling => sql"${normal("ceil")}::$tpeSql"
      case SqlExpr.FunctionName.Sqrt    => sql"${normal("sqrt")}::$tpeSql"

      case SqlExpr.FunctionName.Radians => sql"${normal("radians")}::$tpeSql"
      case SqlExpr.FunctionName.Degrees => sql"${normal("degrees")}::$tpeSql"
      case _                            => super.renderFunctionCall(call, args, tpe)

  override protected def renderBinaryOp(
      lhs: SqlExpr[Codec],
      rhs: SqlExpr[Codec],
      op: SqlExpr.BinaryOperation,
      tpe: String
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} # ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op, tpe)

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
