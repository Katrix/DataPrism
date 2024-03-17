package dataprism.sql

import scala.collection.mutable

extension [A, Codec[_]](a: A)(using SqlInterpolation[Codec])
  def asArg(tpe: Codec[A]): SqlArg[Codec] = SqlArg.SqlArgObj(Seq(a), tpe)
extension [A, Codec[_]](a: Seq[A])(using SqlInterpolation[Codec])
  def asBatchArg(tpe: Codec[A]): SqlArg[Codec] = SqlArg.SqlArgObj(a, tpe)

//noinspection ScalaFileName
trait SqlInterpolation[Type[_]]

extension (sc: StringContext)
  def sql[Type[_]](args: SqlInterpolation[Type] ?=> SqlArg[Type] | SqlStr[Type]*): SqlStr[Type] =
    given sqlInterpolation: SqlInterpolation[Type] = new SqlInterpolation[Type] {}
    StringContext.checkLengths(args, sc.parts)

    val sb    = mutable.StringBuilder()
    val argsb = Seq.newBuilder[SqlArg[Type]]

    sb ++= sc.parts.head

    sc.parts.tail.zip(args.map(f => f(using sqlInterpolation))).foreach {
      case (s, arg: SqlArg[Type] @unchecked) =>
        sb += '?' ++= s
        argsb += arg

      case (s, table: Table[_, _]) =>
        sb ++= table.tableName
        sb ++= s

      case (s, column: Column[_, _]) =>
        sb ++= column.nameStr
        sb ++= s

      case (s, str: SqlStr[Type] @unchecked) =>
        sb ++= str.str
        sb ++= s
        argsb ++= str.args
      case other =>
        println(
          s"""|Got unexpected args
              |${other._1}
              |${other._2.getClass}
              |${other._2}""".stripMargin
        )
    }

    SqlStr(sb.result(), argsb.result())
  end sql
