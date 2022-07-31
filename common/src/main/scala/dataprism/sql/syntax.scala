package dataprism.sql

import scala.collection.mutable

extension [A](a: A)(using SqlInterpolation) def as(tpe: DbType[A]): SqlArg = SqlArg.SqlArgObj(a, tpe)

//noinspection ScalaFileName
trait SqlInterpolation

extension (sc: StringContext)
  def sql(args: SqlInterpolation ?=> (SqlArg | Table[_] | Column[_] | SqlStr)*): SqlStr =
    given sqlInterpolation: SqlInterpolation = new SqlInterpolation {}
    StringContext.checkLengths(args, sc.parts)

    val sb    = mutable.StringBuilder()
    val argsb = Seq.newBuilder[SqlArg]

    sb ++= sc.parts.head

    sc.parts.tail.zip(args.map(f => f(using sqlInterpolation))).foreach {
      case (s, arg: SqlArg) =>
        sb += '?' ++= s
        argsb += arg

      case (s, table: Table[_]) =>
        sb ++= table.tableName
        sb ++= s

      case (s, column: Column[_]) =>
        sb ++= column.nameStr
        sb ++= s

      case (s, str: SqlStr) =>
        sb ++= str.str
        sb ++= s
        argsb ++= str.args
      case other =>
        println("Got unexpected args")
        println(other._1)
        println(other._2.getClass)
        println(other._2)
    }

    SqlStr(sb.result(), argsb.result())
  end sql
