package dataprism.platform.sql.value

import scala.annotation.targetName

import dataprism.sharedast.SqlExpr

trait SqlStringOps extends SqlDbValuesBase { platform =>

  trait SqlStringLpadCapability
  trait SqlStringRpadCapability

  trait SqlStringTrimLeadingCapability
  trait SqlStringTrimTrailingCapability

  trait SqlStringLeftCapability
  trait SqlStringRightCapability

  trait SqlStringMd5Capability
  trait SqlStringSha256Capability

  trait SqlStringRepeatCapability
  trait SqlStringReverseCapability

  trait SqlStringHexCapability

  case class SqlStringConcatOp[A]() extends BinOp[A, A, A]:
    override def name: String = "concat"

    override def ast: SqlExpr.BinaryOperation = SqlExpr.BinaryOperation.Concat

    override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = lhs.tpe

  case class SqlStringLikeOp[A]() extends BinOp[A, A, Boolean]:
    override def name: String = "like"

    override def ast: SqlExpr.BinaryOperation = SqlExpr.BinaryOperation.Like

    override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[Boolean] = AnsiTypes.boolean

  case class SqlStringRegexMatchesOp[A]() extends BinOp[A, A, Boolean]:
    override def name: String = "regex_matches"

    override def ast: SqlExpr.BinaryOperation = SqlExpr.BinaryOperation.RegexMatches

    override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[Boolean] = AnsiTypes.boolean

  trait SqlString[A]:
    extension (lhs: DbValue[A])
      def ++(rhs: DbValue[A]): DbValue[A]
      @targetName("repeat") def *(rhs: DbValue[Int])(using SqlStringRepeatCapability): DbValue[A]

      def length: DbValue[Int]

      def toLowerCase: DbValue[A]
      def toUpperCase: DbValue[A]

      def lpad(length: DbValue[Int], content: DbValue[A])(using SqlStringLpadCapability): DbValue[A]
      def rpad(length: DbValue[Int], content: DbValue[A])(using SqlStringRpadCapability): DbValue[A]

      def ltrim: DbValue[A]
      def rtrim: DbValue[A]

      def indexOf(a: DbValue[A]): DbValue[A]

      def substr(from: DbValue[Int], forLength: DbValue[Int]): DbValue[A]

      def trimLeading(rhs: DbValue[A])(using SqlStringTrimLeadingCapability): DbValue[A]
      def trimTrailing(rhs: DbValue[A])(using SqlStringTrimTrailingCapability): DbValue[A]
      def trimBoth(rhs: DbValue[A]): DbValue[A]

      def like(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]
      def matches(regex: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]

      def startsWith(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]
      def endsWith(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]]

      def left(n: DbValue[Int])(using SqlStringLeftCapability): DbValue[A]
      def right(n: DbValue[Int])(using SqlStringRightCapability): DbValue[A]

      def md5(using SqlStringMd5Capability): DbValue[A]
      def sha256(using SqlStringSha256Capability): DbValue[A]

      def replace(target: DbValue[A], replacement: DbValue[A]): DbValue[A]

      def reverse(using SqlStringReverseCapability): DbValue[A]

      // TODO: Postgres? Split functions

      def hex(using SqlStringHexCapability): DbValue[A]

  object SqlString:
    def concat[A: SqlString](v1: DbValue[A], vars: DbValue[A]*): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Concat, (v1 +: vars).map(_.unsafeAsAnyDbVal), v1.tpe)
    def concatWs[A: SqlString](v1: DbValue[A], vars: DbValue[A]*): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.ConcatWs, (v1 +: vars).map(_.unsafeAsAnyDbVal), v1.tpe)

    def defaultInstance[A]: SqlString[A] = new SqlString[A]:
      extension (lhs: DbValue[A])
        override def ++(rhs: DbValue[A]): DbValue[A] = Impl.binaryOp(lhs, rhs, SqlStringConcatOp())
        @targetName("repeat")
        override def *(rhs: DbValue[Int])(using SqlStringRepeatCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Repeat, Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal), lhs.tpe)

        override def length: DbValue[Int] =
          Impl.function(SqlExpr.FunctionName.CharLength, Seq(lhs.unsafeAsAnyDbVal), AnsiTypes.integer)

        override def toLowerCase: DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Lower, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)
        override def toUpperCase: DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Upper, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

        override def lpad(length: DbValue[Int], content: DbValue[A])(using SqlStringLpadCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Lpad, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)
        override def rpad(length: DbValue[Int], content: DbValue[A])(using SqlStringRpadCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Rpad, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

        override def ltrim: DbValue[A] = Impl.function(SqlExpr.FunctionName.Ltrim, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)
        override def rtrim: DbValue[A] = Impl.function(SqlExpr.FunctionName.Rtrim, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

        override def indexOf(a: DbValue[A]): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.IndexOf, Seq(lhs.unsafeAsAnyDbVal, a.unsafeAsAnyDbVal), lhs.tpe)
        override def substr(from: DbValue[Int], forLength: DbValue[Int]): DbValue[A] = Impl.function(
          SqlExpr.FunctionName.Substring,
          Seq(lhs.unsafeAsAnyDbVal, from.unsafeAsAnyDbVal, forLength.unsafeAsAnyDbVal),
          lhs.tpe
        )

        override def trimLeading(rhs: DbValue[A])(using SqlStringTrimLeadingCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.TrimLeading, Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal), lhs.tpe)
        override def trimTrailing(rhs: DbValue[A])(using SqlStringTrimTrailingCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.TrimTrailing, Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal), lhs.tpe)
        override def trimBoth(rhs: DbValue[A]): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.TrimBoth, Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal), lhs.tpe)

        override def like(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
          Impl.binaryOp(n.castDbVal(lhs), n.castDbVal(rhs), n.wrapBinOp(SqlStringLikeOp()))
        override def matches(regex: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
          Impl.binaryOp(n.castDbVal(lhs), n.castDbVal(regex), n.wrapBinOp(SqlStringRegexMatchesOp()))

        override def startsWith(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
          Impl.function(
            SqlExpr.FunctionName.StartsWith,
            Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal),
            n.wrapType(AnsiTypes.boolean)
          )
        override def endsWith(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
          Impl.function(
            SqlExpr.FunctionName.EndsWith,
            Seq(lhs.unsafeAsAnyDbVal, rhs.unsafeAsAnyDbVal),
            n.wrapType(AnsiTypes.boolean)
          )

        override def left(n: DbValue[Int])(using SqlStringLeftCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Left, Seq(lhs.unsafeAsAnyDbVal, n.unsafeAsAnyDbVal), lhs.tpe)
        override def right(n: DbValue[Int])(using SqlStringRightCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Right, Seq(lhs.unsafeAsAnyDbVal, n.unsafeAsAnyDbVal), lhs.tpe)

        override def md5(using SqlStringMd5Capability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Md5, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)
        override def sha256(using SqlStringSha256Capability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Sha256, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

        override def replace(target: DbValue[A], replacement: DbValue[A]): DbValue[A] = Impl.function(
          SqlExpr.FunctionName.Replace,
          Seq(lhs.unsafeAsAnyDbVal, target.unsafeAsAnyDbVal, replacement.unsafeAsAnyDbVal),
          lhs.tpe
        )
        override def reverse(using SqlStringReverseCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Reverse, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

        override def hex(using SqlStringHexCapability): DbValue[A] =
          Impl.function(SqlExpr.FunctionName.Hex, Seq(lhs.unsafeAsAnyDbVal), lhs.tpe)

    end defaultInstance

    given SqlString[String]         = defaultInstance
    given SqlString[Option[String]] = defaultInstance

  type Api <: SqlStringApi & SqlDbValueApi & QueryApi
  trait SqlStringApi {
    export platform.SqlString
  }
}
