package dataprism.platform.sql.value

import dataprism.sharedast.SqlExpr

trait SqlBitwiseOps extends SqlDbValuesBase { platform =>

  enum SqlBitwiseBinOp[A](val name: String, val ast: SqlExpr.BinaryOperation, bitwise: SqlBitwise[A])
      extends BinOp[A, A, A]:
    case And(bitwise: SqlBitwise[A]) extends SqlBitwiseBinOp("band", SqlExpr.BinaryOperation.BitwiseAnd, bitwise)
    case Or(bitwise: SqlBitwise[A])  extends SqlBitwiseBinOp("bor", SqlExpr.BinaryOperation.BitwiseOr, bitwise)
    case Xor(bitwise: SqlBitwise[A]) extends SqlBitwiseBinOp("bxor", SqlExpr.BinaryOperation.BitwiseXOr, bitwise)

    override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = bitwise.tpe(lhs, rhs)

  case class SqlBitwiseNotOp[A](bitwise: SqlBitwise[A]) extends UnaryOp[A, A] {
    override def name: String = "bnot"

    override def ast: SqlExpr.UnaryOperation = SqlExpr.UnaryOperation.BitwiseNot

    override def tpe(v: DbValue[A]): Type[A] = bitwise.tpe(v, v)
  }

  trait SqlBitwise[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A]

    extension (lhs: DbValue[A])
      def &(rhs: DbValue[A]): DbValue[A]
      def |(rhs: DbValue[A]): DbValue[A]
      def ^(rhs: DbValue[A]): DbValue[A]
      def unary_~ : DbValue[A]

  object SqlBitwise:
    def defaultInstance[A]: SqlBitwise[A] = new SqlBitwise[A]:
      override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = lhs.tpe

      extension (lhs: DbValue[A])
        override def &(rhs: DbValue[A]): DbValue[A] = Impl.binaryOp(lhs, rhs, SqlBitwiseBinOp.And(this))
        override def |(rhs: DbValue[A]): DbValue[A] = Impl.binaryOp(lhs, rhs, SqlBitwiseBinOp.Or(this))
        override def ^(rhs: DbValue[A]): DbValue[A] = Impl.binaryOp(lhs, rhs, SqlBitwiseBinOp.Xor(this))
        override def unary_~ : DbValue[A]           = Impl.unaryOp(lhs, SqlBitwiseNotOp(this))
        
    given longBitwiseOps: SqlBitwise[Long] = defaultInstance
    given optLongBitwiseOps: SqlBitwise[Option[Long]] = defaultInstance

  type Api <: SqlBitwiseApi & SqlDbValueApi & QueryApi
  trait SqlBitwiseApi {
    export platform.SqlBitwise
  }
}
