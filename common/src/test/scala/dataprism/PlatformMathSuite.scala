package dataprism

import cats.Show
import cats.effect.IO
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import org.scalacheck.Gen
import org.scalacheck.cats.implicits.*
import spire.std.BigDecimalIsNRoot
import weaver.{Expectations, Log, SourceLocation}

trait PlatformMathSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform]:
  import platform.Api.*
  import platform.{AnsiTypes, name}

  given [A](using Frac: Fractional[A]): Fractional[Option[A]] with {
    override def div(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.div(a, b))

    override def plus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.plus(a, b))

    override def minus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.minus(a, b))

    override def times(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.times(a, b))

    override def negate(x: Option[A]): Option[A] = x.map(Frac.negate)

    override def fromInt(x: Int): Option[A] = Some(Frac.fromInt(x))

    override def parseString(str: String): Option[Option[A]] = Some(Frac.parseString(str))

    override def toInt(x: Option[A]): Int = x.fold(0)(Frac.toInt)

    override def toLong(x: Option[A]): Long = x.fold(0L)(Frac.toLong)

    override def toFloat(x: Option[A]): Float = x.fold(0F)(Frac.toFloat)

    override def toDouble(x: Option[A]): Double = x.fold(0D)(Frac.toDouble)

    override def compare(x: Option[A], y: Option[A]): Int = x.zip(y).map((a, b) => Frac.compare(a, b)).getOrElse(0)
  }

  given [A](using Integ: Integral[A]): Integral[Option[A]] with {
    override def quot(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.quot(a, b))

    override def rem(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.rem(a, b))

    override def plus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.plus(a, b))

    override def minus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.minus(a, b))

    override def times(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.times(a, b))

    override def negate(x: Option[A]): Option[A] = x.map(Integ.negate)

    override def fromInt(x: Int): Option[A] = Some(Integ.fromInt(x))

    override def parseString(str: String): Option[Option[A]] = Some(Integ.parseString(str))

    override def toInt(x: Option[A]): Int = x.fold(0)(Integ.toInt)

    override def toLong(x: Option[A]): Long = x.fold(0L)(Integ.toLong)

    override def toFloat(x: Option[A]): Float = x.fold(0F)(Integ.toFloat)

    override def toDouble(x: Option[A]): Double = x.fold(0D)(Integ.toDouble)

    override def compare(x: Option[A], y: Option[A]): Int = x.zip(y).map((a, b) => Integ.compare(a, b)).getOrElse(0)
  }

  def typeTest[A](name: String, tpe: Type[A])(run: DbType ?=> IO[Expectations])(using SourceLocation): Unit =
    dbTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  def typeLogTest[A](name: String, tpe: Type[A])(run: DbType ?=> Log[IO] => IO[Expectations])(
      using SourceLocation
  ): Unit =
    dbLogTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  val configuredForall: PartiallyAppliedForall = forall

  def logQuery[A[_[_]]](select: SelectOperation[A])(using log: Log[IO]): IO[SelectOperation[A]] =
    log.debug(s"Query: ${select.sqlAndTypes._1.str}").as(select)

  case class TypeInfo[A](
      tpe: Type[A],
      gen: Gen[A],
      epsilon: A,
      isNan: A => Boolean,
      transform2: Gen[(A, A)] => Gen[(A, A)] = identity[Gen[(A, A)]]
  )(using val numeric: Numeric[A], val show: Show[A])

  def functionTest0[A: SqlNumeric](
      castType: CastType[A],
      typeInfo: TypeInfo[A],
      name: String,
      dbLevel: CastType[A] => DbValue[A],
      valueLevel: () => A
  ): Unit = typeLogTest(name, castType.castTypeType): log =>
    given Log[IO] = log
    import typeInfo.given
    import Numeric.Implicits.*
    import Ordering.Implicits.*
    logQuery(Select(Query.of(dbLevel(castType))))
      .flatMap(_.runOne[F])
      .map: r =>
        val v = valueLevel()
        expect((v - r <= typeInfo.epsilon) || (typeInfo.isNan(v) && typeInfo.isNan(r)))

  def functionTest1[A: SqlNumeric](
      name: String,
      typeInfo: TypeInfo[A],
      dbLevel: DbValue[A] => DbValue[A],
      valueLevel: A => A
  ): Unit = typeLogTest(name, typeInfo.tpe): log =>
    given Log[IO] = log
    import typeInfo.given
    import Numeric.Implicits.*
    import Ordering.Implicits.*
    configuredForall(typeInfo.gen): (a: A) =>
      logQuery(Select(Query.of(dbLevel(a.as(typeInfo.tpe)))))
        .flatMap(_.runOne[F])
        .map: r =>
          val v = valueLevel(a)
          expect((v - r <= typeInfo.epsilon) || (typeInfo.isNan(v) && typeInfo.isNan(r)))

  def functionTest2[A: SqlNumeric](
      name: String,
      typeInfo: TypeInfo[A],
      dbLevel: (DbValue[A], DbValue[A]) => DbValue[A],
      valueLevel: (A, A) => A
  ): Unit = typeLogTest(name, typeInfo.tpe): log =>
    given Log[IO] = log
    import typeInfo.given
    import Numeric.Implicits.*
    import Ordering.Implicits.*
    configuredForall(typeInfo.transform2((typeInfo.gen, typeInfo.gen).tupled)): (a: A, b: A) =>
      logQuery(Select(Query.of(dbLevel(a.as(typeInfo.tpe), b.as(typeInfo.tpe)))))
        .flatMap(_.runOne[F])
        .map: r =>
          val v = valueLevel(a, b)
          expect((v - r <= typeInfo.epsilon) || (typeInfo.isNan(v) && typeInfo.isNan(r)))

  def testPow[A: SqlNumeric](typeInfo: TypeInfo[A], op: (A, A) => A): Unit =
    functionTest2("pow", typeInfo, DbMath.pow, op)

  def testSqrt[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("sqrt", typeInfo, DbMath.sqrt, op)

  def testAbs[A: SqlNumeric](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("abs", typeInfo, DbMath.abs, op)

  def testCeil[A: SqlNumeric](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("ceil", typeInfo, DbMath.ceil, op)

  def testFloor[A: SqlNumeric](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("floor", typeInfo, DbMath.floor, op)

  def testToDegrees[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("toDegrees", typeInfo, DbMath.toDegrees, op)

  def testToRadians[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("toRadians", typeInfo, DbMath.toRadians, op)

  def testLog[A: SqlFractional](typeInfo: TypeInfo[A], op: (A, A) => A): Unit =
    functionTest2("log", typeInfo, DbMath.log, op)

  def testLn[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("ln", typeInfo, DbMath.ln, op)

  def testLog10[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("log10", typeInfo, DbMath.log10, op)

  def testLog2[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("log2", typeInfo, DbMath.log2, op)

  def testExp[A: SqlFractional](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("exp", typeInfo, DbMath.exp, op)

  def testSign[A: SqlNumeric](typeInfo: TypeInfo[A], op: A => A): Unit =
    functionTest1("sign", typeInfo, DbMath.sign, op)

  def testPi[A: SqlNumeric](castType: CastType[A], typeInfo: TypeInfo[A], op: () => A): Unit =
    functionTest0(castType, typeInfo, "pi", DbMath.pi, op)

  def testRandom[A: SqlNumeric](tpe: CastType[A]): Unit =
    typeTest("random", tpe.castTypeType):
      Select(Query.of(DbMath.random(tpe)))
        .runOne[F]
        .map(r => expect(true))

  private val longGen = Gen.choose(-10000L, 10000L)

  val longTypeInfo: TypeInfo[Long] = TypeInfo(
    AnsiTypes.bigint,
    Gen.choose(-10000L, 10000L),
    0,
    _ => false
  )

  protected type LongLikeCastType
  protected def longCastType: CastType[LongLikeCastType]
  protected def longLikeTypeInfo: TypeInfo[LongLikeCastType]
  protected def doubleToLongLikeCastType(d: Double): LongLikeCastType
  protected given longLikeCastTypeSqlNumeric: SqlNumeric[LongLikeCastType]

  testPow(
    longTypeInfo.copy(
      gen = Gen.choose(-10, 10),
      transform2 = _ => {
        for {
          a <- Gen.choose(-10, 10)
          b <-
            if a == 0 then Gen.choose(1, 10)
            else Gen.choose(-10, 10)
        } yield (a, b)
      }
    ),
    (a, b) => Math.pow(a.toDouble, b.toDouble).toLong
  )
  testAbs(longTypeInfo, Math.abs)
  testCeil(longTypeInfo, a => Math.ceil(a.toDouble).toLong)
  testFloor(longTypeInfo, a => Math.floor(a.toDouble).toLong)
  testSign(longTypeInfo, a => Math.signum(a.toDouble).toInt)
  testPi(longCastType, longLikeTypeInfo, () => doubleToLongLikeCastType(Math.PI))
  testRandom(longCastType)

  private val doubleGen = Gen.choose(-10000D, 10000D)

  protected type DoubleLikeCastType
  protected def doubleCastType: CastType[DoubleLikeCastType]
  protected def doubleLikeTypeInfo: TypeInfo[DoubleLikeCastType]
  protected def doubleToDoubleLikeCastType(d: Double): DoubleLikeCastType
  protected given doubleLikeCastTypeSqlNumeric: SqlNumeric[DoubleLikeCastType]

  val doubleTypeInfo: TypeInfo[Double] = TypeInfo(
    AnsiTypes.doublePrecision,
    Gen.choose(-10000D, 10000D),
    0.00001,
    java.lang.Double.isNaN
  )

  val doublePositiveTypeInfo: TypeInfo[Double] = doubleTypeInfo.copy(gen = Gen.choose(0, 10000D))

  testPow(
    doubleTypeInfo.copy(
      gen = Gen.choose(-10D, 10D),
      transform2 = _ => {
        for {
          a <- Gen.choose(-10D, 10D)
          b <-
            if a < 0 then Gen.choose[Int](-10, 10).map(_.toDouble)
            else if a == 0 then Gen.choose(0.001D, 10D)
            else Gen.choose(-10D, 10D)
        } yield (a, b)
      }
    ),
    Math.pow
  )
  testSqrt(doublePositiveTypeInfo, Math.sqrt)
  testAbs(doubleTypeInfo, Math.abs)
  testCeil(doubleTypeInfo, Math.ceil)
  testFloor(doubleTypeInfo, Math.floor)
  testToDegrees(doubleTypeInfo, Math.toDegrees)
  testToRadians(doubleTypeInfo, Math.toRadians)
  testLog(doublePositiveTypeInfo, (a, b) => Math.log(b) / Math.log(a))
  testLn(doublePositiveTypeInfo, Math.log)
  testLog10(doublePositiveTypeInfo, Math.log10)
  testLog2(doublePositiveTypeInfo, a => Math.log(a) / Math.log(2))
  testExp(doubleTypeInfo.copy(gen = Gen.choose(-10D, 10D)), Math.exp)
  testSign(doubleTypeInfo, Math.signum)
  testPi(doubleCastType, doubleLikeTypeInfo, () => doubleToDoubleLikeCastType(Math.PI))
  testRandom(doubleCastType)

  val decimalTypeInfo: TypeInfo[BigDecimal] = TypeInfo(
    AnsiTypes.decimalN(15, 9),
    Gen.choose(BigDecimal(-10000D), BigDecimal(10000D)),
    0.00001,
    _ => false
  )

  val decimalPositiveTypeInfo: TypeInfo[BigDecimal] =
    decimalTypeInfo.copy(gen = Gen.choose(BigDecimal(0), BigDecimal(10000)))

  import spire.implicits.*

  // Hard to test
  //testPow(
  //  decimalTypeInfo.copy(
  //    gen = Gen.choose(BigDecimal(-10), BigDecimal(10)),
  //    transform2 = _ => {
  //      for {
  //        a <- Gen.choose(BigDecimal(-10), BigDecimal(10))
  //        b <-
  //          if a < 0 then Gen.choose[Int](-10, 10).map(BigDecimal(_))
  //          else if a == 0 then Gen.choose(BigDecimal(0.001D), BigDecimal(10D))
  //          else Gen.choose(BigDecimal(-10), BigDecimal(10))
  //      } yield (a, b)
  //    }
  //  ),
  //  (a: BigDecimal, b: BigDecimal) => {
  //    if b.isWhole then a.pow(b.toInt) else spire.math.exp(a.log * b)
  //  }
  //)
  testSqrt(decimalPositiveTypeInfo, (_: BigDecimal).sqrt)
  testAbs(decimalTypeInfo, (_: BigDecimal).abs)
  testCeil(decimalTypeInfo, (_: BigDecimal).setScale(0, BigDecimal.RoundingMode.CEILING))
  testFloor(decimalTypeInfo, (_: BigDecimal).setScale(0, BigDecimal.RoundingMode.FLOOR))
  testToDegrees(decimalTypeInfo, (_: BigDecimal) * 180 / BigDecimal(Math.PI))
  testToRadians(decimalTypeInfo, (_: BigDecimal) * BigDecimal(Math.PI) / 180)
  testLog(decimalPositiveTypeInfo, (a: BigDecimal, b: BigDecimal) => b.log / a.log)
  testLn(decimalPositiveTypeInfo, (_: BigDecimal).log)
  testLog10(decimalPositiveTypeInfo, (a: BigDecimal) => a.log / BigDecimal(10).log)
  testLog2(decimalPositiveTypeInfo, (a: BigDecimal) => a.log / BigDecimal(2).log)
  testExp(decimalTypeInfo.copy(gen = Gen.choose(-10D, 10D)), BigDecimal(Math.E).fpow(_: BigDecimal))
  testSign(decimalTypeInfo, (_: BigDecimal).sign)
