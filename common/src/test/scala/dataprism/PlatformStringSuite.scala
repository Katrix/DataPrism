package dataprism

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import cats.effect.IO
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import org.scalacheck.Gen
import org.scalacheck.cats.implicits.*
import weaver.{Expectations, Log}

trait PlatformStringSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform]:

  import platform.AnsiTypes
  import platform.Api.{*, given}

  val configuredForall: PartiallyAppliedForall = forall

  def stringGenOf(n: Int): Gen[String] = for
    n <- Gen.choose(0, n)
    s <- Gen.stringOfN(n, Gen.frequency(10 -> Gen.alphaNumChar, 1 -> Gen.const(' ')))
  yield s

  val stringGen: Gen[String] = stringGenOf(40)

  val intGen: Gen[Int] = Gen.choose(-1000, 1000)

  val natIntGen: Gen[Int] = Gen.choose(0, 1000)

  def logQuery[A[_[_]]](select: SelectOperation[A])(using log: Log[IO]): IO[SelectOperation[A]] =
    val (selectStr, _) = select.sqlAndTypes
    log.debug(s"Query: ${selectStr.str}, args: ${selectStr.args}").as(select)

  extension (s: String) def asDbString: DbValue[String] = s.as(AnsiTypes.defaultStringType)

  dbTest("Concat"):
    configuredForall((stringGen, stringGen).tupled): (a, b) =>
      Select(Query.of(a.asDbString ++ b.asDbString, b.asDbString ++ a.asDbString))
        .runOne[F]
        .map: (r1, r2) =>
          expect.all(
            r1 == (a + b),
            r2 == (b + a)
          )

  def doTestRepeat()(using platform.SqlStringRepeatCapability): Unit = dbTest("Repeat"):
    configuredForall((stringGen, intGen).tupled): (a, b) =>
      Select(Query.of(a.asDbString * b.as(AnsiTypes.integer)))
        .runOne[F]
        .map: r =>
          expect(r == (a * b))

  dbTest("Length"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.length))
        .runOne[F]
        .map: r =>
          expect(r == a.length)

  dbTest("Uppercase"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.toUpperCase))
        .runOne[F]
        .map: r =>
          expect(r == a.toUpperCase)

  dbTest("Lowercase"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.toLowerCase))
        .runOne[F]
        .map: r =>
          expect(r == a.toLowerCase)

  def padSingleCharacterOnly: Boolean = false

  def doTestLpad()(using platform.SqlStringLpadCapability): Unit = dbTest("Lpad"):
    configuredForall((stringGen, Gen.choose(0, 100), stringGenOf(10).retryUntil(_.nonEmpty)).tupled): (a, b, c) =>
      Select(Query.of(a.asDbString.lpad(b.as(AnsiTypes.integer), c.asDbString)))
        .runOne[F]
        .map: r =>
          if a.length >= b then expect(r == a.take(b))
          else
            val padStr          = if padSingleCharacterOnly then c.take(1) else c
            val fullRepetitions = (b - a.length) / padStr.length
            val extraChars      = (b - a.length) % padStr.length

            val padded = padStr * fullRepetitions + padStr.take(extraChars) + a

            expect(r == padded)

  def doTestRpad()(using platform.SqlStringRpadCapability): Unit = dbTest("Rpad"):
    configuredForall((stringGen, Gen.choose(0, 100), stringGenOf(10).retryUntil(_.nonEmpty)).tupled): (a, b, c) =>
      Select(Query.of(a.asDbString.rpad(b.as(AnsiTypes.integer), c.asDbString)))
        .runOne[F]
        .map: r =>
          if a.length >= b then expect(r == a.take(b))
          else
            val padStr          = if padSingleCharacterOnly then c.take(1) else c
            val fullRepetitions = (b - a.length) / padStr.length
            val extraChars      = (b - a.length) % padStr.length

            val padded = a + padStr * fullRepetitions + padStr.take(extraChars)

            expect(r == padded)

  dbTest("Ltrim"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.ltrim))
        .runOne[F]
        .map: r =>
          val (space, content) = a.span(_.isSpaceChar)
          expect(r == a.dropWhile(_.isSpaceChar))

  dbTest("Rtrim"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.rtrim))
        .runOne[F]
        .map: l =>
          val (space, content) = a.span(_.isSpaceChar)
          expect(l == (space + content.trim))

  dbLogTest("IndexOf"): log =>
    configuredForall(
      Gen.frequency(
        9 -> (
          for
            str  <- stringGen
            from <- Gen.choose(0, str.length - 1)
            to   <- Gen.choose(from, str.length)
          yield (str, str.substring(from, to))
        ),
        1 -> (stringGen, stringGen).tupled
      )
    ): (a, b) =>
      Select(Query.of(a.asDbString.indexOf(b.asDbString), b.asDbString.indexOf(a.asDbString)))
        .runOne[F]
        .flatMap((r1, r2) => log.debug((r1, r2, a.indexOf(b), b.indexOf(a)).toString).as((r1, r2)))
        .map: (r1, r2) =>
          expect.all(
            r1 == (a.indexOf(b) + 1),
            r2 == (b.indexOf(a) + 1)
          )

  dbTest("Substr"):
    configuredForall(
      for
        str  <- stringGen
        from <- Gen.choose(0, str.length)
        len  <- Gen.choose(0, str.length - from)
      yield (str, from, len)
    ): (str, from, len) =>
      Select(Query.of(str.asDbString.substr(from.as(AnsiTypes.integer), len.as(AnsiTypes.integer))))
        .runOne[F]
        .map: r =>
          expect(r == str.substring(Math.max(0, from - 1), Math.max(0, from + len - 1)))

  def doTestTrimLeading()(using platform.SqlStringTrimLeadingCapability): Unit = dbLogTest("TrimLeading"): log =>
    given Log[IO] = log
    configuredForall((stringGen, stringGen.retryUntil(_.nonEmpty)).tupled): (a, b) =>
      logQuery(Select(Query.of(a.asDbString.trimLeading(b.asDbString))))
        .flatMap(_.runOne[F])
        .map: r =>
          expect(r == a.dropWhile(c => b.contains(c)))

  def doTestTrimTrailing()(using platform.SqlStringTrimTrailingCapability): Unit = dbLogTest("TrimTrailing"): log =>
    given Log[IO] = log
    configuredForall((stringGen, stringGen.retryUntil(_.nonEmpty)).tupled): (a, b) =>
      logQuery(Select(Query.of(a.asDbString.trimTrailing(b.asDbString))))
        .flatMap(_.runOne[F])
        .map: r =>
          expect(r == a.reverse.dropWhile(c => b.contains(c)).reverse)

  dbLogTest("TrimBoth"): log =>
    given Log[IO] = log
    configuredForall((stringGen, stringGen).tupled): (a, b) =>
      logQuery(Select(Query.of(a.asDbString.trimBoth(b.asDbString))))
        .flatMap(_.runOne[F])
        .map: r =>
          expect(r == a.dropWhile(c => b.contains(c)).reverse.dropWhile(c => b.contains(c)).reverse)

  // Just test that it runs, we test it a bit with starts with and ends with
  dbTest("Like"):
    configuredForall((stringGen, stringGen).tupled): (a, b) =>
      Select(Query.of(a.asDbString.like(b.asDbString))).runOne[F].map(_ => Expectations.multiplicativeMonoid.empty)

  // Just test that it runs
  def doRegexMatchesTest()(using platform.SqlStringRegexMatchesCapability): Unit = dbTest("Regex matches"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.matches(".*".asDbString)))
        .runOne[F]
        .map(_ => Expectations.multiplicativeMonoid.empty)

  dbTest("StartsWith"):
    configuredForall(
      Gen.frequency(
        5 -> (
          for
            str <- stringGen
            len <- Gen.choose(0, str.length)
          yield (str, str.substring(0, len))
        ),
        4 -> (stringGen, stringGen).tupled
      )
    ): (a, b) =>
      Select(Query.of(a.asDbString.startsWith(b.asDbString)))
        .runOne[F]
        .map: r =>
          expect(r == a.startsWith(b))

  dbTest("EndsWith"):
    configuredForall(
      Gen.frequency(
        5 -> (
          for
            str <- stringGen
            len <- Gen.choose(0, str.length)
          yield (str, str.substring(len, str.length))
        ),
        4 -> (stringGen, stringGen).tupled
      )
    ): (a, b) =>
      Select(Query.of(a.asDbString.endsWith(b.asDbString)))
        .runOne[F]
        .map: r =>
          expect(r == a.endsWith(b))

  def doTestLeft()(using platform.SqlStringLeftCapability): Unit = dbTest("Left"):
    configuredForall((stringGen, natIntGen).tupled): (a, b) =>
      Select(Query.of(a.asDbString.left(b.as(AnsiTypes.integer))))
        .runOne[F]
        .map: r =>
          val v = if b < 0 then a.takeRight(b) else a.take(b)
          expect(r == v)

  def doTestRight()(using platform.SqlStringRightCapability): Unit = dbTest("Right"):
    configuredForall((stringGen, natIntGen).tupled): (a, b) =>
      Select(Query.of(a.asDbString.right(b.as(AnsiTypes.integer))))
        .runOne[F]
        .map: r =>
          val v = if b > 0 then a.takeRight(b) else a.take(b)
          expect(r == v)

  private val HEX_ARRAY = "0123456789ABCDEF".toCharArray

  // https://stackoverflow.com/questions/9655181/java-convert-a-byte-array-to-a-hex-string
  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars: Array[Char] = new Array[Char](bytes.length * 2)
    for (j <- bytes.indices) {
      val v: Int = bytes(j) & 0xFF
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0F)
    }
    new String(hexChars)
  }

  def hash(alg: String, str: String): String =
    bytesToHex(MessageDigest.getInstance(alg).digest(str.getBytes(StandardCharsets.UTF_8)))

  def doTestMd5()(using platform.SqlStringMd5Capability): Unit = dbTest("Md5"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.md5))
        .runOne[F]
        .map: r =>
          expect(r.toUpperCase == hash("MD5", a))

  def doTestSha256()(using platform.SqlStringSha256Capability): Unit = dbTest("Sha256"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.sha256))
        .runOne[F]
        .map: r =>
          expect(r.toUpperCase == hash("SHA-256", a))

  dbTest("Replace"):
    configuredForall((stringGen, stringGen, stringGen).tupled): (a, b, c) =>
      Select(Query.of(a.asDbString.replace(b.asDbString, c.asDbString)))
        .runOne[F]
        .map: r =>
          expect(r == a.replace(b, c))

  def doTestReverse()(using platform.SqlStringReverseCapability): Unit = dbTest("Reverse"):
    configuredForall(stringGen): a =>
      Select(Query.of(a.asDbString.reverse))
        .runOne[F]
        .map: r =>
          expect(r == a.reverse)

  dbTest("ConcatSeq"):
    configuredForall((stringGen, Gen.listOfN(3, stringGenOf(10))).tupled): (x, xs) =>
      Select(Query.of(SqlString.concat(x.asDbString, xs.map(_.asDbString)*)))
        .runOne[F]
        .map: r =>
          expect.same((x +: xs).mkString, r)

  def concatWsIgnoresEmptyStrings: Boolean = false

  dbTest("ConcatWsSeq"):
    configuredForall((stringGenOf(10), stringGenOf(10), Gen.listOfN(3, stringGenOf(10))).tupled): (sep, x, xs) =>
      Select(Query.of(SqlString.concatWs(sep.asDbString, x.asDbString, xs.map(_.asDbString)*)))
        .runOne[F]
        .map: r =>
          expect.same((x +: xs).filter(s => if concatWsIgnoresEmptyStrings then s.nonEmpty else true).mkString(sep), r)

  def doTestHex()(using platform.SqlStringHexCapability): Unit = dbTest("Hex"):
    configuredForall(intGen): a =>
      Select(Query.of(SqlString.hex(a.toLong.as(AnsiTypes.bigint))))
        .runOne[F]
        .map: r =>
          expect(r.toUpperCase == java.lang.Long.toHexString(a).toUpperCase)
