package dataprism.skunk.platform

import scala.annotation.targetName

import cats.data.State
import dataprism.platform.base.MapRes
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.skunk.sql.SkunkAnsiTypes
import dataprism.sql.{AnsiTypes, NullabilityTypeChoice}
import perspective.*
import skunk.data.Arr
import skunk.util.Origin
import skunk.{Codec, Decoder, data}

trait PostgresSkunkPlatform extends PostgresQueryPlatform {

  override type ArrayTypeArgs[_] = DummyImplicit
  override type Codec[A]         = skunk.Codec[A]
  extension [A](tpe: Type[A])
    @targetName("typeName")
    override def name: String = tpe.codec.types.head.name

  override protected def arrayType[A](elemType: Type[A])(using extraArrayTypeArgs: DummyImplicit): Type[Seq[A]] =
    NullabilityTypeChoice.notNullByDefault(
      Codec
        .array[A](
          a => elemType.codec.encode(a).head.get,
          s => elemType.codec.decode(0, List(Some(s))).left.map(_.message),
          elemType.codec.types.head
        )
        .imap(arr => Seq.tabulate(arr.size)(arr.get(_).get))(seq => Arr(seq: _*)),
      _.opt
    ).notNull

  override def AnsiTypes: AnsiTypes[Codec] = SkunkAnsiTypes

  type Compile = SkunkCompile
  object Compile extends SkunkCompile

  trait SkunkCompile extends SqlCompile:
    def queryK[A[_[_]]: ApplyKC: TraverseKC, Res[_[_]]](types: A[Type])(f: A[DbValue] => ResultOperation[Res])(
        using origin: Origin
    ): skunk.Query[A[Id], Res[Id]] =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] = types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues =
        tpesWithIdentifiers.mapK([Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue)
      val op = f(dbValues)
      import op.given
      val (sqlStr, resTypes) = op.sqlAndTypes

      skunk.Query(
        sqlStr.str,
        origin,
        new skunk.Encoder[A[Id]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: A[Id]): List[Option[String]] =
            val replacements =
              a.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
            sqlStr.args.toList.flatMap(arg => arg.tpe.encode(arg.compile(replacements).value))

          override def types: List[data.Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        },
        new skunk.Decoder[Res[Id]] {
          override def types: List[data.Type] = resTypes.foldMapK([Z] => (codec: Type[Z]) => codec.codec.types)

          override def decode(offset: Int, ss: List[Option[String]]): Either[Decoder.Error, Res[Id]] =
            val indicesState: State[Int, Res[[Z] =>> Either[Decoder.Error, Z]]] =
              resTypes.traverseK(
                [Z] => (c: Type[Z]) => State((acc: Int) => (acc + c.codec.types.length, c.codec.decode(acc, ss)))
              )

            indicesState.runA(offset).value.sequenceIdK
        }
      )

    inline def query[A, Res[_[_]]](types: A)(using res: MapRes[Type, A])(f: res.K[DbValue] => ResultOperation[Res])(
        using origin: Origin
    ): skunk.Query[res.K[Id], Res[Id]] =
      queryK(res.toK(types))(f)(using res.applyKC, res.traverseKC, origin)

    def commandK[A[_[_]]: ApplyKC: TraverseKC](types: A[Type])(f: A[DbValue] => IntOperation)(
        using origin: Origin
    ): skunk.Command[A[Id]] =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] = types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues =
        tpesWithIdentifiers.mapK([Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue)
      val (sqlStr, _) = f(dbValues).sqlAndTypes

      skunk.Command(
        sqlStr.str,
        origin,
        new skunk.Encoder[A[Id]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: A[Id]): List[Option[String]] =
            val replacements =
              a.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
            sqlStr.args.toList.flatMap(arg => arg.tpe.encode(arg.compile(replacements).value))

          override def types: List[data.Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        }
      )

    inline def command[A](types: A)(using res: MapRes[Type, A])(f: res.K[DbValue] => IntOperation)(
        using origin: Origin
    ): skunk.Command[res.K[Id]] =
      commandK(res.toK(types))(f)(using res.applyKC, res.traverseKC, origin)
}
object PostgresSkunkPlatform extends PostgresSkunkPlatform
