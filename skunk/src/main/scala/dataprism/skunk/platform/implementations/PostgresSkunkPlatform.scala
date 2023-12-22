package dataprism.skunk.platform.implementations

import cats.data.State
import dataprism.platform.base.MapRes
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.skunk.sql.SkunkAnsiTypes
import dataprism.sql.AnsiTypes
import perspective.*
import skunk.{Codec, Decoder, data}
import skunk.data.Arr
import skunk.util.Origin

import scala.annotation.targetName

trait PostgresSkunkPlatform extends PostgresQueryPlatform {

  override type ArrayTypeArgs[_] = DummyImplicit
  override type Type[A]          = Codec[A]
  extension [A](tpe: Codec[A])
    @targetName("typeName")
    override def name: String = tpe.types.head.name

  override protected def arrayType[A](elemType: Codec[A])(using extraArrayTypeArgs: DummyImplicit): Codec[Seq[A]] =
    Codec
      .array[A](
        a => elemType.encode(a).head.get,
        s => elemType.decode(0, List(Some(s))).left.map(_.message),
        elemType.types.head
      )
      .imap(arr => Seq.tabulate(arr.size)(arr.get(_).get))(seq => Arr(seq: _*))

  override def AnsiTypes: AnsiTypes[Codec] = SkunkAnsiTypes

  type Compile = SkunkCompile
  object Compile extends SkunkCompile

  trait SkunkCompile extends SqlCompile:
    def queryK[A[_[_]]: ApplyKC: TraverseKC, Res[_[_]]](types: A[Codec])(f: A[DbValue] => ResultOperation[Res])(using origin: Origin): skunk.Query[A[Id], Res[Id]] =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] = types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues = tpesWithIdentifiers.mapK([Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue)
      val op = f(dbValues)
      import op.given
      val (sqlStr, resTypes) = op.sqlAndTypes

      skunk.Query(
        sqlStr.str,
        origin,
        new skunk.Encoder[A[Id]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: A[Id]): List[Option[String]] =
            val replacements = a.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
            sqlStr.args.toList.flatMap(arg => arg.tpe.encode(arg.compile(replacements).value))

          override def types: List[data.Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        },
        new skunk.Decoder[Res[Id]] {
          override def types: List[data.Type] = resTypes.foldMapK([Z] => (codec: Codec[Z]) => codec.types)

          override def decode(offset: Int, ss: List[Option[String]]): Either[Decoder.Error, Res[Id]] =
            val indicesState: State[Int, Res[[Z] =>> Either[Decoder.Error, Z]]] =
              resTypes.traverseK([Z] => (c: Codec[Z]) => State((acc: Int) => (acc + c.types.length, c.decode(acc, ss))))

            indicesState.runA(offset).value.sequenceIdK
        }
      )

    inline def query[A, Res[_[_]]](types: A)(using res: MapRes[Codec, A])(f: res.K[DbValue] => ResultOperation[Res])(using origin: Origin): skunk.Query[res.K[Id], Res[Id]] =
      queryK(res.toK(types))(f)(using res.applyKC, res.traverseKC, origin)

    def commandK[A[_[_]] : ApplyKC : TraverseKC](types: A[Codec])(f: A[DbValue] => IntOperation)(using origin: Origin): skunk.Command[A[Id]] =
      given FunctorKC[A] = summon[ApplyKC[A]]

      val tpesWithIdentifiers: A[Tuple2K[Const[Object], Type]] = types.mapK([Z] => (tpe: Type[Z]) => (new Object, tpe))

      val dbValues = tpesWithIdentifiers.mapK([Z] => (t: (Object, Type[Z])) => SqlDbValue.CompilePlaceholder(t._1, t._2).liftDbValue)
      val (sqlStr, _) = f(dbValues).sqlAndTypes

      skunk.Command(
        sqlStr.str,
        origin,
        new skunk.Encoder[A[Id]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: A[Id]): List[Option[String]] =
            val replacements = a.map2Const(tpesWithIdentifiers)([Z] => (v: Z, t: (Object, Type[Z])) => (t._1, v: Any)).toListK.toMap
            sqlStr.args.toList.flatMap(arg => arg.tpe.encode(arg.compile(replacements).value))

          override def types: List[data.Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        }
      )

    inline def command[A](types: A)(using res: MapRes[Codec, A])(f: res.K[DbValue] => IntOperation)(using origin: Origin): skunk.Command[res.K[Id]] =
      commandK(res.toK(types))(f)(using res.applyKC, res.traverseKC, origin)
}
object PostgresSkunkPlatform extends PostgresSkunkPlatform
