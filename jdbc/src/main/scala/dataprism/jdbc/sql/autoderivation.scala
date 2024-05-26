package dataprism.jdbc.sql

import scala.annotation.StaticAnnotation
import scala.quoted.*
import scala.reflect.TypeTest

import dataprism.KMacros
import dataprism.sql.{Column, SelectedType, Table}
import perspective.{ApplyKC, FunctorK, FunctorKC, Id, TraverseKC}

class jdbcType[A](tpe: SelectedType[JdbcCodec, A]) extends StaticAnnotation

case class JdbcColumns[F[_[_]]](columns: F[[A] =>> Column[JdbcCodec, A]]):
  def prefixWith(name: String)(using FunctorKC[F]): JdbcColumns[F] =
    JdbcColumns(columns.mapK([Z] => (col: Column[JdbcCodec, Z]) => Column(name + col.name, col.tpe)))

object JdbcColumns:
  inline def derived[F[_[_]]]: JdbcColumns[F] = ${ derivedImpl[F] }

  def derivedImpl[F[_[_]]](using quotes: Quotes, fTpe: Type[F]): Expr[JdbcColumns[F]] =
    import quotes.reflect.*
    val namedSym = Symbol.requiredClass("dataprism.sql.named")

    val fType = TypeRepr.of[F]
    if !summon[TypeTest[TypeRepr, TypeLambda]].unapply(fType).isDefined then
      report.errorAndAbort(s"${Type.show[F]} not instance of TypeLambda")

    val fTypeLambda                                        = fType.asInstanceOf[TypeLambda]
    val TypeLambda(fTypeLambdaNames, fTypeLambdaBounds, _) = fTypeLambda

    val jdbcColumnsType = Symbol
      .requiredPackage("dataprism")
      .termRef
      .select(Symbol.requiredPackage("dataprism.jdbc"))
      .select(Symbol.requiredPackage("dataprism.jdbc.sql"))
      .select(Symbol.requiredClass("dataprism.jdbc.sql.JdbcColumns"))
    val functorKcType = Symbol.requiredPackage("perspective").typeMember("FunctorKC").typeRef

    val fSym = fType.typeSymbol

    if !fSym.isClassDef || fSym.caseFields.isEmpty then
      report.errorAndAbort(s"${Type.show[F]} must be a case class with fields")

    if fSym.primaryConstructor.paramSymss.length != 2 then
      report.errorAndAbort(s"${Type.show[F]} must have one type parameter block, and one value parameter block")

    val paramSyms = fSym.primaryConstructor.paramSymss(1)

    val columnsE: List[Either[(String, Option[Position]), Term]] =
      paramSyms.map { paramSym =>
        val paramType = fType.memberType(paramSym)
        val fieldType = fType.memberType(fSym.declaredField(paramSym.name))

        val nameExpr = paramSym
          .getAnnotation(namedSym)
          .map(_.asExpr)
          .map { case '{ new dataprism.sql.named($name) } =>
            name.asExprOf[String]
          }
          .getOrElse(Expr(paramSym.name))

        val typeExpr: Either[(String, Option[Position]), Either[
          (Expr[Any], Type[? <: AnyKind]),
          (Expr[JdbcColumns[?]], Expr[FunctorK[?]])
        ]] =
          val fieldTypeUpd = TypeLambda(
            fTypeLambdaNames.map(_ + "Upd"),
            _ => fTypeLambdaBounds,
            l =>
              KMacros.replaceTypes(fieldType)(
                fTypeLambdaNames.indices.toList.map(idx => fTypeLambda.param(idx)),
                fTypeLambdaNames.indices.toList.map(idx => l.param(idx))
              )
          )

          def implicitSearch(tpe: TypeRepr): Either[String, Term] = Implicits.search(tpe) match
            case iss: ImplicitSearchSuccess => Right(iss.tree)
            case isf: ImplicitSearchFailure =>
              Left(isf.explanation)

          val searchColumns = implicitSearch(AppliedType(jdbcColumnsType, List(fieldTypeUpd)))
          val searchFunctor = implicitSearch(AppliedType(functorKcType, List(fieldTypeUpd)))

          (searchColumns, searchFunctor) match
            case (Right(tree), Right(functor)) =>
              Right(Right(tree.asExprOf[JdbcColumns[?]] -> functor.asExprOf[FunctorK[?]]))
            case (e1, e2) =>
              // println((e1, e2))
              paramType match
                case AppliedType(TypeRef(_, _), List(v)) =>
                  paramSym.annotations
                    .map(_.asExpr)
                    .collectFirst { case '{ new dataprism.jdbc.sql.jdbcType[t]($tpe) } =>
                      if v.dealias.simplified =:= TypeRepr.of[t] then Right(Left(tpe -> Type.of[t]))
                      else
                        Left((s"Invalid type, ${v.show} and ${Type.show[t]} are different", paramType.typeSymbol.pos))
                    }
                    .getOrElse {
                      v match
                        case TypeRef(path, _) if path.derivesFrom(Symbol.requiredClass("dataprism.sql.SelectedType")) =>
                          val pathTermSymbols = Seq.unfold(path) {
                            case t @ TermRef(innerPath, _) => Some((t.termSymbol, innerPath))
                            case _                         => None
                          }
                          val term =
                            pathTermSymbols.init.foldRight(Ident(pathTermSymbols.last.termRef))((sym, term) =>
                              term.select(sym)
                            )

                          Right(Left(term.asExpr -> v.dealias.asType))
                        case TypeRef(_, _) =>
                          Left(
                            (
                              s"${v.show} is not a SelectedType and no JdbcColumns or FunctorKC found for parameter ${paramSym.name}",
                              v.typeSymbol.pos
                            )
                          )
                        case _ => Left((s"Unknown type ${v.show}", v.typeSymbol.pos))
                    }

                case AppliedType(_, _) =>
                  Left((s"No JdbcColumns or FunctorKC found for parameter ${paramSym.name}", paramType.typeSymbol.pos))

                case _ =>
                  Left(
                    (
                      s"Param ${paramSym.name} has no Jdbc type specified or has an unknown scala type shape",
                      paramType.typeSymbol.pos
                    )
                  )
        end typeExpr

        typeExpr.map {
          case Right((cols, functor)) =>
            type G[_[_]] = Any
            val colsT    = cols.asInstanceOf[Expr[JdbcColumns[G]]]
            val functorT = functor.asInstanceOf[Expr[FunctorKC[G]]]
            '{ $colsT.prefixWith($nameExpr)(using $functorT).columns }.asTerm
          case Left((tpeExpr, '[t])) => '{ Column($nameExpr, ${ tpeExpr.asExprOf[SelectedType[JdbcCodec, t]] }) }.asTerm
          case _                     => report.errorAndAbort("impossible")
        }
      }

    if columnsE.exists(_.isLeft) then
      val errors = columnsE.filter(_.isLeft).map(_.left.toOption.get)
      errors.foreach {
        case (e, Some(p)) => report.error(e, p)
        case (e, None)    => report.error(e)
      }
      errors.last match
        case (e, Some(p)) => report.errorAndAbort(e, p)
        case (e, None)    => report.errorAndAbort(e)

    val columns = columnsE.map(_.toOption.get)
    '{
      JdbcColumns(${
        New(TypeTree.of[F])
          .select(fSym.primaryConstructor)
          .appliedToTypes(List(TypeRepr.of[[A] =>> Column[JdbcCodec, A]]))
          .appliedToArgs(columns)
          .asExprOf[F[[A] =>> Column[JdbcCodec, A]]]
      })
    }

  def table[F[_[_]]: ApplyKC: TraverseKC](tableName: String)(using t: JdbcColumns[F]): Table[JdbcCodec, F] =
    Table(tableName, t.columns)
