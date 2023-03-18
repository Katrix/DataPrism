package dataprism.platform.sql

import scala.annotation.targetName

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.sharedast.SelectAst.Data
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.{Column, Table}
import perspective.*
import perspective.derivation.ProductKPar

//noinspection ScalaUnusedSymbol
trait SqlQueryPlatformQuery { platform: SqlQueryPlatform =>

  trait SqlQueryBase[A[_[_]]] {

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    def addWhere(f: A[DbValue] => DbValue[Boolean]): Query[A]

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B]

    def addFlatMap[B[_[_]]](f: A[DbValue] => Query[B])(using ApplyKC[B], TraverseKC[B]): Query[B]

    def addInnerJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]]

    def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]]

    def addLeftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]]

    def addRightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]]

    def addFullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]]

    def addGroupMapK[B[_[_]]: FoldableKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C]

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A]

    def addLimit(i: Int): Query[A]

    def addOffset(i: Int): Query[A]

    def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])]

    def selectAst: SelectAst = selectAstAndValues.runA(freshTaggedState).value._1

    def applyK: ApplyKC[A]

    given ApplyKC[A] = applyK

    def traverseK: TraverseKC[A]

    given TraverseKC[A] = traverseK
  }

  type Query[A[_[_]]] <: SqlQueryBase[A]

  trait SqlQueryBaseGrouped[A[_[_]]] extends SqlQueryBase[A] {

    def addHaving(f: A[DbValue] => DbValue[Boolean]): QueryGrouped[A]
  }
  trait SqlQueryGrouped[A[_[_]]] extends SqlQueryBaseGrouped[A] with SqlQuery[A]

  type QueryGrouped[A[_[_]]] <: SqlQueryBaseGrouped[A]

  sealed trait SqlQuery[A[_[_]]] extends SqlQueryBase[A] {

    def nested: Query[A] =
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromQuery(this.liftSqlQuery).liftSqlValueSource).liftSqlQuery

    def addWhere(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      nested.addWhere(f)

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] = addWhere(f)

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
      nested.addMap(f)

    def addFlatMap[B[_[_]]](f: A[DbValue] => Query[B])(using ApplyKC[B], TraverseKC[B]): Query[B] =
      SqlQuery.SqlQueryFlatMap(this.liftSqlQuery, f).liftSqlQuery

    def addInnerJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]] = nested.addInnerJoin(that)(on)

    def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] = nested.addCrossJoin(that)

    def addLeftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = nested.addLeftJoin(that)(on)

    def addRightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = nested.addRightJoin(that)(on)

    def addFullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = nested.addFullJoin(that)(on)

    def addGroupMapK[B[_[_]]: FoldableKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C] = nested.addGroupMapK(group)(map)

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = nested.addOrderBy(f)

    def addLimit(i: Int): Query[A] = nested.addLimit(i)

    def addOffset(i: Int): Query[A] = nested.addOffset(i)
  }

  object SqlQuery {
    private def tagValues[A[_[_]]](
        values: A[DbValue]
    )(using TraverseKC[A], ApplyKC[A]): TagState[(A[Const[String]], Seq[SelectAst.ExprWithAlias])] =
      State { st =>
        val columnNum = st.columnNum

        val columnNumState: State[Int, A[Const[String]]] =
          values.traverseK(
            [X] => (_: DbValue[X]) => State[Int, Const[String][X]]((acc: Int) => (acc + 1, s"x$acc"))
          )

        val (newColumnNum, columnAliases) = columnNumState.run(columnNum).value

        val exprWithAliases = values
          .tupledK(columnAliases)
          .foldMapK(
            [X] => (t: (DbValue[X], String)) => List(SelectAst.ExprWithAlias(t._1.ast, Some(t._2)))
          )

        (st.withNewColumnNum(newColumnNum), (columnAliases, exprWithAliases))
      }

    case class SqlQueryWithoutFrom[A[_[_]]](values: A[DbValue])(
        using val applyK: ApplyKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {
      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        tagValues(values).map { case (aliases, exprWithAliases) =>
          val selectAst = SelectAst(
            SelectAst.Data.SelectFrom(
              None,
              selectExprs = exprWithAliases,
              None,
              None,
              None,
              None
            ),
            SelectAst.OrderLimit(None, None, None)
          )

          (selectAst, aliases, values)
        }

      override def addMap[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
        copy(f(values)).liftSqlQuery
    }

    type AppTravKC[A[_[_]]] = ApplyKC[A] with TraverseKC[A]

    private def innerJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplyKC[MA],
        BA: ApplyKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplyKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] =
      new ApplyKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] {
        type F[X[_]] = (MA[X], MB[X])

        extension [A[_], C](fa: F[A])
          def map2K[B[_], Z[_]](fb: F[B])(f: [X] => (A[X], B[X]) => Z[X]): F[Z] =
            (fa._1.map2K(fb._1)(f), fa._2.map2K(fb._2)(f))

        extension [A[_], C](fa: F[A])
          override def mapK[B[_]](f: A ~>: B): F[B] =
            (AA.mapK(fa._1)(f), BA.mapK(fa._2)(f))

        extension [A[_], C](fa: F[A])
          def traverseK[G[_]: Applicative, B[_]](f: A ~>: Compose2[G, B]): G[F[B]] =
            val r1 = fa._1.traverseK(f)
            val r2 = fa._2.traverseK(f)

            r1.product(r2)

        extension [A[_], C](fa: F[A])
          def foldLeftK[B](b: B)(f: B => A ~>#: B): B =
            val r1 = fa._1.foldLeftK(b)(f)
            val r2 = fa._2.foldLeftK(r1)(f)
            r2
      }

    private def leftJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplyKC[MA],
        BA: ApplyKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplyKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplyKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]]]

    private def rightJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplyKC[MA],
        BA: ApplyKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplyKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplyKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]]]

    private def fullJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplyKC[MA],
        BA: ApplyKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplyKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplyKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]]]

    case class SqlQueryFromStage[A[_[_]]](valueSource: ValueSource[A])(
        using val applyK: ApplyKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {

      override def addWhere(f: A[DbValue] => DbValue[Boolean]): Query[A] =
        SqlQueryMapWhereStage(valueSource).addWhere(f)

      override def addMap[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryMapWhereStage(valueSource).addMap(f)

      override def addInnerJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.InnerJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addCrossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.CrossJoin(valueSource, ValueSource.getFromQuery(that)).liftSqlValueSource
        ).liftSqlQuery

      override def addLeftJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[LeftJoin[A, B]] =
        import that.given
        given AppTravKC[LeftJoin[A, B]] = leftJoinInstances

        copy(
          SqlValueSource.LeftJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addRightJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[RightJoin[A, B]] =
        import that.given
        given AppTravKC[RightJoin[A, B]] = rightJoinInstances

        copy(
          SqlValueSource.RightJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addFullJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[FullJoin[A, B]] =
        import that.given
        given AppTravKC[FullJoin[A, B]] = fullJoinInstances

        copy(
          SqlValueSource.FullJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def addGroupMapK[B[_[_]]: FoldableKC, C[_[_]]: ApplyKC: TraverseKC](group: A[DbValue] => B[DbValue])(
          map: (B[DbValue], A[Many]) => C[DbValue]
      ): QueryGrouped[C] = SqlQueryGroupedHavingStage(this.liftSqlQuery, group, map).liftSqlQueryGrouped

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def addLimit(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        valueSource.fromPartAndValues.flatMap { case (from, values) =>
          tagValues(values).map { case (aliases, exprWithAliases) =>
            val selectAst = SelectAst(
              SelectAst.Data.SelectFrom(None, exprWithAliases, Some(from), None, None, None),
              SelectAst.OrderLimit(None, None, None)
            )

            (selectAst, aliases, values)
          }
        }
    }

    case class SqlQueryMapWhereStage[A[_[_]], B[_[_]]](
        valueSource: ValueSource[A],
        map: A[DbValue] => B[DbValue] = identity[A[DbValue]],
        where: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplyKC[A],
        FTA: TraverseKC[A],
        val applyK: ApplyKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {

      override def addWhere(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(map(values))
          where.fold(newBool)(old => old(values) && newBool)

        copy(where = Some(cond)).liftSqlQuery

      override def addMap[C[_[_]]](
          f: B[DbValue] => C[DbValue]
      )(using FA: ApplyKC[C], FT: TraverseKC[C]): Query[C] =
        copy(
          map = this.map.andThen(f)
        ).liftSqlQuery

      override def addOrderBy(f: B[DbValue] => OrdSeq): Query[B] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def addLimit(i: Int): Query[B] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[B] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        valueSource.fromPartAndValues.flatMap { case (from, values) =>
          val mappedValues = map(values)
          tagValues(mappedValues).map { case (aliases, exprWithAliases) =>
            val selectAst = SelectAst(
              SelectAst.Data.SelectFrom(
                None,
                exprWithAliases,
                Some(from),
                where.map(f => f(values).ast),
                None,
                None
              ),
              SelectAst.OrderLimit(None, None, None)
            )

            (selectAst, aliases, mappedValues)
          }
        }
    }

    case class SqlQueryGroupedHavingStage[A[_[_]], Gr[_[_]], Ma[_[_]]](
        query: Query[A],
        group: A[DbValue] => Gr[DbValue],
        map: (Gr[DbValue], A[Many]) => Ma[DbValue],
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplyKC[A],
        FTA: TraverseKC[A],
        GR: FoldableKC[Gr],
        val applyK: ApplyKC[Ma],
        val traverseK: TraverseKC[Ma]
    ) extends SqlQuery[Ma]
        with SqlQueryGrouped[Ma] {

      private inline def valuesAsMany(values: A[DbValue]): A[Many] =
        values.asInstanceOf[A[Many]] // Safe as many is an opaque type in another file

      override def addMap[X[_[_]]](
          f: Ma[DbValue] => X[DbValue]
      )(using FA: ApplyKC[X], FT: TraverseKC[X]): Query[X] =
        copy(
          map = (gr, a) => f(map(gr, a))
        ).liftSqlQuery

      override def addHaving(f: Ma[DbValue] => DbValue[Boolean]): QueryGrouped[Ma] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(map(group(values), valuesAsMany(values)))
          having.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQueryGrouped

      override def addFilter(f: Ma[DbValue] => DbValue[Boolean]): Query[Ma] = addHaving(f)

      override def addOrderBy(f: Ma[DbValue] => OrdSeq): Query[Ma] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def addLimit(i: Int): Query[Ma] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[Ma] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, Ma[Const[String]], Ma[DbValue])] =
        query.selectAstAndValues.flatMap { case (selectAst, _, values) =>
          val groupedBy  = group(values)
          val groupByAst = groupedBy.foldMapK([Z] => (dbVal: DbValue[Z]) => List(dbVal.ast))

          val groupedValues = map(groupedBy, valuesAsMany(values))

          def astAnd(lhs: Option[SqlExpr], rhs: Option[SqlExpr]): Option[SqlExpr] = (lhs, rhs) match
            case (Some(lhs), Some(rhs)) => Some(SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BoolAnd))
            case (Some(lhs), None)      => Some(lhs)
            case (None, Some(rhs))      => Some(rhs)
            case (None, None)           => None
          end astAnd

          tagValues(groupedValues).map { case (aliases, exprWithAliases) =>
            val newSelectAst = selectAst.copy(
              data = selectAst.data match
                case from: Data.SelectFrom =>
                  from.copy(
                    selectExprs = exprWithAliases,
                    groupBy = Some(SelectAst.GroupBy(groupByAst)),
                    having = astAnd(from.having, having.map(f => f(values).ast))
                  )

                case data: Data.SetOperatorData => data
            )

            (newSelectAst, aliases, groupedValues)
          }
        }
    }

    case class SqlQueryOrderedStage[A[_[_]]](
        query: Query[A],
        orderBy: A[DbValue] => OrdSeq
    ) extends SqlQuery[A] {
      export query.{applyK, traverseK}

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = copy(orderBy = f).liftSqlQuery

      override def addLimit(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        query.selectAstAndValues.map { case (selectAst, aliases, values) =>
          val oldOrderLimit = selectAst.orderLimit

          val orderByValuesAst = orderBy(values).ast

          val newOrderLimit = oldOrderLimit.copy(orderBy =
            Some(
              oldOrderLimit.orderBy.fold(SelectAst.OrderBy(orderByValuesAst)) { old =>
                old.copy(exprs = old.exprs ++ orderByValuesAst)
              }
            )
          )

          (selectAst.copy(orderLimit = newOrderLimit), aliases, values)
        }
    }

    case class SqlQueryLimitOffsetStage[A[_[_]]](
        query: Query[A],
        limit: Option[Int] = None,
        offset: Int = 0
    ) extends SqlQuery[A] {
      export query.{applyK, traverseK}

      override def addLimit(i: Int): Query[A] = copy(limit = Some(i)).liftSqlQuery

      override def addOffset(i: Int): Query[A] = copy(offset = i).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        query.selectAstAndValues.map { case (selectAst, aliases, values) =>
          val oldOrderLimit = selectAst.orderLimit

          val newLimitOffset = oldOrderLimit.limitOffset match {
            case Some(old: SelectAst.LimitOffset) =>
              Some(old.copy(limit = limit.orElse(old.limit), offset = if offset != 0 then offset else old.offset))

            case None if limit.isDefined || offset != 0 =>
              Some(SelectAst.LimitOffset(limit, offset, false))

            case None => None
          }

          (selectAst.copy(orderLimit = selectAst.orderLimit.copy(limitOffset = newLimitOffset)), aliases, values)
        }
    }

    case class SqlQueryFlatMap[A[_[_]], B[_[_]]](query: Query[A], f: A[DbValue] => Query[B])(
        using val applyK: ApplyKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {
      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        query.selectAstAndValues.flatMap { case (selectAstA, _, valuesA) =>
          val stNewValuesAndAstA: TagState[(Option[SelectAst.From], Option[SqlExpr], A[DbValue])] =
            if selectAstA.orderLimit.isEmpty && (selectAstA.data match
                case from: SelectAst.Data.SelectFrom =>
                  from.distinct.isEmpty && from.groupBy.isEmpty && from.having.isEmpty
                case _ => false
              )
            then
              val selectFrom = selectAstA.data.asInstanceOf[SelectAst.Data.SelectFrom]
              State.pure((selectFrom.from, selectFrom.where, valuesA))
            else SqlValueSource.FromQuery(query).fromPartAndValues.map(t => (Some(t._1), None, t._2))

          def combineOption[C](optA: Option[C], optB: Option[C])(combine: (C, C) => C): Option[C] = (optA, optB) match
            case (Some(a), Some(b)) => Some(combine(a, b))
            case (Some(a), None)    => Some(a)
            case (None, Some(b))    => Some(b)
            case (None, None)       => None

          stNewValuesAndAstA.flatMap { case (fromAOpt, whereExtra, newValuesA) =>
            f(newValuesA).selectAstAndValues.map { case (selectAstB, aliasesB, valuesB) =>
              val newSelectAstB = selectAstB.copy(
                data = selectAstB.data match
                  case from: Data.SelectFrom =>
                    from.copy(
                      from = combineOption(fromAOpt, from.from)(SelectAst.From.FromMulti.apply),
                      where = combineOption(whereExtra, from.where)((a, b) =>
                        SqlExpr.BinOp(a, b, SqlExpr.BinaryOperation.BoolAnd)
                      )
                    )

                  case data: Data.SetOperatorData => ???
              )

              (newSelectAstB, aliasesB, valuesB)
            }
          }
        }
    }
  }

  extension [A[_[_]]](sqlQuery: SqlQuery[A]) def liftSqlQuery: Query[A]

  extension [A[_[_]]](sqlQuery: SqlQueryGrouped[A]) def liftSqlQueryGrouped: QueryGrouped[A]

  extension (q: QueryCompanion)
    @targetName("queryCompanionFrom")
    def from[A[_[_]]](table: Table[A]): Query[A] =
      import table.given
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromTable(table).liftSqlValueSource).liftSqlQuery
    end from

  extension [A[_[_]]](query: Query[A])

    @targetName("queryFilter")
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      query.addFilter(f)

    @targetName("queryWhere")
    def where(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      query.addWhere(f)

    @targetName("queryMapK")
    def mapK[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplyKC[B], TraverseKC[B]): Query[B] =
      query.addMap(f)

    @targetName("querySelectK")
    inline def selectK[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplyKC[B], TraverseKC[B]): Query[B] =
      mapK(f)

    @targetName("queryFlatmap") def flatMap[B[_[_]]](
        f: A[DbValue] => Query[B]
    )(using ApplyKC[B], TraverseKC[B]): Query[B] =
      query.addFlatMap(f)

    @targetName("queryJoin")
    def join[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[InnerJoin[A, B]] =
      query.addInnerJoin(that)(on)

    @targetName("queryCrossJoin")
    def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] =
      query.addCrossJoin(that)

    @targetName("queryLeftJoin")
    def leftJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[LeftJoin[A, B]] =
      query.addLeftJoin(that)(on)

    @targetName("queryRightJoin")
    def rightJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[RightJoin[A, B]] =
      query.addRightJoin(that)(on)

    @targetName("queryFullJoin")
    def fullJoin[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[FullJoin[A, B]] =
      query.addFullJoin(that)(on)

    @targetName("queryJoin") def join[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[InnerJoin[A, B]] = query.join(Query.from(that))(on)

    @targetName("queryCrossJoin") def crossJoin[B[_[_]]](that: Table[B]): Query[InnerJoin[A, B]] =
      query.crossJoin(Query.from(that))

    @targetName("queryLeftJoin") def leftJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = query.leftJoin(Query.from(that))(on)

    @targetName("queryRightJoin") def rightJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = query.rightJoin(Query.from(that))(on)

    @targetName("queryFullJoin") def fullJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = query.fullJoin(Query.from(that))(on)

    @targetName("queryGroupMapK") def groupMapK[B[_[_]]: FoldableKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C] = query.addGroupMapK(group)(map)

    @targetName("queryOrderBy")
    def orderBy(f: A[DbValue] => OrdSeq): Query[A] = query.addOrderBy(f)

    @targetName("queryLimit")
    def take(n: Int): Query[A] = query.addLimit(n)

    @targetName("queryLimit") inline def limit(n: Int): Query[A] = take(n)

    @targetName("queryOffset")
    def drop(n: Int): Query[A] = query.addOffset(n)

    @targetName("queryLimit") inline def offset(n: Int): Query[A] = drop(n)
  end extension

  extension [A[_[_]]](query: QueryGrouped[A])
    @targetName("queryHaving") def having(f: A[DbValue] => DbValue[Boolean]): QueryGrouped[A] =
      query.addHaving(f)
}
