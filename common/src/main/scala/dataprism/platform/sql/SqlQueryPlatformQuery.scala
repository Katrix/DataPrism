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

trait SqlQueryPlatformQuery { platform: SqlQueryPlatform =>

  trait SqlQueryBase[A[_[_]]] {

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A]

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B]

    def addFlatMap[B[_[_]]](f: A[DbValue] => Query[B])(using ApplicativeKC[B], TraverseKC[B]): Query[B]

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

    def addGroupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B]

    def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A]

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A]

    def addLimit(i: Int): Query[A]

    def addOffset(i: Int): Query[A]

    def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])]

    def selectAst: SelectAst = selectAstAndValues.runA(freshTaggedState).value._1

    def applicativeK: ApplicativeKC[A]

    given ApplicativeKC[A] = applicativeK

    def traverseK: TraverseKC[A]

    given TraverseKC[A] = traverseK
  }

  type Query[A[_[_]]] <: SqlQueryBase[A]

  sealed trait SqlQuery[A[_[_]]] extends SqlQueryBase[A] {

    def nested: Query[A] =
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromQuery(this.liftSqlQuery).liftSqlValueSource).liftSqlQuery

    def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] = nested.addFilter(f)

    def addMap[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
      nested.addMap(f)

    def addFlatMap[B[_[_]]](f: A[DbValue] => Query[B])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
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

    def addGroupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
      nested.addGroupBy(f)

    def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A] = nested.addHaving(f)

    def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = nested.addOrderBy(f)

    def addLimit(i: Int): Query[A] = nested.addLimit(i)

    def addOffset(i: Int): Query[A] = nested.addOffset(i)
  }

  object SqlQuery {
    private def tagValues[A[_[_]]](
        values: A[DbValue]
    )(using TraverseKC[A], ApplicativeKC[A]): TagState[(A[Const[String]], Seq[SelectAst.ExprWithAlias])] =
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
        using val applicativeK: ApplicativeKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {
      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        tagValues(values).map { case (aliases, exprWithAliases) =>
          val selectAst = SelectAst(
            SelectAst.Data
              .SelectFrom(
                None,
                exprWithAliases,
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
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        copy(f(values)).liftSqlQuery
    }

    type AppTravKC[A[_[_]]] = ApplicativeKC[A] with TraverseKC[A]

    private def innerJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] =
      new ApplicativeKC[InnerJoin[MA, MB]] with TraverseKC[InnerJoin[MA, MB]] {
        type F[X[_]] = (MA[X], MB[X])

        extension [A[_]](a: ValueK[A])
          def pure[C]: F[A] =
            (a.pure, a.pure)

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
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]]]

    private def rightJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]]]

    private def fullJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]] =
      innerJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]]]

    case class SqlQueryFromStage[A[_[_]]](valueSource: ValueSource[A])(
        using val applicativeK: ApplicativeKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {

      override def addFilter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
        SqlQueryMapFilterHavingStage(valueSource).addFilter(f)

      override def addMap[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryMapFilterHavingStage(valueSource).addMap(f)

      override def addHaving(f: A[Grouped] => DbValue[Boolean]): Query[A] =
        SqlQueryMapFilterHavingStage(valueSource).addHaving(f)

      override def addGroupBy[B[_[_]]](
          f: A[Grouped] => B[DbValue]
      )(using FA: ApplicativeKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryFilterGroupByHavingStage[A, A](this.liftSqlQuery).addGroupBy(f)

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

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[A] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
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

    case class SqlQueryMapFilterHavingStage[A[_[_]], B[_[_]]](
        valueSource: ValueSource[A],
        map: A[DbValue] => B[DbValue] = identity[A[DbValue]],
        filter: Option[A[DbValue] => DbValue[Boolean]] = None,
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplicativeKC[A],
        FTA: TraverseKC[A],
        val applicativeK: ApplicativeKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {

      override def addFilter(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(map(values))
          filter.fold(newBool)(old => old(values) && newBool)

        copy(filter = Some(cond)).liftSqlQuery

      override def addMap[C[_[_]]](
          f: B[DbValue] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        copy(
          map = this.map.andThen(f),
          filter = this.filter
        ).liftSqlQuery

      override def addGroupBy[C[_[_]]](
          f: B[Grouped] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        SqlQueryFilterGroupByHavingStage[B, B](this.liftSqlQuery).addGroupBy(f)

      override def addHaving(f: B[Grouped] => DbValue[Boolean]): Query[B] =
        given FunctorKC[B] = applicativeK

        val cond = (values: A[DbValue]) =>
          val newBool = f(map(values).mapK([A] => (value: DbValue[A]) => value.asGrouped))
          filter.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addOrderBy(f: B[DbValue] => OrdSeq): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
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
                filter.map(f => f(values).ast),
                None,
                having.map(f => f(values).ast)
              ),
              SelectAst.OrderLimit(None, None, None)
            )

            (selectAst, aliases, mappedValues)
          }
        }
    }

    case class SqlQueryFilterGroupByHavingStage[A[_[_]], B[_[_]]](
        query: Query[A],
        groupBy: Option[A[Grouped] => B[DbValue]] = None,
        filter: Option[A[DbValue] => DbValue[Boolean]] = None,
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(using val applicativeK: ApplicativeKC[B], val traverseK: TraverseKC[B])
        extends SqlQuery[B] {

      import query.given

      private def valuesAsGrouped[C[_[_]]: ApplicativeKC](values: C[DbValue]): C[Grouped] =
        values.asInstanceOf[C[Grouped]] // Safe as Grouped is an opaque type in another file

      override def addFilter(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(
            groupBy.fold(values.asInstanceOf[B[DbValue]])(f => f(valuesAsGrouped(values)))
          )
          filter.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addGroupBy[C[_[_]]](
          f: B[Grouped] => C[DbValue]
      )(using FA: ApplicativeKC[C], FT: TraverseKC[C]): Query[C] =
        copy(groupBy =
          Some(
            groupBy.fold(f.asInstanceOf[A[Grouped] => C[DbValue]])(oldGroupBy =>
              oldGroupBy.andThen(bValues => f(valuesAsGrouped(bValues)))
            )
          )
        ).liftSqlQuery

      override def addHaving(f: B[Grouped] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(
            valuesAsGrouped(
              groupBy.fold(values.asInstanceOf[B[DbValue]])(f => f(valuesAsGrouped(values)))
            )
          )
          having.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQuery

      override def addOrderBy(f: B[DbValue] => OrdSeq): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        orderBy = Some(f)
      ).liftSqlQuery

      override def addLimit(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def addOffset(i: Int): Query[B] = SqlQueryOrderedLimitOffsetStage(
        this.liftSqlQuery,
        offset = Some(i)
      ).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        query.selectAstAndValues.flatMap { case (selectAst, _, values) =>
          given FunctorKC[A] = query.applicativeK

          val groupedValues = this.groupBy.fold(values.asInstanceOf[B[DbValue]])(f => f(valuesAsGrouped(values)))

          tagValues(groupedValues).map { case (aliases, exprWithAliases) =>
            val newSelectAst = selectAst.copy(
              data = selectAst.data match
                case from: SelectAst.Data.SelectFrom =>
                  val filterAst = this.filter.map(f => f(values).ast)
                  val havingAst = this.having.map(f => f(values).ast)

                  def astAnd(lhs: Option[SqlExpr], rhs: Option[SqlExpr]): Option[SqlExpr] = (lhs, rhs) match
                    case (Some(lhs), Some(rhs)) => Some(SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BoolAnd))
                    case (Some(lhs), None)      => Some(lhs)
                    case (None, Some(rhs))      => Some(rhs)
                    case (None, None)           => None

                  val optGroupBys = this.groupBy.map { _ =>
                    groupedValues.foldMapK(
                      [X] => (value: DbValue[X]) => if value.hasGroupBy then List(value.ast) else Nil
                    )
                  }

                  from
                    .copy(
                      selectExprs = exprWithAliases,
                      where = astAnd(from.where, filterAst),
                      groupBy = optGroupBys.map(SelectAst.GroupBy.apply),
                      having = astAnd(from.having, havingAst)
                    )

                case data: SelectAst.Data.SetOperatorData => data
            )

            (newSelectAst, aliases, groupedValues)
          }
        }
    }

    case class SqlQueryOrderedLimitOffsetStage[A[_[_]]](
        query: Query[A],
        orderBy: Option[A[DbValue] => OrdSeq] = None,
        limit: Option[Int] = None,
        offset: Option[Int] = None
    ) extends SqlQuery[A] {
      export query.{applicativeK, traverseK}

      override def addOrderBy(f: A[DbValue] => OrdSeq): Query[A] = copy(orderBy = Some(f)).liftSqlQuery

      override def addLimit(i: Int): Query[A] = copy(limit = Some(i)).liftSqlQuery

      override def addOffset(i: Int): Query[A] = copy(offset = Some(i)).liftSqlQuery

      override def selectAstAndValues: TagState[(SelectAst, A[Const[String]], A[DbValue])] =
        query.selectAstAndValues.map { case (selectAst, aliases, values) =>
          val orderLimit = selectAst.orderLimit

          val newOrderLimit =
            if orderLimit.isEmpty then
              SelectAst.OrderLimit(
                orderBy.map(f => SelectAst.OrderBy(f(values).ast)),
                if limit.isDefined || offset.isDefined then
                  Some(SelectAst.LimitOffset(limit, offset.getOrElse(0), withTies = false))
                else None,
                None
              )
            else orderLimit

          (selectAst.copy(orderLimit = newOrderLimit), aliases, values)
        }
    }

    case class SqlQueryFlatMap[A[_[_]], B[_[_]]](query: Query[A], f: A[DbValue] => Query[B])(
        using val applicativeK: ApplicativeKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {
      override def selectAstAndValues: TagState[(SelectAst, B[Const[String]], B[DbValue])] =
        query.selectAstAndValues.flatMap { case (selectAstA, aliasesA, valuesA) =>
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

          def combineOption[A](optA: Option[A], optB: Option[A])(combine: (A, A) => A): Option[A] = (optA, optB) match
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
                      where = combineOption(whereExtra, from.where)((a, b) => SqlExpr.BinOp(a, b, SqlExpr.BinaryOperation.BoolAnd))
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

  extension (q: QueryCompanion)
    @targetName("queryCompanionFrom")
    def from[A[_[_]]](table: Table[A])(using TraverseKC[A]): Query[A] =
      import table.given
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromTable(table).liftSqlValueSource).liftSqlQuery
    end from

  extension [A[_[_]]](query: Query[A])

    @targetName("queryFilter")
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      query.addFilter(f)

    @targetName("queryWhere")
    inline def where(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      filter(f)

    @targetName("queryMapK")
    def mapK[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      query.addMap(f)

    @targetName("querySelectK")
    inline def selectK[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      mapK(f)

    @targetName("querySelectT") inline def selectT[T <: NonEmptyTuple](f: A[DbValue] => T)(
      using ev: Tuple.IsMappedBy[DbValue][T]
    ): Query[ProductKPar[Tuple.InverseMap[T, DbValue]]] = query.mapT(f)

    @targetName("queryFlatmap") def flatMap[B[_[_]]](
        f: A[DbValue] => Query[B]
    )(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      query.addFlatMap(f)

    @targetName("queryJoin")
    def join[B[_[_]]](that: Query[B])(on: (A[DbValue], B[DbValue]) => DbValue[Boolean]): Query[InnerJoin[A, B]] =
      query.addInnerJoin(that)(on)

    @targetName("queryJoin")
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

    @targetName("queryCrossJoin") def crossJoin[B[_[_]]](that: Table[B])(using TraverseKC[B]): Query[InnerJoin[A, B]] =
      query.crossJoin(Query.from(that))

    @targetName("queryLeftJoin") def leftJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[LeftJoin[A, B]] = query.leftJoin(Query.from(that))(on)

    @targetName("queryRightJoin") def rightJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[RightJoin[A, B]] = query.rightJoin(Query.from(that))(on)

    @targetName("queryFullJoin") def fullJoin[B[_[_]]](that: Table[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[FullJoin[A, B]] = query.fullJoin(Query.from(that))(on)

    @targetName("queryGroupBy")
    def groupByK[B[_[_]]](f: A[Grouped] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] =
      query.addGroupBy(f)

    @targetName("queryHaving")
    def having(f: A[Grouped] => DbValue[Boolean]): Query[A] = query.addHaving(f)

    @targetName("queryOrderBy")
    def orderBy(f: A[DbValue] => OrdSeq): Query[A] = query.addOrderBy(f)

    @targetName("queryLimit")
    def take(n: Int): Query[A] = query.addLimit(n)

    @targetName("queryLimit") inline def limit(n: Int): Query[A] = take(n)

    @targetName("queryOffset")
    def drop(n: Int): Query[A] = query.addOffset(n)

    @targetName("queryLimit") inline def offset(n: Int): Query[A] = drop(n)
  end extension
}
