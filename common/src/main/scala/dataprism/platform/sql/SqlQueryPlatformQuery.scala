package dataprism.platform.sql

import scala.annotation.targetName

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*
import perspective.derivation.ProductKPar

//noinspection ScalaUnusedSymbol
trait SqlQueryPlatformQuery { platform: SqlQueryPlatform =>

  case class QueryAstMetadata[A[_[_]]](ast: SelectAst[Codec], aliases: A[Const[String]], values: A[DbValue])

  trait SqlQueryBase[A[_[_]]] extends QueryBase[A] {

    private[platform] def selectAstAndValues: TagState[QueryAstMetadata[A]]

    def selectAst: SelectAst[Codec] = selectAstAndValues.runA(freshTaggedState).value.ast

    def distinct: Query[A]

    def join[B[_[_]]](that: Table[B, Codec])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    )(using TraverseKC[B]): Query[InnerJoin[A, B]] = this.join(Query.from(that))(on)

    def crossJoin[B[_[_]]](that: Table[B, Codec]): Query[InnerJoin[A, B]] =
      this.crossJoin(Query.from(that))

    def leftJoin[B[_[_]]](that: Table[B, Codec])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = this.leftJoin(Query.from(that))(on)

    def fullJoin[B[_[_]]](that: Table[B, Codec])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = this.fullJoin(Query.from(that))(on)

    inline def limit(n: Int): Query[A] = take(n)

    inline def offset(n: Int): Query[A] = drop(n)

    def union(that: Query[A]): Query[A]
    def unionAll(that: Query[A]): Query[A]

    def intersect(that: Query[A]): Query[A]
    def intersectAll(that: Query[A]): Query[A]

    def except(that: Query[A]): Query[A]
    def exceptAll(that: Query[A]): Query[A]

    // TODO: Ensure the type of this will always be Long
    def size: DbValue[Long] = this.map(_ => Query.queryCount).asDbValue

    inline def count: DbValue[Long] = this.size

    def nonEmpty: DbValue[Boolean] = size > 0.toLong.as(AnsiTypes.bigint)

    def isEmpty: DbValue[Boolean] = size === 0.toLong.as(AnsiTypes.bigint)

    def applyK: ApplyKC[A]

    given ApplyKC[A] = applyK

    def traverseK: TraverseKC[A]

    given TraverseKC[A] = traverseK
  }

  type Query[A[_[_]]] <: SqlQueryBase[A]

  trait SqlQueryGrouped[A[_[_]]] extends QueryGroupedBase[A] with SqlQuery[A]

  type QueryGrouped[A[_[_]]] <: QueryGroupedBase[A]

  sealed trait SqlQuery[A[_[_]]] extends SqlQueryBase[A] {

    def nested: Query[A] =
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromQuery(this.liftSqlQuery).liftSqlValueSource).liftSqlQuery

    override def where(f: A[DbValue] => DbValue[Boolean]): Query[A] =
      nested.where(f)

    override def filter(f: A[DbValue] => DbValue[Boolean]): Query[A] = where(f)

    override def mapK[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
      nested.mapK(f)

    override def flatMap[B[_[_]]](f: A[DbValue] => Query[B])(using ApplyKC[B], TraverseKC[B]): Query[B] =
      SqlQuery.SqlQueryFlatMap(this.liftSqlQuery, f).liftSqlQuery

    override def join[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[InnerJoin[A, B]] = nested.join(that)(on)

    override def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] = nested.crossJoin(that)

    override def leftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = nested.leftJoin(that)(on)

    override def rightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = nested.rightJoin(that)(on)

    def rightJoin[B[_[_]]](that: Table[B, Codec])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = this.rightJoin(Query.from(that))(on)

    override def fullJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = nested.fullJoin(that)(on)

    override def groupMapK[B[_[_]]: TraverseKC, C[_[_]]: ApplyKC: TraverseKC](
        group: A[DbValue] => B[DbValue]
    )(map: (B[DbValue], A[Many]) => C[DbValue]): QueryGrouped[C] = nested.groupMapK(group)(map)

    override def orderBy(f: A[DbValue] => OrdSeq): Query[A] = nested.orderBy(f)

    override def take(n: Int): Query[A] = nested.take(n)

    override def drop(n: Int): Query[A] = nested.drop(n)

    override def distinct: Query[A] = nested.distinct

    override def union(that: Query[A]): Query[A]    = nested.union(that)
    override def unionAll(that: Query[A]): Query[A] = nested.unionAll(that)

    override def intersect(that: Query[A]): Query[A]    = nested.intersect(that)
    override def intersectAll(that: Query[A]): Query[A] = nested.intersectAll(that)

    override def except(that: Query[A]): Query[A]    = nested.except(that)
    override def exceptAll(that: Query[A]): Query[A] = nested.exceptAll(that)
  }

  object SqlQuery {
    private def tagValues[A[_[_]]](
        values: A[DbValue]
    )(using TraverseKC[A], ApplyKC[A]): TagState[(A[Const[String]], Seq[SelectAst.ExprWithAlias[Codec]])] =
      State[TaggedState, (A[Const[String]], TagState[List[SelectAst.ExprWithAlias[Codec]]])] { st =>
        val columnNum = st.columnNum

        val columnNumState: State[Int, A[Const[String]]] =
          values.traverseK(
            [X] => (dbVal: DbValue[X]) => State[Int, Const[String][X]]((acc: Int) => (acc + 1, dbVal.columnName(s"x$acc")))
          )

        val (newColumnNum, columnAliases) = columnNumState.run(columnNum).value

        val valuesAstSt = values.traverseK[TagState, Const[SqlExpr[Codec]]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)

        val exprWithAliasesSt = valuesAstSt.map { valuesAst =>
          valuesAst
            .tupledK(columnAliases)
            .foldMapK(
              [X] => (t: (SqlExpr[Codec], String)) => List(SelectAst.ExprWithAlias(t._1, Some(t._2)))
            )
        }

        (st.withNewColumnNum(newColumnNum), (columnAliases, exprWithAliasesSt))
      }.flatMap(t => t._2.map(aliases => (t._1, aliases)))

    case class SqlQueryWithoutFrom[A[_[_]]](values: A[DbValue])(
        using val applyK: ApplyKC[A],
        val traverseK: TraverseKC[A]
    ) extends SqlQuery[A] {
      override def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        tagValues(values).map { case (aliases, exprWithAliases) =>
          val selectAst = SelectAst.SelectFrom(
            None,
            selectExprs = exprWithAliases,
            None,
            None,
            None,
            None,
            None,
            None,
            None
          )

          QueryAstMetadata(selectAst, aliases, values)
        }

      override def mapK[B[_[_]]](
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
          override def mapK[B[_]](f: A :~>: B): F[B] =
            (AA.mapK(fa._1)(f), BA.mapK(fa._2)(f))

        extension [A[_], C](fa: F[A])
          def traverseK[G[_]: Applicative, B[_]](f: A :~>: Compose2[G, B]): G[F[B]] =
            val r1 = fa._1.traverseK(f)
            val r2 = fa._2.traverseK(f)

            (r1, r2).tupled

        extension [A[_], C](fa: F[A])
          def foldLeftK[B](b: B)(f: B => A :~>#: B): B =
            val r1 = fa._1.foldLeftK(b)(f)
            val r2 = fa._2.foldLeftK(r1)(f)
            r2

          def foldRightK[B](b: B)(f: A :~>#: (B => B)): B =
            val r1 = fa._2.foldRightK(b)(f)
            val r2 = fa._1.foldRightK(r1)(f)
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

      override def nested: Query[A] = this.liftSqlQuery

      override def where(f: A[DbValue] => DbValue[Boolean]): Query[A] =
        SqlQueryMapWhereStage(valueSource).where(f)

      override def mapK[B[_[_]]](
          f: A[DbValue] => B[DbValue]
      )(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
        SqlQueryMapWhereStage(valueSource).mapK(f) // TODO: Maybe send this to the value source instead?

      override def join[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.InnerJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def crossJoin[B[_[_]]](that: Query[B]): Query[InnerJoin[A, B]] =
        import that.given
        given AppTravKC[InnerJoin[A, B]] = innerJoinInstances

        copy(
          SqlValueSource.CrossJoin(valueSource, ValueSource.getFromQuery(that)).liftSqlValueSource
        ).liftSqlQuery

      override def leftJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[LeftJoin[A, B]] =
        import that.given
        given AppTravKC[LeftJoin[A, B]] = leftJoinInstances

        copy(
          SqlValueSource.LeftJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def rightJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[RightJoin[A, B]] =
        import that.given
        given AppTravKC[RightJoin[A, B]] = rightJoinInstances

        copy(
          SqlValueSource.RightJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def fullJoin[B[_[_]]](that: Query[B])(
          on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
      ): Query[FullJoin[A, B]] =
        import that.given
        given AppTravKC[FullJoin[A, B]] = fullJoinInstances

        copy(
          SqlValueSource.FullJoin(valueSource, ValueSource.getFromQuery(that), on).liftSqlValueSource
        ).liftSqlQuery

      override def groupMapK[B[_[_]]: TraverseKC, C[_[_]]: ApplyKC: TraverseKC](group: A[DbValue] => B[DbValue])(
          map: (B[DbValue], A[Many]) => C[DbValue]
      ): QueryGrouped[C] = SqlQueryGroupedHavingStage(this.liftSqlQuery, group, map).liftSqlQueryGrouped

      override def orderBy(f: A[DbValue] => OrdSeq): Query[A] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def distinct: Query[A] = SqlQueryDistinctStage(
        this.liftSqlQuery,
        defaultDistinct = true
      ).liftSqlQuery

      override def union(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(false), that))).liftSqlQuery
      override def unionAll(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(true), that))).liftSqlQuery

      override def intersect(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(false), that))).liftSqlQuery
      override def intersectAll(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(true), that))).liftSqlQuery

      override def except(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(false), that))).liftSqlQuery
      override def exceptAll(that: Query[A]): Query[A] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(true), that))).liftSqlQuery

      override def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        valueSource.fromPartAndValues.flatMap { case ValueSourceAstMetaData(from, values) =>
          tagValues(values).map { case (aliases, exprWithAliases) =>
            val selectAst = SelectAst.SelectFrom(None, exprWithAliases, Some(from), None, None, None, None, None, None)

            QueryAstMetadata(selectAst, aliases, values)
          }
        }
    }

    case class SqlQueryMapWhereStage[A[_[_]], B[_[_]]](
        valueSource: ValueSource[A],
        mapV: A[DbValue] => B[DbValue] = identity[A[DbValue]],
        whereV: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplyKC[A],
        FTA: TraverseKC[A],
        val applyK: ApplyKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {

      override def where(f: B[DbValue] => DbValue[Boolean]): Query[B] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(mapV(values))
          whereV.fold(newBool)(old => old(values) && newBool)

        copy(whereV = Some(cond)).liftSqlQuery

      override def mapK[C[_[_]]](
          f: B[DbValue] => C[DbValue]
      )(using FA: ApplyKC[C], FT: TraverseKC[C]): Query[C] =
        copy(
          mapV = this.mapV.andThen(f)
        ).liftSqlQuery

      override def groupMapK[C[_[_]]: TraverseKC, D[_[_]]: ApplyKC: TraverseKC](group: B[DbValue] => C[DbValue])(
          map: (C[DbValue], B[Many]) => D[DbValue]
      ): QueryGrouped[D] = SqlQueryGroupedHavingStage(this.liftSqlQuery, group, map).liftSqlQueryGrouped

      override def orderBy(f: B[DbValue] => OrdSeq): Query[B] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[B] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[B] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def distinct: Query[B] = SqlQueryDistinctStage(
        this.liftSqlQuery,
        defaultDistinct = true
      ).liftSqlQuery

      override def union(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(false), that))).liftSqlQuery

      override def unionAll(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(true), that))).liftSqlQuery

      override def intersect(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(false), that))).liftSqlQuery

      override def intersectAll(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(true), that))).liftSqlQuery

      override def except(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(false), that))).liftSqlQuery

      override def exceptAll(that: Query[B]): Query[B] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(true), that))).liftSqlQuery

      override def selectAstAndValues: TagState[QueryAstMetadata[B]] =
        for
          meta <- valueSource.fromPartAndValues
          mappedValues = mapV(meta.values)
          t <- tagValues(mappedValues)
          (aliases, exprWithAliases) = t
          wherePart <- whereV.traverse(f => f(meta.values).ast)
        yield QueryAstMetadata(
          SelectAst.SelectFrom(
            None,
            exprWithAliases,
            Some(meta.ast),
            wherePart,
            None,
            None,
            None,
            None,
            None
          ),
          aliases,
          mappedValues
        )
    }

    case class SqlQueryGroupedHavingStage[A[_[_]], Gr[_[_]], Ma[_[_]]](
        query: Query[A],
        group: A[DbValue] => Gr[DbValue],
        map: (Gr[DbValue], A[Many]) => Ma[DbValue],
        having: Option[A[DbValue] => DbValue[Boolean]] = None
    )(
        using FAA: ApplyKC[A],
        FTA: TraverseKC[A],
        GR: TraverseKC[Gr],
        val applyK: ApplyKC[Ma],
        val traverseK: TraverseKC[Ma]
    ) extends SqlQuery[Ma]
        with SqlQueryGrouped[Ma] {

      private inline def valuesAsMany(values: A[DbValue]): A[Many] =
        values.asInstanceOf[A[Many]] // Safe as many is an opaque type in another file

      override def mapK[X[_[_]]](
          f: Ma[DbValue] => X[DbValue]
      )(using FA: ApplyKC[X], FT: TraverseKC[X]): Query[X] =
        copy(
          map = (gr, a) => f(map(gr, a))
        ).liftSqlQuery

      override def having(f: Ma[DbValue] => DbValue[Boolean]): QueryGrouped[Ma] =
        val cond = (values: A[DbValue]) =>
          val newBool = f(map(group(values), valuesAsMany(values)))
          having.fold(newBool)(old => old(values) && newBool)

        copy(having = Some(cond)).liftSqlQueryGrouped

      override def filter(f: Ma[DbValue] => DbValue[Boolean]): Query[Ma] = having(f)

      override def orderBy(f: Ma[DbValue] => OrdSeq): Query[Ma] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[Ma] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[Ma] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def distinct: Query[Ma] = SqlQueryDistinctStage(
        this.liftSqlQuery,
        defaultDistinct = true
      ).liftSqlQuery

      override def union(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(false), that))).liftSqlQuery

      override def unionAll(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Union(true), that))).liftSqlQuery

      override def intersect(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(false), that))).liftSqlQuery

      override def intersectAll(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Intersect(true), that))).liftSqlQuery

      override def except(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(false), that))).liftSqlQuery

      override def exceptAll(that: Query[Ma]): Query[Ma] =
        SqlQuerySetOperations(this.liftSqlQuery, Seq((SetOperation.Except(true), that))).liftSqlQuery

      override def selectAstAndValues: TagState[QueryAstMetadata[Ma]] =
        for
          meta <- query.selectAstAndValues
          QueryAstMetadata(selectAst, _, values) = meta
          groupedBy                              = group(values)
          groupByAst <- groupedBy.traverseK[TagState, Const[SqlExpr[Codec]]]([Z] => (dbVal: DbValue[Z]) => dbVal.ast)
          groupByAstList = groupByAst.toListK

          havingAst <- having.traverse(f => f(values).ast)

          groupedValues = map(groupedBy, valuesAsMany(values))
          t <- tagValues(groupedValues)
          (aliases, exprWithAliases) = t
        yield
          def astAnd(lhs: Option[SqlExpr[Codec]], rhs: Option[SqlExpr[Codec]]): Option[SqlExpr[Codec]] = (lhs, rhs) match
            case (Some(lhs), Some(rhs)) => Some(SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BoolAnd))
            case (Some(lhs), None)      => Some(lhs)
            case (None, Some(rhs))      => Some(rhs)
            case (None, None)           => None
          end astAnd

          selectAst match
            case from: SelectAst.SelectFrom[Codec] =>
              QueryAstMetadata(
                from.copy(
                  selectExprs = exprWithAliases,
                  groupBy = Option.when(groupByAstList.nonEmpty)(SelectAst.GroupBy(groupByAstList)),
                  having = astAnd(from.having, havingAst)
                ),
                aliases,
                groupedValues
              )

            case _ =>
              // TODO: Allow
              throw new Exception("Found non SelectFrom ast in groupByHaving stage. Not allowed")
    }

    case class SqlQueryDistinctStage[A[_[_]]](
        query: Query[A],
        defaultDistinct: Boolean
    ) extends SqlQuery[A] {
      export query.{applyK, traverseK}

      override def orderBy(f: A[DbValue] => OrdSeq): Query[A] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        query.selectAstAndValues.flatMap { meta =>
          meta.ast match
            case from: SelectAst.SelectFrom[Codec] =>
              State.pure(meta.copy(ast = from.copy(distinct = Some(SelectAst.Distinct(Nil)))))
            case _ =>
              for
                queryNum <- State((s: TaggedState) => (s.withNewQueryNum(s.queryNum + 1), s.queryNum))
                queryName = s"y$queryNum"

                newValues = meta.aliases.map2K(meta.values)(
                  [X] =>
                    (alias: String, value: DbValue[X]) => SqlDbValue.QueryColumn[X](alias, queryName, value.tpe).lift
                )
                t <- tagValues(newValues)
                (aliases, exprs) = t
              yield QueryAstMetadata(
                SelectAst.SelectFrom(
                  Some(SelectAst.Distinct(Nil)),
                  exprs,
                  Some(SelectAst.From.FromQuery(meta.ast, queryName)),
                  None,
                  None,
                  None,
                  None,
                  None,
                  None
                ),
                aliases,
                newValues
              )
        }
    }

    case class SqlQueryValues[A[_[_]]](
        value: A[DbValue],
        values: Seq[A[DbValue]]
    )(using val applyK: ApplyKC[A], val traverseK: TraverseKC[A])
        extends SqlQuery[A] {

      override def mapK[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
        copy(f(value), values.map(a => f(a))).liftSqlQuery
      override def orderBy(f: A[DbValue] => OrdSeq): Query[A] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override private[platform] def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        given FunctorKC[A] = applyK

        State[TaggedState, TagState[QueryAstMetadata[A]]] { st =>
          val queryNum  = st.queryNum
          val queryName = s"y$queryNum"

          val columnName = st.columnNum

          val columnNumState: State[Int, A[[X] =>> (DbValue[X], String)]] =
            value.traverseK(
              [X] =>
                (dbVal: DbValue[X]) =>
                  State[Int, (DbValue[X], String)]((acc: Int) =>
                    val colName = dbVal.columnName(s"x$acc")
                    (acc + 1, (SqlDbValue.QueryColumn[X](colName, queryName, dbVal.tpe).lift, colName))
                )
            )

          val (newColumnNum, columns) = columnNumState.run(columnName).value
          val dbValues                = columns.mapK([Z] => (t: (DbValue[Z], String)) => t._1)
          val aliases                 = columns.mapConst([Z] => (t: (DbValue[Z], String)) => t._2)
          val aliasesList             = aliases.toListK

          val valueExprsSt = (value +: values).traverse { a =>
            a.traverseK[TagState, Const[SqlExpr[Codec]]]([Z] => (v: DbValue[Z]) => v.ast).map(_.toListK)
          }

          (
            st.withNewQueryNum(queryNum + 1).withNewColumnNum(newColumnNum),
            valueExprsSt.map { valueExprs =>
              QueryAstMetadata(
                SelectAst.Values(valueExprs, Some(queryName), Some(aliasesList)),
                aliases,
                dbValues
              )
            }
          )
        }.flatMap(identity)
    }

    case class SqlQueryOrderedStage[A[_[_]]](
        query: Query[A],
        orderBy: A[DbValue] => OrdSeq
    ) extends SqlQuery[A] {
      export query.{applyK, traverseK}

      override def orderBy(f: A[DbValue] => OrdSeq): Query[A] = copy(orderBy = f).liftSqlQuery

      override def take(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        for
          meta             <- query.selectAstAndValues
          orderByValuesAst <- orderBy(meta.values).ast
        yield meta.ast match
          case from: SelectAst.SelectFrom[Codec] =>
            val oldOrder = from.orderBy
            val newOrder = oldOrder.fold(SelectAst.OrderBy(orderByValuesAst)) { old =>
              old.copy(exprs = old.exprs ++ orderByValuesAst)
            }

            meta.copy(ast = from.copy(orderBy = Some(newOrder)))

          case _ => ???
    }

    case class SqlQueryLimitOffsetStage[A[_[_]]](
        query: Query[A],
        limit: Option[Int] = None,
        offset: Int = 0
    ) extends SqlQuery[A] {
      export query.{applyK, traverseK}

      override def take(i: Int): Query[A] = copy(limit = Some(i)).liftSqlQuery

      override def drop(i: Int): Query[A] = copy(offset = i).liftSqlQuery

      override def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        query.selectAstAndValues.map { meta =>
          meta.ast match
            case from: SelectAst.SelectFrom[Codec] =>
              val oldLimitOffset = from.limitOffset

              val newLimitOffset = oldLimitOffset match {
                case Some(old: SelectAst.LimitOffset) =>
                  Some(old.copy(limit = limit.orElse(old.limit), offset = if offset != 0 then offset else old.offset))

                case None if limit.isDefined || offset != 0 =>
                  Some(SelectAst.LimitOffset(limit, offset, false))

                case None => None
              }

              meta.copy(ast = from.copy(limitOffset = newLimitOffset))
            case _ => ???
        }
    }

    case class SqlQueryFlatMap[A[_[_]], B[_[_]]](query: Query[A], f: A[DbValue] => Query[B])(
        using val applyK: ApplyKC[B],
        val traverseK: TraverseKC[B]
    ) extends SqlQuery[B] {
      override def selectAstAndValues: TagState[QueryAstMetadata[B]] =
        query.selectAstAndValues.flatMap { case QueryAstMetadata(selectAstA, _, valuesA) =>
          val stNewValuesAndAstA: TagState[(Option[SelectAst.From[Codec]], Option[SqlExpr[Codec]], A[DbValue])] =
            if selectAstA match
                case from: SelectAst.SelectFrom[Codec] =>
                  from.distinct.isEmpty && from.groupBy.isEmpty && from.having.isEmpty && from.orderBy.isEmpty && from.limitOffset.isEmpty
                case _ => false
            then
              val selectFrom = selectAstA.asInstanceOf[SelectAst.SelectFrom[Codec]]
              State.pure((selectFrom.from, selectFrom.where, valuesA))
            else SqlValueSource.FromQuery(query).fromPartAndValues.map(t => (Some(t._1), None, t._2))

          def combineOption[C](optA: Option[C], optB: Option[C])(combine: (C, C) => C): Option[C] = (optA, optB) match
            case (Some(a), Some(b)) => Some(combine(a, b))
            case (Some(a), None)    => Some(a)
            case (None, Some(b))    => Some(b)
            case (None, None)       => None

          stNewValuesAndAstA.flatMap { case (fromAOpt, whereExtra, newValuesA) =>
            f(newValuesA).selectAstAndValues.map { case QueryAstMetadata(selectAstB, aliasesB, valuesB) =>
              val newSelectAstB = selectAstB match
                case from: SelectAst.SelectFrom[Codec] =>
                  from.copy(
                    from = combineOption(fromAOpt, from.from)(SelectAst.From.FromMulti.apply),
                    where = combineOption(whereExtra, from.where)((a, b) =>
                      SqlExpr.BinOp(a, b, SqlExpr.BinaryOperation.BoolAnd)
                    )
                  )

                case _ => throw new Exception("Encountered flatmap on non SelectFrom data. Not possible")

              QueryAstMetadata(newSelectAstB, aliasesB, valuesB)
            }
          }
        }
    }

    enum SetOperation(val all: Boolean):
      case Union(override val all: Boolean)     extends SetOperation(all)
      case Intersect(override val all: Boolean) extends SetOperation(all)
      case Except(override val all: Boolean)    extends SetOperation(all)

    case class SqlQuerySetOperations[A[_[_]]](head: Query[A], tail: Seq[(SetOperation, Query[A])]) extends SqlQuery[A] {
      override private[platform] def selectAstAndValues: TagState[QueryAstMetadata[A]] =
        (head.selectAstAndValues, tail.traverse(t => t._2.selectAstAndValues.map(t._1 -> _))).mapN {
          (headMeta, tailMeta) =>
            val mergedAst = tailMeta.foldLeft(headMeta.ast):
              case (acc, (SetOperation.Union(all), meta))     => SelectAst.Union(acc, meta.ast, all)
              case (acc, (SetOperation.Intersect(all), meta)) => SelectAst.Intersect(acc, meta.ast, all)
              case (acc, (SetOperation.Except(all), meta))    => SelectAst.Except(acc, meta.ast, all)

            QueryAstMetadata(mergedAst, headMeta.aliases, headMeta.values)
        }

      override def mapK[B[_[_]]](f: A[DbValue] => B[DbValue])(using FA: ApplyKC[B], FT: TraverseKC[B]): Query[B] =
        if tail.forall(_._1.all) then
          SqlQuerySetOperations(head.mapK(f), tail.map(t => (t._1, t._2.mapK(f)))).liftSqlQuery
        else super.mapK(f)

      override def union(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Union(false), that))).liftSqlQuery

      override def unionAll(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Union(true), that))).liftSqlQuery

      override def intersect(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Intersect(false), that))).liftSqlQuery

      override def intersectAll(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Intersect(true), that))).liftSqlQuery

      override def except(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Except(false), that))).liftSqlQuery

      override def exceptAll(that: Query[A]): Query[A] =
        copy(tail = tail ++ Seq((SetOperation.Except(true), that))).liftSqlQuery

      override def orderBy(f: A[DbValue] => OrdSeq): Query[A] =
        SqlQueryOrderedStage(
          this.liftSqlQuery,
          f
        ).liftSqlQuery

      override def take(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        limit = Some(i)
      ).liftSqlQuery

      override def drop(i: Int): Query[A] = SqlQueryLimitOffsetStage(
        this.liftSqlQuery,
        offset = i
      ).liftSqlQuery

      override def applyK: ApplyKC[A] = head.applyK

      override def traverseK: TraverseKC[A] = head.traverseK
    }
  }

  extension [A[_[_]]](sqlQuery: SqlQuery[A]) def liftSqlQuery: Query[A]

  extension [A[_[_]]](sqlQuery: SqlQueryGrouped[A]) def liftSqlQueryGrouped: QueryGrouped[A]

  extension (q: QueryCompanion)
    @targetName("queryCompanionFrom")
    def from[A[_[_]]](table: Table[A, Codec]): Query[A] =
      import table.given
      SqlQuery.SqlQueryFromStage(SqlValueSource.FromTable(table).liftSqlValueSource).liftSqlQuery
    end from

    def queryCount: DbValue[Long] = SqlDbValue.QueryCount.lift

    def ofK[A[_[_]]: ApplyKC: TraverseKC](value: A[DbValue]): Query[A] =
      SqlQuery.SqlQueryWithoutFrom(value).liftSqlQuery

    inline def of[A](value: A)(using MR: MapRes[DbValue, A]): Query[MR.K] =
      ofK(MR.toK(value))(using MR.applyKC, MR.traverseKC)

    def values[A[_[_]]: ApplyKC: TraverseKC](types: A[Type], value: A[Id], values: Seq[A[Id]] = Nil): Query[A] =
      val liftValues = [Z] => (v: Z, tpe: Type[Z]) => v.as(tpe)

      SqlQuery
        .SqlQueryValues(
          value.map2K(types)(liftValues),
          values.map(_.map2K(types)(liftValues))
        )
        .liftSqlQuery

    def valuesOf[A[_[_]]](table: Table[A, Codec], value: A[Id], values: Seq[A[Id]] = Nil): Query[A] =
      import table.given
      given FunctorKC[A] = table.FA
      Query.values(table.columns.mapK([Z] => (col: Column[Z, Codec]) => col.tpe), value, values)

    def valueOpt[A[_[_]]](types: A[Type], value: A[Option])(
        using FA: ApplyKC[A],
        FT: TraverseKC[A]
    ): Query[[F[_]] =>> A[Compose2[Option, F]]] =
      given optValuesInstance: ApplyKC[[F[_]] =>> A[Compose2[Option, F]]]
        with TraverseKC[[F[_]] =>> A[Compose2[Option, F]]]
        with {
        extension [B[_], D](fa: A[Compose2[Option, B]])
          def map2K[C[_], Z[_]](fb: A[Compose2[Option, C]])(f: [X] => (B[X], C[X]) => Z[X]): A[Compose2[Option, Z]] =
            FA.map2K(fa)(fb)([X] => (v1o: Option[B[X]], v2o: Option[C[X]]) => v1o.zip(v2o).map((v1, v2) => f(v1, v2)))

          def traverseK[G[_]: Applicative, C[_]](f: B :~>: Compose2[G, C]): G[A[Compose2[Option, C]]] =
            FT.traverseK(fa)([Z] => (vo: Option[B[Z]]) => vo.traverse[G, C[Z]](v => f(v)))

          def foldLeftK[C](b: C)(f: C => B :~>#: C): C =
            FT.foldLeftK(fa)(b)(b1 => [Z] => (vo: Option[B[Z]]) => vo.fold(b1)(v => f(b1)(v)))

          def foldRightK[C](b: C)(f: B :~>#: (C => C)): C =
            FT.foldRightK(fa)(b)([Z] => (vo: Option[B[Z]]) => (b1: C) => vo.fold(b1)(v => f(v)(b1)))
      }

      values(types.map2K(value)([X] => (tpe: Type[X], opt: Option[X]) => opt.map(_ => tpe)), value, Nil)

    def valueOfOpt[A[_[_]]](table: Table[A, Codec], value: A[Option]): Query[[F[_]] =>> A[Compose2[Option, F]]] =
      import table.given
      given FunctorKC[A] = table.FA
      valueOpt(table.columns.mapK([Z] => (col: Column[Z, Codec]) => col.tpe), value)

  extension [A](query: Query[[F[_]] =>> F[A]])
    // TODO: Make use of an implicit conversion here?
    @targetName("queryAsMany") def asMany: Many[A] = query.asDbValue.unsafeDbValAsMany

    @targetName("queryAsDbValue") def asDbValue: DbValue[A] = SqlDbValue.SubSelect(query).lift
}
