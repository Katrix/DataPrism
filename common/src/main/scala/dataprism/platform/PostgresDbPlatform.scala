package dataprism.platform

import scala.annotation.targetName

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.sql.*
import perspective.*

enum PostgresBinOp[A, B, R] {
  case Eq[A]()  extends PostgresBinOp[A, A, Boolean]
  case Neq[A]() extends PostgresBinOp[A, A, Boolean]
  case And      extends PostgresBinOp[Boolean, Boolean, Boolean]
  case Or       extends PostgresBinOp[Boolean, Boolean, Boolean]
}

class PostgresDbPlatform extends DbPlatform {

  enum DbValue[A] {
    case DbColumn private[PostgresDbPlatform] (column: Column[A])
    case QueryColumn private[PostgresDbPlatform] (queryName: String, fromName: String)
    case GroupBy private[PostgresDbPlatform] (value: DbValue[A])
    case BinOp[A, B, R](lhs: DbValue[A], rhs: DbValue[B], op: PostgresBinOp[A, B, R]) extends DbValue[R]
    case JoinNullable[A] private[PostgresDbPlatform] (value: DbValue[A])              extends DbValue[Option[A]]
    case Function(name: String, values: Seq[DbValue[_]])
    case ArrayOf(values: Seq[DbValue[A]]) extends DbValue[Seq[A]]
    case AddAlias(value: DbValue[A], alias: String)

    def render: SqlStr = this match
      case DbValue.DbColumn(_)                          => throw new IllegalArgumentException("Value not tagged")
      case DbValue.QueryColumn(queryName, fromName)     => sql"${SqlStr.const(queryName)}.${SqlStr.const(fromName)}"
      case DbValue.GroupBy(value)                       => value.render
      case DbValue.BinOp(lhs, rhs, PostgresBinOp.Eq())  => sql"${lhs.render} = ${rhs.render}"
      case DbValue.BinOp(lhs, rhs, PostgresBinOp.Neq()) => sql"${lhs.render} != ${rhs.render}"
      case DbValue.BinOp(lhs, rhs, PostgresBinOp.And)   => sql"${lhs.render} AND ${rhs.render}"
      case DbValue.BinOp(lhs, rhs, PostgresBinOp.Or)    => sql"${lhs.render} OR ${rhs.render}"
      case DbValue.JoinNullable(value)                  => value.render
      case DbValue.Function(f, values)    => sql"${SqlStr.const(f)}(${values.map(_.render).intercalate(sql", ")})"
      case DbValue.ArrayOf(values)        => sql"ARRAY[${values.map(_.render).intercalate(sql", ")}]"
      case DbValue.AddAlias(value, alias) => sql"${value.render} AS ${SqlStr.const(alias)}"
    end render

    private[PostgresDbPlatform] def hasGroupBy: Boolean = this match
      case DbValue.DbColumn(_)                        => false
      case DbValue.QueryColumn(queryName, columnName) => false
      case DbValue.GroupBy(value)                     => true
      case DbValue.BinOp(lhs, rhs, _)                 => lhs.hasGroupBy || rhs.hasGroupBy
      case DbValue.JoinNullable(value)                => value.hasGroupBy
      case DbValue.Function(_, values)                => values.exists(_.hasGroupBy)
      case DbValue.ArrayOf(values)                    => values.exists(_.hasGroupBy)
      case DbValue.AddAlias(value, _)                 => value.hasGroupBy
    end hasGroupBy
  }

  extension [A](dbVal: DbValue[A])
    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean] =
      DbValue.BinOp(dbVal, that, PostgresBinOp.Eq())
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean] =
      DbValue.BinOp(dbVal, that, PostgresBinOp.Neq())

    def asc: Ord = Ord.Asc(dbVal)

    def desc: Ord = Ord.Desc(dbVal)

    def singletonArray: DbValue[Seq[A]] = DbValue.ArrayOf(Seq(dbVal))

  extension (boolVal: DbValue[Boolean])
    @targetName("and") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      DbValue.BinOp(boolVal, that, PostgresBinOp.And)
    @targetName("or") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      DbValue.BinOp(boolVal, that, PostgresBinOp.Or)

  sealed trait OrdSeq {
    def render: SqlStr
  }

  enum Ord extends OrdSeq {
    case Asc(value: DbValue[_])
    case Desc(value: DbValue[_])

    def render: SqlStr = this match
      case Ord.Asc(value)  => sql"${value.render} ASC"
      case Ord.Desc(value) => sql"${value.render} DESC"
  }

  case class MultiOrdSeq(init: OrdSeq, tail: Ord) extends OrdSeq {
    def render: SqlStr =
      sql"${init.render}, ${tail.render}"
  }

  extension (ordSeq: OrdSeq) def andThen(ord: Ord): OrdSeq = MultiOrdSeq(ordSeq, ord)

  opaque type Grouped[A] = DbValue[A]
  opaque type Many[A]    = DbValue[A]

  extension [A](grouped: Grouped[A])
    def asMany: Many[A]       = grouped
    def groupedBy: DbValue[A] = DbValue.GroupBy(grouped)

  extension [A](many: Many[A]) def arrayAgg: DbValue[Seq[A]] = DbValue.Function[Seq[A]]("array_agg", Seq(many))

  enum ValueSource[A[_[_]]] {
    case FromQuery[QValues[_[_]], QGroupBy[_[_]], QMap[_[_]]](q: BaseQuery[QValues, QGroupBy, QMap])
        extends ValueSource[QMap]
    case FromTable(t: Table[A])
    case FullJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends ValueSource[[F[_]] =>> (A[F], B[F])]
    case LeftJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends ValueSource[[F[_]] =>> (A[Compose2[F, Option]], B[F])]
    case RightJoin[A[_[_]], B[_[_]]](
        lhs: ValueSource[A],
        rhs: ValueSource[B],
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ) extends ValueSource[[F[_]] =>> (A[F], B[Compose2[F, Option]])]

    private def mapOptCompose[F[_[_]], A[_], B[_]](fa: F[Compose2[A, Option]])(f: A ~>: B)(
        using F: FunctorKC[F]
    ): F[Compose2[B, Option]] =
      fa.mapK([Z] => (v: A[Option[Z]]) => f[Option[Z]](v))

    def functorKC: FunctorKC[A] = this match
      case ValueSource.FromQuery(q)     => q.FMA
      case ValueSource.FromTable(table) => table.FA
      case ValueSource.FullJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[F])] {
          extension [X[_], C](fa: (lt[X], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Y]) =
              (fa._1.mapK(f), fa._2.mapK(f))
        }
      case ValueSource.LeftJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given RF: FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[Compose2[F, Option]], rt[F])] {
          extension [X[_], C](fa: (lt[Compose2[X, Option]], rt[X]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Compose2[Y, Option]], rt[Y]) =
              val l: lt[Compose2[X, Option]] = fa._1
              (mapOptCompose(fa._1)(f), fa._2.mapK(f))
        }
      case ValueSource.RightJoin(l: ValueSource[lt], r: ValueSource[rt], _) =>
        given FunctorKC[lt] = l.functorKC

        given FunctorKC[rt] = r.functorKC

        new FunctorKC[[F[_]] =>> (lt[F], rt[Compose2[F, Option]])] {
          extension [X[_], C](fa: (lt[X], rt[Compose2[X, Option]]))
            def mapK[Y[_]](f: X ~>: Y): (lt[Y], rt[Compose2[Y, Option]]) =
              (fa._1.mapK(f), mapOptCompose(fa._2)(f))
        }
    end functorKC

    def fromPartAndValues: TagState[(SqlStr, A[DbValue])] = this match
      case ValueSource.FromQuery(q) =>
        q.selectRenderAndValues.flatMap { case (queryStr, values) =>
          State { case (queryNum, columnNum) =>
            given ApplicativeKC[A] = q.FMA

            val queryName = s"y$queryNum"

            val newValues = values.mapK(
              [A] =>
                (column: DbValue[A]) =>
                  column match
                    case DbValue.AddAlias(_, alias) => DbValue.QueryColumn[A](queryName, alias)
                    case _ => throw new IllegalStateException("Values were not given alias in SELECT")
            )

            ((queryNum + 1, columnNum), (sql"($queryStr) ${SqlStr.const(queryName)}", newValues))
          }
        }
      case ValueSource.FromTable(table) =>
        import table.given

        State { case (tableNum, columnNum) =>
          val queryName = s"${table.tableName}_y$tableNum"
          val values = table.columns.mapK(
            [A] => (column: Column[A]) => DbValue.QueryColumn[A](queryName, column.nameStr)
          )

          ((tableNum + 1, columnNum), (sql"${SqlStr.const(table.tableName)} AS ${SqlStr.const(queryName)}", values))
        }
      case ValueSource.FullJoin(lhs, rhs, on) =>
        lhs.fromPartAndValues.map2(rhs.fromPartAndValues) { case ((lfrom, lvalues), (rfrom, rvalues)) =>
          (sql"$lfrom FULL JOIN $rfrom ON ${on(lvalues, rvalues).render}", (lvalues, rvalues))
        }
      case ValueSource.LeftJoin(lhs: ValueSource[l], rhs, on) =>
        given FunctorKC[l] = lhs.functorKC

        lhs.fromPartAndValues.map2(rhs.fromPartAndValues) { case ((lfrom, lvalues), (rfrom, rvalues)) =>
          val newLValues =
            lvalues.mapK[Compose2[DbValue, Option]]([A] => (value: DbValue[A]) => DbValue.JoinNullable(value))

          (sql"$lfrom LEFT JOIN $rfrom ON ${on(lvalues, rvalues).render}", (newLValues, rvalues))
        }
      case ValueSource.RightJoin(lhs, rhs: ValueSource[r], on) =>
        given FunctorKC[r] = rhs.functorKC

        lhs.fromPartAndValues.map2(rhs.fromPartAndValues) { case ((lfrom, lvalues), (rfrom, rvalues)) =>
          val newRValues =
            rvalues.mapK[Compose2[DbValue, Option]]([A] => (value: DbValue[A]) => DbValue.JoinNullable(value))

          (sql"$lfrom LEFT JOIN $rfrom ON ${on(lvalues, rvalues).render}", (lvalues, newRValues))
        }
    end fromPartAndValues
  }

  object ValueSource {
    def getFromQuery[A[_[_]]](query: Query[A]): ValueSource[A] = query match
      case baseQuery: BaseQuery[v, g, m] =>
        if anyDefined(
            baseQuery.filter,
            baseQuery.groupBy,
            baseQuery.having,
            baseQuery.map,
            baseQuery.orderBy,
            baseQuery.limit,
            baseQuery.offset
          )
        then ValueSource.FromQuery(baseQuery)
        else baseQuery.values.asInstanceOf
  }

  private def anyDefined(options: Option[_]*): Boolean =
    options.exists(_.isDefined)

  sealed trait Query[A[_[_]]] {
    def selectRenderAndValues: TagState[(SqlStr, A[DbValue])]

    def selectRender: SqlStr = selectRenderAndValues.runA((0, 0)).value._1
  }

  object Query

  case class BaseQuery[Values[_[_]], GroupBy[_[_]], Map[_[_]]](
      values: ValueSource[Values],
      filter: Option[Values[DbValue] => DbValue[Boolean]] = None,
      groupBy: Option[Values[Grouped] => GroupBy[DbValue]] = None,
      having: Option[GroupBy[Grouped] => DbValue[Boolean]] = None,
      map: Option[GroupBy[DbValue] => Map[DbValue]] = None,
      orderBy: Option[Map[DbValue] => OrdSeq] = None,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  )(
      using val FVA: ApplicativeKC[Values],
      val FVT: TraverseKC[Values],
      val FGA: ApplicativeKC[GroupBy],
      val FGT: TraverseKC[GroupBy],
      val FMA: ApplicativeKC[Map],
      val FMT: TraverseKC[Map]
  ) extends Query[Map] {

    private def nested: BaseQuery[Map, Map, Map] = BaseQuery(ValueSource.FromQuery(this))

    def groupByValues(values: Values[DbValue]): GroupBy[DbValue] =
      given FunctorKC[Values] = FVA

      groupBy.fold(values.asInstanceOf[GroupBy[DbValue]])(f =>
        f(values.mapK([A] => (value: DbValue[A]) => value: Grouped[A]))
      )

    def mapValues(values: GroupBy[DbValue]): Map[DbValue] =
      map.fold(values.asInstanceOf[Map[DbValue]])(f => f(values))

    def addFilter(f: Map[DbValue] => DbValue[Boolean]): Query[Map] =
      if anyDefined(limit, offset, orderBy, map, groupBy)
      then nested.addFilter(f)
      else copy(filter = Some(f.asInstanceOf[Values[DbValue] => DbValue[Boolean]]))

    def addMap[M2[_[_]]](f: Map[DbValue] => M2[DbValue])(using ApplicativeKC[M2], TraverseKC[M2]): Query[M2] =
      if anyDefined(orderBy, limit, offset)
      then nested.addMap(f)
      else copy(map = Some(f.asInstanceOf[GroupBy[DbValue] => M2[DbValue]]), orderBy = None)

    private def fullJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]] =
      new ApplicativeKC[FullJoin[MA, MB]] with TraverseKC[FullJoin[MA, MB]] {
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
      fullJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[LeftJoin[MA, MB]] with TraverseKC[LeftJoin[MA, MB]]]

    private def rightJoinInstances[MA[_[_]], MB[_[_]]](
        using AA: ApplicativeKC[MA],
        BA: ApplicativeKC[MB],
        AT: TraverseKC[MA],
        BT: TraverseKC[MB]
    ): ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]] =
      fullJoinInstances[MA, MB].asInstanceOf[ApplicativeKC[RightJoin[MA, MB]] with TraverseKC[RightJoin[MA, MB]]]

    def addFullJoin[M2[_[_]]](
        that: Query[M2],
        on: (Map[DbValue], M2[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[Map, M2]] =
      if anyDefined(limit, offset, orderBy, map, having, groupBy, filter)
      then nested.addFullJoin(that, on)
      else
        that match
          case baseQuery: BaseQuery[v, g, m] =>
            import baseQuery.given

            type AppTravFullJoinKC = ApplicativeKC[FullJoin[Values, M2]] with TraverseKC[FullJoin[Values, M2]]

            given AppTravFullJoinKC = fullJoinInstances

            copy[FullJoin[Values, M2], FullJoin[Values, M2], FullJoin[Values, M2]](
              values = ValueSource.FullJoin(
                this.values,
                ValueSource.getFromQuery(that),
                on.asInstanceOf[(Values[DbValue], M2[DbValue]) => DbValue[Boolean]]
              ),
              orderBy = None,
              map = None,
              having = None,
              groupBy = None,
              filter = None
            ).asInstanceOf[Query[FullJoin[Map, M2]]]

    def addLeftJoin[M2[_[_]]](
        that: Query[M2],
        on: (Map[DbValue], M2[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[Map, M2]] =
      if anyDefined(limit, offset, orderBy, map, having, groupBy, filter)
      then nested.addLeftJoin(that, on)
      else
        that match
          case baseQuery: BaseQuery[v, g, m] =>
            import baseQuery.given

            type AppTravLeftJoinKC = ApplicativeKC[LeftJoin[Values, M2]] with TraverseKC[LeftJoin[Values, M2]]

            given AppTravLeftJoinKC = leftJoinInstances

            copy[LeftJoin[Values, M2], LeftJoin[Values, M2], LeftJoin[Values, M2]](
              values = ValueSource.LeftJoin(
                this.values,
                ValueSource.getFromQuery(that),
                on.asInstanceOf[(Values[DbValue], M2[DbValue]) => DbValue[Boolean]]
              ),
              orderBy = None,
              map = None,
              having = None,
              groupBy = None,
              filter = None
            ).asInstanceOf[Query[LeftJoin[Map, M2]]]

    def addRightJoin[M2[_[_]]](
        that: Query[M2],
        on: (Map[DbValue], M2[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[Map, M2]] =
      if anyDefined(limit, offset, orderBy, map, having, groupBy, filter)
      then nested.addRightJoin(that, on)
      else
        that match
          case baseQuery: BaseQuery[v, g, m] =>
            import baseQuery.given

            type AppTravRightJoinKC = ApplicativeKC[RightJoin[Values, M2]] with TraverseKC[RightJoin[Values, M2]]

            given AppTravRightJoinKC = rightJoinInstances

            copy[RightJoin[Values, M2], RightJoin[Values, M2], RightJoin[Values, M2]](
              values = ValueSource.RightJoin(
                this.values,
                ValueSource.getFromQuery(that),
                on.asInstanceOf[(Values[DbValue], M2[DbValue]) => DbValue[Boolean]]
              ),
              orderBy = None,
              map = None,
              having = None,
              groupBy = None,
              filter = None
            ).asInstanceOf[Query[RightJoin[Map, M2]]]

    def addGroupBy[M2[_[_]]](f: Map[Grouped] => M2[DbValue])(using ApplicativeKC[M2], TraverseKC[M2]): Query[M2] =
      if anyDefined(limit, offset, orderBy, map, having)
      then nested.addGroupBy(f)
      else
        copy(groupBy = Some(f.asInstanceOf[Values[Grouped] => M2[DbValue]]), orderBy = None, map = None, having = None)

    def addHaving(f: Map[Grouped] => DbValue[Boolean]): Query[Map] =
      if anyDefined(limit, offset, orderBy, map)
      then nested.addHaving(f)
      else copy(having = Some(f.asInstanceOf[GroupBy[Grouped] => DbValue[Boolean]]))

    def addOrderBy(f: Map[DbValue] => OrdSeq): Query[Map] =
      if anyDefined(limit, offset)
      then nested.addOrderBy(f)
      else copy(orderBy = Some(f))

    def addLimit(n: Int): Query[Map] = copy(limit = Some(n))

    def addOffset(n: Int): Query[Map] = copy(offset = Some(n))

    def selectRenderAndValues: TagState[(SqlStr, Map[DbValue])] =
      def collectGroupBys[A[_[_]]](values: A[DbValue])(using FoldableKC[A]): Seq[DbValue[_]] =
        values.foldMapK([A] => (value: DbValue[A]) => if value.hasGroupBy then List(value) else Nil)

      def renderValues[A[_[_]]](values: A[DbValue], renderAliases: Boolean = false)(using FoldableKC[A]): SqlStr =
        values.foldMapK([A] => (value: DbValue[A]) => List(value.render)).intercalate(sql", ")

      def retagValues[A[_[_]]](values: A[DbValue])(using TraverseKC[A], ApplicativeKC[A]): TagState[A[DbValue]] =
        State { case (tableNum, columnNum) =>
          val columnNumState: State[Int, A[Const[Int]]] =
            values.traverseK(
              [A] => (_: DbValue[A]) => State[Int, Const[Int][A]]((acc: Int) => (acc + 1, acc))
            )

          val (newColumnNum, tags) = columnNumState.run(columnNum).value
          val retagedValues = values.map2K(tags)(
            [A] => (value: DbValue[A], tag: Int) => DbValue.AddAlias[A](value, s"x$tag")
          )

          ((tableNum, newColumnNum), retagedValues)
        }

      values.fromPartAndValues
        .flatMap { case (fromPart, values) =>
          val groupByValuesV: GroupBy[DbValue] = groupByValues(values)
          val selectValues: Map[DbValue]       = mapValues(groupByValuesV)

          retagValues(selectValues).map(retagedValues =>
            (fromPart, values, groupByValuesV, selectValues, retagedValues)
          )
        }
        .map { case (fromPart, values, groupByValues, selectValues, retagedValues) =>
          val wherePart = filter.fold(sql"")(f => sql"WHERE ${f(values).render} ")
          val groupByPart = groupBy.fold(sql"")(f =>
            sql"GROUP BY ${collectGroupBys(groupByValues).map(_.render).intercalate(sql", ")} "
          )
          val havingPart  = having.fold(sql"")(f => sql"HAVING ${f(groupByValues).render} ")
          val selectPart  = sql"SELECT ${renderValues(retagedValues)} "
          val orderByPart = orderBy.fold(sql"")(f => sql"ORDER BY ${f(selectValues).render} ")
          val limitPart   = limit.fold(sql"")(n => sql"LIMIT ${n.as(DbType.int32)} ")
          val offsetPart  = offset.fold(sql"")(n => sql"OFFSET ${n.as(DbType.int32)} ")

          val sqlRes =
            selectPart |+| sql"FROM $fromPart " |+| wherePart |+| groupByPart |+| havingPart |+| orderByPart |+| limitPart |+| offsetPart

          (sqlRes, retagedValues)
        }
    end selectRenderAndValues
  }

  extension [A[_[_]]](query: Query[A])
    def filter(f: A[DbValue] => DbValue[Boolean]): Query[A] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addFilter(f)

    def map[B[_[_]]](f: A[DbValue] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addMap(f)

    def join[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[FullJoin[A, B]] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addFullJoin(that, on)

    def leftJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[LeftJoin[A, B]] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addLeftJoin(that, on)

    def rightJoin[B[_[_]]](that: Query[B])(
        on: (A[DbValue], B[DbValue]) => DbValue[Boolean]
    ): Query[RightJoin[A, B]] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addRightJoin(that, on)

    def groupBy[B[_[_]]](f: A[Grouped] => B[DbValue])(using ApplicativeKC[B], TraverseKC[B]): Query[B] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addGroupBy(f)

    def having(f: A[Grouped] => DbValue[Boolean]): Query[A] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addHaving(f)

    def orderBy(f: A[DbValue] => OrdSeq): Query[A] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addOrderBy(f)

    def limit(n: Int): Query[A] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addLimit(n)

    def offset(n: Int): Query[A] = query match
      case baseQuery: BaseQuery[v, g, m] => baseQuery.addOffset(n)

  override type QueryCompanion = Query.type
  extension (q: QueryCompanion)
    def from[A[_[_]]](table: Table[A])(using TraverseKC[A]): Query[A] =
      import table.given
      BaseQuery[A, A, A](ValueSource.FromTable(table))
    end from

  type TagState[A] = State[(Int, Int), A]

  def select[Res[_[_]]](
      query: Query[Res]
  )(using db: Db, dbTypes: Res[DbType], FA: ApplicativeKC[Res], FT: TraverseKC[Res]) =
    db.runIntoRes(query.selectRenderAndValues.runA((0, 0)).value._1)(
      using dbTypes,
      FA,
      FT
    )
}
