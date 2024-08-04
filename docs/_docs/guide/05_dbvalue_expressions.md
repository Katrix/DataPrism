---
title: DbValue and Expressions
---

# {{page.title}}

It's time to talk about `DbValue`s and expressions.

## Nullability

DataPrism tries to keep good track of nullability of operations. You should never see a value
of `DbValue[Option[Option[A]]]`. If you see such a value, it is a bug. Please report it.

## Prepared statement arguments

At any point, you can convert a value `A` into a `DbValue[A]` by converting it into a prepared statement argument. To do
so, call `.as(type)` on the value. For example `"foo".as(text)`. To create a nullable value, you can either
do `Some("a").as(text.nullable)` or more simply `"a".asNullable(text)`.

## Expressions

Here are some operations that can be done on expressions. This is not a comprehensive list.

### Primitive operators and functions

Here are some of the operators and functions found on all db values:

All DbValues can be compared for equality. This is done using the `===` and `!==` operators.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val a: DbValue[Boolean] = ???
val b: DbValue[Boolean] = ???

val eq = a === b
val neq = a !== b
```

Casting and converting to Some. For casting you need a `CastType`. For most platforms, this is the normal types used
elsewhere in DataPrism. For MySQL platforms, they are special types found at `MySqlJdbcTypes.castType`.

```
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import dataprism.jdbc.sql.PostgresJdbcTypes
val a: DbValue[Boolean] = ???

val asSome = a.asSome //Wrapping in Some
val casted = a.cast(PostgresJdbcTypes.integer)
```

### Booleans

Normal boolean operations work with DbValues

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val a: DbValue[Boolean] = ???
val b: DbValue[Boolean] = ???

val and = a && b
val or = a || b
val not = !a
```

### Numerics

Most dataprism numeric operators are found in the `SqlNumeric` typeclass

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val a: DbValue[Double] = ???
val b: DbValue[Double] = ???

val plus = a + b
val minus = a - b
val times = a * b
val div = a / b
val mod = a % b
val neg = -a
```

### Math functions

DataPrism puts a lot of math functions in the `DbMath` object.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import dataprism.jdbc.sql.PostgresJdbcTypes

val a: DbValue[Double] = ???
val b: DbValue[Double] = ???

val pow = DbMath.pow(a, b)
val sqrt = DbMath.sqrt(a)
val ceil = DbMath.ceil(a)
val floor = DbMath.floor(a)
val log = DbMath.log(a, b)
val sign = DbMath.sign(a)
val pi = DbMath.pi(PostgresJdbcTypes.doublePrecision) //Cast type here
val random = DbMath.random(PostgresJdbcTypes.doublePrecision) //Cast type here

val sin = DbMath.sin(a)
val cos = DbMath.cos(a)
val tan = DbMath.tan(a)
```

### Nullable values
Here are operations that can be used on nullable values.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val a: DbValue[Option[Double]] = ???
val b: DbValue[Option[Double]] = ???
val c: DbValue[Double] = ???

val map = a.map(v => v + c)
val filter = a.filter(v => v > c)
val flatMap = a.flatMap(_ => b)

val isEmpty = a.isEmpty
val isDefined = a.isDefined

val orElse = a.orElse(b)
val getOrElse = a.getOrElse(c)

val mapN = (a, b).mapNullableN((v1, v2) => v1 + v2)
```

### Many
Many is the type used to represent a group of values, usually from groupBy or having.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val v: DbValue[Double] = ???
val m1: Many[Double] = ???
val m2: Many[Double] = ???

val map = m1.map(_ + v)
val count = m2.count

val mapN = (m1, m2).mapManyN((a, b) => a + b)
```

### String operations
Here are some SQL string operations. These are found in the `SqlString` typeclass.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import dataprism.jdbc.sql.PostgresJdbcTypes

val a: DbValue[String] = ???
val b: DbValue[String] = ???
val c: DbValue[String] = ???

val concat = a ++ b
val repeat = a * 5.as(PostgresJdbcTypes.integer)
val length = a.length
val lower = a.toLowerCase
val upper = a.toUpperCase
val like = a.like(b)
val startsWith = a.startsWith(b)
val endsWith = a.endsWith(b)
val replace = a.replace(b, c)

val concat2 = SqlString.concat(a, b)
val concatWs = SqlString.concatWs(" ".as(PostgresJdbcTypes.text), a, b)
```

### In
SQL IN looks like this.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import dataprism.jdbc.sql.PostgresJdbcTypes

val a: DbValue[Boolean] = ???
val b: DbValue[Boolean] = ???
val c: DbValue[Boolean] = ???

val in1 = a.in(b, c)
val in2 = a.notIn(b, c)
val in3 = a.in(Query.of(b))
val in4 = a.notIn(Query.of(b))
val in5 = a.inAs(Seq(true, false), PostgresJdbcTypes.boolean)
val in6 = a.notInAs(Seq(true, false), PostgresJdbcTypes.boolean)
```

### Case
DataPrism supports case both as many if checks, and as a pattern match like operator.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

val v: DbValue[Double] = ???
val w1: DbValue[Double] = ???
val w2: DbValue[Double] = ???

val t1: DbValue[Int] = ???
val t2: DbValue[Int] = ???
val t3: DbValue[Int] = ???

Case(v)
  .when(w1)(t1)
  .when(w2)(t2)
  .otherwise(t3)

Case
  .when(v === w1)(t1)
  .when(v === w2)(t2)
  .otherwise(t3)
```

### Custom SQL

Custom SQL can be created using the functions `DbValue.raw` and `DbValue.rawK`.

### Custom SQL functions

When a custom SQL function is needed, there exists helpers called `DbValue.function` and `DbValue.functionK` to help
create these functions.

