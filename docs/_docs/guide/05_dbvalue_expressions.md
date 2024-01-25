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

TODO

### Booleans

### Numerics

### Nullable values

### Many

### String operations

### In

### Case

### Custom SQL functions

### Custom SQL strings
