---
title: MapRes and Exotic data
---

# {{page.title}}

So far we've seen HKD and tuples used in queries and results, but there are far more types you can
use here. In theory, any type that has instances of `ApplyKC` and `TraverseKC` can be used in
queries.

However, Scala's type inference does not work well enough if the destination type does not have the
kind `F[_[_]]`. This excludes types, or other non-simple types. To solve this problem, DataPrism
introduces the type `MapRes[F, R] { type K[A[_]] }`. `MapRes` serves as an evidence that the type R
incorporates the type F on its elements, and that the type `K[F] = R`. For
example `MapRes[DbValue, (DbValue[Boolean], DbValue[Int])]` says that in the
type `(DbValue[Boolean], DbValue[Int])`, the values are wrapped in `DbValue`. It also indicates that
the type `K[F[_]] = (F[Boolean], F[Int])`.

`MapRes` also contains functions to convert to and from `K[F]`, in addition to `ApplyKC`
and `TraverseKC` instances.

Here is a list of all the types `MapRes` will handle. The Aux type of `MapRes` will be used to show
all the types of `MapRes`. Where a type could be anything, a type lambda will be used to show that
the types could be something else.

## `[F[_[_]], G[_]] =>> MapRes.Aux[F[G], G, F]`

MapRes handles higher kinded data, or other types with the kind `F[_[_]]`.

## `[F[_], T <: NonEmptyTuple]` =>> MapRes.Aux[F, T, [F0[_]] =>> Tuple.Map[Tuple.InverseMap[T, F], F0]

MapRes handles non-empty tuples, as long as all the elements are mapped by the required type, for
example `DbValue`.

## `[F[_], A] =>> MapRes.Aux[F, F[A], [F0[_]] =>> F0[A]`

MapRes handles single values of type `F[A]`.

## Tuple recursion

If all the elements of a tuple have instances of `MapRes`, so does the tuple itself

## Lifting Apply and Traverse to MapRes

If the `MapRes` instance `[F[_], V, MRK[_[_]]] =>> MapRes.Aux[F, V, MRK]` exists, and `G[_]` is a
type with instances `Apply[G]` and `Traverse[G]`, then there is also a `MapRes` of
type `[F[_], G[_], V, MRK[_[_]]] =>> MapRes.Aux[F, G[V], [F0[_]] =>> G[MRK[F0]]`

What this practically mean is that `Option[DbValue[Int]]` could for example be a valid type, and
will follow the semantics of the `Apply` and `Traverse` instances for `G`.

Think of this as a sort of metaprogramming for the generated SQL.

Derived instances of `ApplyKC` and `TraverseKC` also supports the same principle.

### Beware Lists

Beware of using `List` in the above manner. `List` will operate in accordance to it's `Apply`
instance. For example, `map2` on two lists of 3 elements each will produce an element of 9 elements,
combining the cross product of the two lists. If you want zipping behavior, use `ZipList` instead.

