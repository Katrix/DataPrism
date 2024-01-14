---
title: Types
---

# {{page.title}}

We've already talked a bit about types through codecs. Let's go over them a bit more. One important fact about types is
that DataPrism will never guess what type to use. It will always ask the user to pass a type in if it needs it.

## Codecs

Codecs are the first piece of this all. They describe how to read a value from a database result, and how to set it in a
prepared statement. DataPrism does not hardcode the codec type. Normally codec is going to
be `dataprism.jdbc.sql.JdbcCodec`, but it can also be `skunk.Codec`.

### JdbcCodec

`JdbcCodec` is DataPrism's native codec. Instances for new types can generally be constructed using `JdbcCodec.simple`
or `JdbcCodec.byClass`. `JdbcCodec` is nullable by default. There is no way to construct a not nullable instance from a
nullable one. Calling `get` (`codec.get`) on an nullable codec will convert it to a not nullable codec.

## SelectedType

`SelectedType` takes a codec, and encodes in its type if it is nullable or not. A type as is generally talked about is
a `SelectedType`.

## NullabilityTypeChoice

`NullabilityTypeChoice` gives access to two `SelectedType`s. One not null (accessed through `choice.notNull`), and one
nullable (accessed through `choice.nullable`). One can construct a choice from a not nullable codec, and a function to
convert it to a nullable codec (`NullabilityTypeChoice.notNullByDefault`) or a nullable codec, and a function to convert
it to a not nullable codec (`NullabilityTypeChoice.nullableByDefault`). A choice can also be obtained from
a `SelectedType` using the `choice` function. (`tpe.choice`).

`NullabilityTypeChoice` are also by convention not nullable `SelectedType`s.
