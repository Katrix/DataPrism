---
title: Types
---

# {{page.title}}

Types have already been covered a byt through codecs. In DataPrism, types are slightly more than a
codec. It is a codec, and information on it's nullability. One important fact about types is that 
DataPrism will never guess what type to use. It will always ask the user to pass a type in if it 
needs it.

## Codecs

Codecs are the first piece of this. They describe how to read a value from a database result,
and how to set it in a prepared statement. DataPrism does not hardcode the codec type. Normally 
the codec type is going to be `dataprism.jdbc.sql.JdbcCodec`, but it can also be `skunk.Codec`.

### JdbcCodec

`JdbcCodec` is DataPrism's native codec ttpe. Instances for new types can generally be constructed
using `JdbcCodec.simple`or `JdbcCodec.byClass`. `JdbcCodec` is nullable by default. There is no way 
to construct a nullable instance from a not nullable one. Calling `get` (`codec.get`) on a nullable 
codec will convert it to a not nullable codec.

## SelectedType

`SelectedType` takes a codec, and encodes in its type if it is nullable or not. A type as is
generally talked about is a `SelectedType`.

## NullabilityTypeChoice

`NullabilityTypeChoice` gives access to two `SelectedType`s. One not null (accessed through 
`choice.notNull`), and one nullable (accessed through `choice.nullable`). One can construct a 
choice from a not nullable codec, and a function to convert it to a nullable codec 
(`NullabilityTypeChoice.notNullByDefault`) or a nullable codec, and a function to convert it to a 
not nullable codec (`NullabilityTypeChoice.nullableByDefault`). A choice can also be obtained from
a `SelectedType` using the `choice` function. (`tpe.choice`).

A `NullabilityTypeChoice` is also by convention a not nullable `SelectedType`.
