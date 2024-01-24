---
title: Usage with Skunk
---

# {{page.title}}

In addition to working with JDBC, DataPrism also includes integrations with Skunk.

## Types

The codec to use with Skunk is Skunk's `skunk.Codec`. Note that `skunk.Codec`s corresponding to more
than one SQL type are disallowed to be used, and something will likely go very wrong. To use these
codecs as types, they need to be wrapped. The extension method `SkunkAnsiTypes.wrap` does this for
you. Some already wrapped types can also be found in `SkunkAnsiTypes`. Alternatively you can find
wrapped variants of most Skunk types in `SkunkTypes`. This object does not contain the ANSI names
for the types, but instead the names Skunk uses for them.

## Platform

The Skunk module comes with its own platform (`dataprism.skunk.platform.PostgresSkunkPlatform`) to
use. In addition to specifying stuff like codec type and implementations for some needed functions,
this platform also expands on query compilation. Using this platform, you can compile an `Operation`
into a `skunk.Command` or `skunk.Query`, and use them like normal Skunk commands or queries.

## Db

The db you use with Skunk is a `dataprism.skunk.sql.SkunkDb[F]`. You need to create an instance
of `skunk.Session[F]` yourself to pass to the constructor of `SkunkDb`


