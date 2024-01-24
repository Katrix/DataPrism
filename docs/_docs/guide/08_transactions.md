---
title: Transactions
---

# {{page.title}}

DataPrism has basic support for transactions. To perform a transaction, you need
a `TransactionlDb[F, Codec]`. Most top level `Db`s you construct are transactional. You perform a
transaction by calling `transaction` on the `TransactionalDb`.

`transaction` has this signature:
`def transaction[A](f: TransactionDb[F, Codec] ?=> F[A])(using NotGiven[TransactionDb[F, Codec]]): F[A]`.
It takes as a single argument a block where you execute your logic. Within this block, you have
access to an implicit `TransactionDb`. Because of how Scala 3 prioritizes implicits, this `Db` is
the one that will be used for any operations you perform. The function also takes an implicit
evidence that there is not already a `TransactionDb` in scope, as that would imply a transaction has
already been started.

The `TransactionalDb` will commit the transaction once the block finishes, or roll it back if the
block fails because of an exception.

## TransactionDb

If you want more fine-grained control over the transaction, you can use the functions found
on `TransactionDb` to commit, rollback, create savepoints, and rolling back to these savepoints.
