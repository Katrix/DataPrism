package dataprism.jdbc.sql

import dataprism.sql.*

import java.sql.{Connection, PreparedStatement, SQLException}
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

class DataSourceDb(ds: DataSource)(using ExecutionContext) extends ConnectionDb[Future]:

  protected def makePrepared[A](
      sql: SqlStr[JdbcCodec]
  )(f: Using.Manager ?=> (PreparedStatement, Connection) => A): Future[A] =
    Future {
      Using.Manager { implicit man =>
        val con = ds.getConnection.acquired
        val ps  = con.prepareStatement(sql.str).acquired

        val batchSizes = sql.args.map(_.batchSize).distinct
        if batchSizes.length != 1 then throw new SQLException(s"Multiple batch sizes: ${batchSizes.mkString(" ")}")
        val batchSize = batchSizes.head

        var batch = 0
        while batch < batchSize do {
          for ((obj, i) <- sql.args.zipWithIndex) {
            obj.tpe.set(ps, i + 1, obj.value(batch), con)
          }
          batch += 1
          if batch < batchSize then ps.addBatch()
        }

        f(ps, con)
      }
    }.flatMap(Future.fromTry)
