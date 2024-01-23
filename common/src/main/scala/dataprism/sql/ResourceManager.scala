package dataprism.sql

import scala.collection.mutable
import scala.util.{Success, Try, Using}

trait ResourceManager:
  extension [A: Using.Releasable](a: A) def acquire: A

object ResourceManager:
  def proxyForUsingManager(using man: Using.Manager): ResourceManager = new ResourceManager:
    extension [A: Using.Releasable](a: A) override def acquire: A = man(a)

  private case class StoredManaged[A](obj: A, releaseable: Using.Releasable[A])

  class Storing private (managed: mutable.Buffer[StoredManaged[_]]) extends ResourceManager {
    extension [A: Using.Releasable](a: A)
      override def acquire: A = {
        managed += StoredManaged(a, summon[Using.Releasable[A]])
        a
      }

    def addExecution[A](thunk: => A): A = addExecutionTry(thunk).get

    def addExecutionTry[A](thunk: => A): Try[A] = try {
      Success(thunk)
    } catch {
      case e: Throwable =>
        finishWithException(e)
    }

    def finishWithException(throwable: Throwable): Try[Nothing] = Using.Manager { man =>
      managed.foreach(m => man.acquire(m.obj)(m.releaseable))
      throw throwable
    }

    def finish(): Try[Unit] = Using.Manager { man =>
      managed.foreach(m => man.acquire(m.obj)(m.releaseable))
    }
  }
  object Storing {
    def make: Storing = new Storing(mutable.Buffer.empty)
  }
