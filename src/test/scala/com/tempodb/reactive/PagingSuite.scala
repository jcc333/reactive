package com.tempodb.reactive

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

import com.twitter.util.{Await, Future, Promise}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import rx.lang.scala.{Observable, Producer, Subscriber}


@RunWith(classOf[JUnitRunner])
class PagingSuite extends FunSuite {
  import Paging2._

  def toFuture(obs: Observable[Int]): Future[Seq[Int]] = {
    val p = Promise[Seq[Int]]()
    obs.toSeq.subscribe(new Subscriber[Seq[Int]] {
      override def onStart() {
        request(1)
      }

      override def onNext(i: Seq[Int]) {
        p.setValue(i)
      }

      override def onError(e: Throwable) {
        p.setException(e)
      }
    })
    p
  }

  test("get zipped ints") {
    case class IntProducer(s: Subscriber[Int], start: Int, end: Int) extends Producer {
      val requested = new AtomicLong
      val numbers = (start until end).toIterator

      override def request(n: Long): Unit = {
        if(n == Long.MaxValue) {
          println("max")
          numbers.takeWhile(_ => !s.isUnsubscribed).foreach(s.onNext)
        } else {
          val request = requested.getAndAdd(n)
          println("n %d".format(n))
          println("blah: %d".format(request))
          if(request == 0) {
            do {
              println("request: %d".format(requested.get))
              if(s.isUnsubscribed) return;
              if(numbers.hasNext) {
                println("producer onNext")
                s.onNext(numbers.next)
              } else {
                println("complete")
                s.onCompleted
              }
            } while(requested.decrementAndGet() > 0)
          }
        }
      }
    }

    def getObs = Observable {
      s: Subscriber[Int] => {
        s.setProducer(new IntProducer(s, 0, 10))
      }
    }

    val obs2 = getObs
    val obs3 = getObs
    val out = Await.result(toFuture(obs2.zip(obs3).map(_._1).take(10)))

    val expected = for(i <- 0 until 10) yield i
    assert(expected === out)
  }

  test("backpressure exception") {
    def getObs = Observable {
      s: Subscriber[Int] => {
        val start = 0
        val end = 1000
        val numbers = start until end
        numbers.foreach(s.onNext)
      }
    }

    val obs2 = getObs
    val obs3 = getObs
    intercept[rx.exceptions.MissingBackpressureException] {
      val out = Await.result(toFuture(obs2.zip(obs3).map(_._1).take(10)))
    }
  }
}
