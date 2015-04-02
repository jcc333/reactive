package com.tempodb.reactive

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable

import com.twitter.conversions.time._
import com.twitter.util.{Await, Future, JavaTimer, Promise}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import rx.lang.scala.{Observable, Producer, Subscriber}
import rx.lang.scala.schedulers._


@RunWith(classOf[JUnitRunner])
class PagingSuite extends FunSuite {
  import Paging2._
  implicit val timer = new JavaTimer

  def toFuture(obs: Observable[Int]): Future[Seq[Int]] = {
    val p = Promise[Seq[Int]]()
    obs.toSeq.subscribeOn(IOScheduler()).subscribe(new Subscriber[Seq[Int]] {
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

  test("iterator") {
    case class IntProducer(s: Subscriber[Int], start: Int, end: Int) extends Producer {
      val requested = new AtomicLong
      val numbers = (start until end).toIterator

      override def request(n: Long): Unit = {
        if(n == Long.MaxValue) {
          numbers.takeWhile(_ => !s.isUnsubscribed).foreach(s.onNext)
        } else {
          val request = requested.getAndAdd(n)
          if(request == 0) {
            do {
              if(s.isUnsubscribed) return;
              if(numbers.hasNext) {
                s.onNext(numbers.next)
              } else {
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

  test("async") {
    case class IntProducer(s: Subscriber[Int], start: Int, end: Int) extends Producer {
      val pageSize = 10000
      val requested = new AtomicLong
      val emitting = new AtomicBoolean
      val requesting = new AtomicBoolean
      val queue = new ConcurrentLinkedQueue[Int]()
      val numbers = Stream.from(start).iterator

      override def request(n: Long): Unit = {
        if(n == Long.MaxValue) {
          numbers.takeWhile(_ => !s.isUnsubscribed).foreach(s.onNext)
        } else {
          val request = requested.getAndAdd(n)
          // try and claim emission if no other threads are doing so
          tick()
        }
      }

      def more() {
        if(requesting.compareAndSet(false, true)) {
          Future.sleep(1.milliseconds) map { _ =>
            for(i <- 0 until pageSize) {
              queue.offer(numbers.next)
            }
            requesting.set(false)
            tick()
          } handle {
            case t: Throwable =>
              if(!s.isUnsubscribed) {
                s.onError(t)
              }
          }
        }
      }

      def tick() {
        if(queue.isEmpty && requested.get > 0) {
          more()
        } else {
          if(!queue.isEmpty && emitting.compareAndSet(false, true)) {
            while(!queue.isEmpty && requested.get() > 0) {
              if(s.isUnsubscribed) {
                emitting.set(false)
                return
              }
              s.onNext(queue.poll)
              requested.decrementAndGet()
            }

            emitting.set(false)

            if(queue.isEmpty && requested.get > 0) {
              more()
            }
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
    //val out = Await.result(toFuture(obs2.drop(10000).take(10)))

    val out = Await.result(toFuture(obs2.zip(obs3).map(_._1).drop(1000000).take(10)))
    val expected = for(i <- 1000000 until 1000010) yield i
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
