package com.tempodb.reactive

import java.util.concurrent.atomic.AtomicLong

import com.twitter.util.{Future, Promise, Return, Throw}
import rx.lang.scala.{Observable, Observer, Producer, Subscriber}

case class IntProducer(s: Subscriber[Int], numbers: Stream[Int]) extends Producer {
    val requested = new AtomicLong

    def produce(i: Long): Unit = i match {
      case Long.MaxValue =>
        // no backpressure requested. spew out data
        println("no back")
        numbers.takeWhile(_ => !s.isUnsubscribed).foreach(s.onNext)
      case n: Long =>
        println("back")
        if(requested.getAndAdd(n) == 0) {
          do {
            if(s.isUnsubscribed) {
              return
            }
            s.onNext(numbers.head)
          } while(requested.decrementAndGet() > 0)
        }
    }
}

object Paging2 {
  val size = 3

  def stream(start: Int): Observable[Int] = Observable { s =>
    println("stream")
    val numbers = Stream.from(start)
    val producer = IntProducer(s, numbers)
    s.setProducer(producer)
  }

  private def paged(start: Int): Observable[Observable[Int]] =
    Observable { observer =>
      observer.onNext(singlePage(observer, start))
    }

  private def singlePage(paging: Observer[Observable[Int]], start: Int): Observable[Int] =
    Observable {
      observer => {
        val f = Future { start until (start + size) }
        f.respond {
          case Return(data) if data.isEmpty =>
            observer.onCompleted()
            paging.onCompleted()
          case Return(data) =>
            data.foreach(i => observer.onNext(i))
            observer.onCompleted()
            paging.onNext(singlePage(paging, start + size))
          case Throw(e) => observer.onError(e)
        }
      }
    }
}
