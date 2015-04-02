package com.tempodb.reactive

import com.twitter.util.{Future, Promise}
import rx.lang.scala.{Observable, Observer}


object Paging {
  val size = 3

  def ints(start: Int, limit: Int): Future[Seq[Int]] = {
    val p = Promise[Seq[Int]]()
    val obs = stream(start).take(limit).toSeq
    obs.subscribe(
      onNext = (i: Seq[Int]) => p.setValue(i),
      onError = t => p.setException(t),
      onCompleted = () => ()
    )
    p
  }

  def stream(start: Int): Observable[Int] = paged(start).concat

  private def paged(start: Int): Observable[Observable[Int]] =
    Observable { observer =>
      observer.onNext(singlePage(observer, start))
    }

  private def singlePage(paging: Observer[Observable[Int]], start: Int): Observable[Int] =
    Observable {
      observer => {
        val end = start + size
        val f = Future.Unit
        f.onSuccess(_ => {
          for(i <- start until end) {
            observer.onNext(i)
          }
          observer.onCompleted()
          paging.onNext(singlePage(paging, end))
        })
        f.onFailure(observer.onError(_))
      }
    }
}
