package com.iamninad

import com.lightbend.kafka.scala.streams.DefaultSerdes
import com.sksamuel.avro4s.RecordFormat
import dbserver1.moviedemo.moviesales.{Envelope, MovieSales}
import dbserver1.moviedemo.{movie, moviesales}
import org.apache.kafka.streams.Consumed
import DefaultSerdes._
import com.iamninad.model.{BusinessEvent, MovieAndSalesBEvent}
import dbserver1.moviedemo.movie.Movie
import org.apache.kafka.streams.kstream.{Joined, Produced, Serialized}

object AppSerdes {

  object movieSerde {
    implicit val format                                     = RecordFormat[movie.Envelope]
    val movieEnvelopserde                                   = new CaseClassSerde[movie.Envelope](isKey = false)
    implicit val consumed: Consumed[String, movie.Envelope] = Consumed.`with`(stringSerde, movieEnvelopserde)
    implicit val produced: Produced[String, movie.Envelope] = Produced.`with`(stringSerde, movieEnvelopserde)

  }

  object movieSalesSerde {
    implicit val format                                          = RecordFormat[Envelope]
    val serde                                                    = new CaseClassSerde[Envelope](isKey = false)
    implicit val consumed: Consumed[String, moviesales.Envelope] = Consumed.`with`(stringSerde, serde)
  }

  object movieBEventSerde {
    implicit val movieFormat = RecordFormat[Movie]
    implicit val saleFormat  = RecordFormat[MovieSales]
    implicit val format      = RecordFormat[MovieAndSalesBEvent]
    implicit val eventFormat = RecordFormat[BusinessEvent]

    val movieBEvent = new CaseClassSerde[MovieAndSalesBEvent](isKey = false)
    val movieSerde  = new CaseClassSerde[Movie](isKey = false)
    val salesSerde  = new CaseClassSerde[MovieSales](isKey = false)
    val eventSerde  = new CaseClassSerde[BusinessEvent](isKey = false)

    implicit val joined: Joined[Int, Movie, MovieSales]       = Joined.`with`(integerSerde, movieSerde, salesSerde)
    implicit val produced: Produced[Int, MovieAndSalesBEvent] = Produced.`with`(integerSerde, movieBEvent)
    implicit val movieSerialized                              = Serialized.`with`(integerSerde, movieSerde)
    implicit val salesSerialized                              = Serialized.`with`(integerSerde, salesSerde)
    implicit val eventProduced: Produced[Int, BusinessEvent]  = Produced.`with`(integerSerde, eventSerde)
    implicit val eventConsumed: Consumed[Int, BusinessEvent]  = Consumed.`with`(integerSerde, eventSerde)
  }

}
