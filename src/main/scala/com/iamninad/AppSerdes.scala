package com.iamninad

import com.lightbend.kafka.scala.streams.DefaultSerdes
import com.sksamuel.avro4s.RecordFormat
import dbserver1.moviedemo.MOVIE_SALES.{Envelope, MovieSales}
import dbserver1.moviedemo.{MOVIE, MOVIE_SALES}
import org.apache.kafka.streams.Consumed
import DefaultSerdes._
import com.iamninad.model.MovieAndSalesBEvent
import dbserver1.moviedemo.MOVIE.Value
import org.apache.kafka.streams.kstream.{Joined, Serialized}

object AppSerdes {

  object movieSerde {
    implicit val format                                     = RecordFormat[MOVIE.Envelope]
    val movieEnvelopserde                                   = new CaseClassSerde[MOVIE.Envelope](isKey = false)
    implicit val consumed: Consumed[String, MOVIE.Envelope] = Consumed.`with`(stringSerde, movieEnvelopserde)
  }

  object movieSalesSerde {
    implicit val format                                           = RecordFormat[Envelope]
    val serde                                                     = new CaseClassSerde[Envelope](isKey = false)
    implicit val consumed: Consumed[String, MOVIE_SALES.Envelope] = Consumed.`with`(stringSerde, serde)
  }

  object movieBEventSerde {
    implicit val movieFormat = RecordFormat[Value]
    implicit val saleFormat  = RecordFormat[MovieSales]
    implicit val format      = RecordFormat[MovieAndSalesBEvent]

    val movieSerde = new CaseClassSerde[Value](isKey = false)
    val salesSerde = new CaseClassSerde[MovieSales](isKey = false)

    val serde                                           = new CaseClassSerde[MovieAndSalesBEvent](isKey = false)
    implicit val joined: Joined[Int, Value, MovieSales] = Joined.`with`(integerSerde, movieSerde, salesSerde)
    implicit val movieSerialized                        = Serialized.`with`(integerSerde, movieSerde)
    implicit val salesSerialized                        = Serialized.`with`(integerSerde, salesSerde)
  }

}
