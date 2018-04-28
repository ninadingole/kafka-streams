package com.iamninad

import com.iamninad.deserializer.MovieDeserializer
import com.iamninad.serializer.MovieSerializer
import org.apache.kafka.streams.Consumed
import com.lightbend.kafka.scala.streams.DefaultSerdes._
import org.apache.kafka.common.serialization.Serdes

object AppSerdes {

  object movieSerde {
    implicit val movieConsumed = Consumed.`with`(stringSerde, Serdes.serdeFrom(new MovieSerializer(), new MovieDeserializer()))
  }

}
