package com.iamninad.filter

import com.lightbend.kafka.scala.streams.KStreamS
import dbserver1.moviedemo.movie.Envelope

class MovieCreatedFilter {

  def filter(stream: KStreamS[String, Envelope]) = {
    stream
      .filter((id, value) => {
        println("filtering sales creation message")
        value.op.equalsIgnoreCase("c")
      })
  }

}
