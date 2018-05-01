package com.iamninad.filter

import com.lightbend.kafka.scala.streams.KStreamS
import dbserver1.moviedemo.movie.Envelope

class MovieUpdateFilter {

  def filter(stream: KStreamS[String, Envelope]) = {
    stream
      .filter((id, value) => {
        println("filtering movies for updates")
        value.op.equalsIgnoreCase("u")
      })
  }

}
