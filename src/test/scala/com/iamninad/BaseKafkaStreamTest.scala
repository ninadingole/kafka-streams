package com.iamninad

import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.scalatest.{FunSuite, Matchers}

trait BaseKafkaStreamTest extends FunSuite with Matchers with EmbeddedKafkaStreamsAllInOne {}
