package com.iamninad.util

object Utils {

  private val TOPIC_PREFIX = "dbserver1.moviedemo."

  def getTopic(topicName: String): String = {
    TOPIC_PREFIX + topicName
  }

}
