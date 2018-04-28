package dbserver1.moviedemo.MOVIE

import dbserver1.moviedemo.MOVIE_SALES.MovieSales
import io.debezium.connector.mysql.Source

case class Envelope(before: Option[Value] = None, after: Option[MovieSales] = None, source: Source, op: String, ts_ms: Option[Long] = None)