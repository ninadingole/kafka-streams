package dbserver1.moviedemo.moviesales

import io.debezium.connector.mysql.Source

case class Envelope(before: Option[MovieSales] = None,
                    after: Option[MovieSales] = None,
                    source: Source,
                    op: String,
                    ts_ms: Option[Long] = None)
