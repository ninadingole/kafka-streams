package dbserver1.moviedemo.MOVIE

case class Movie(movie_id: Option[Int] = None,
                 title: Option[String] = None,
                 year: Option[String] = None,
                 budget: Option[Int] = None)
