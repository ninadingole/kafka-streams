package com.iamninad.model

import dbserver1.moviedemo.MOVIE.Movie
import dbserver1.moviedemo.MOVIE_SALES.MovieSales

case class MovieAndSalesBEvent(movie: Movie, movieSales: MovieSales) extends Event
