package com.iamninad.model

import dbserver1.moviedemo.MOVIE.Value
import dbserver1.moviedemo.MOVIE_SALES.MovieSales

case class MovieAndSalesBEvent(movie: Value, movieSales: MovieSales)
