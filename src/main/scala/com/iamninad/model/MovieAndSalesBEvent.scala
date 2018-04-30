package com.iamninad.model

import dbserver1.moviedemo.movie.Movie
import dbserver1.moviedemo.moviesales.MovieSales

case class MovieAndSalesBEvent(movie: Movie, movieSales: MovieSales) extends Event
