package com.hx.mrs.common

/**
  * Created  on 2019/04/08.
  */
object Constant {

  val MOVIE_DATA_PATH = "D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\movies.csv"
  val RATING_DATA_PATH = "D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\ratings.csv"
  val TAG_DATA_PATH = "D:\\IdeaProjects\\mrs\\recommender\\dataloader\\src\\main\\resources\\aboutmovie\\tags.csv"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  //统计表的名字
  val RATE_MORE_MOVIES="RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES="RateMoreRecentlyMovies"
  val AVERAGE_MOVIES="AverageMovies"
  val GENRES_TOP_MOVIES="GenresTopMovies"

}
