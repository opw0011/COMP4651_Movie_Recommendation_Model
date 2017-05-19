
# coding: utf-8

# In[23]:

# Set up AWS S3 access credentials
AWS_BUCKET_NAME = "sam4651-movie-data"


# In[24]:

# For AWS EMR
# Convert csv file to spark data frame
# INPUT: 
# fileName: the full file name(e.g. "file.csv"), 
# fileSchema: the schema (StructType Array with StructField)
# OUTPUT:
# Spark DataFrame
def loadDataFrame(fileName, fileSchema):
    return (spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                    .option("header", "true")
                    .option("mode", "DROPMALFORMED")
                    .schema(fileSchema)
                    .load("s3://%s/%s" % (AWS_BUCKET_NAME, fileName)))


# In[25]:

from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

movieRatingSchema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)])

movieSchema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)])

smallMovieRatingsDF = loadDataFrame("ratings-small.csv", movieRatingSchema).cache()
smallMoviesDF = loadDataFrame("movies-small.csv", movieSchema).cache()


# In[26]:

# Print out the DataFrame shcema, and a few lines as example
smallMovieRatingsDF.printSchema()
smallMovieRatingsDF.show(5)

smallMoviesDF.printSchema()
smallMoviesDF.show(5)


# In[27]:

from pyspark.sql.functions import mean, min, max, stddev

# Data summary of the dataset
print "Number of ratings: %s" % (smallMovieRatingsDF.count())
print "Number of distinct users: %s" % (smallMovieRatingsDF.select('userId').distinct().count())
print "Number of distinct movies: %s" % (smallMovieRatingsDF.select('movieId').distinct().count())
smallMovieRatingsDF.select([mean('rating'), min('rating'), max('rating'), stddev('rating')]).show()
smallMovieRatingsDF.groupBy('rating').count().orderBy('rating').show()


# In[28]:

# Partition the dataset into traning, validation and testing for cross-validation
(trainingSet, validationSet, testingSet) = smallMovieRatingsDF.randomSplit([0.6, 0.2, 0.2], seed=12345)
training = trainingSet.cache()
validation = validationSet.cache()
testing = testingSet.cache()


# In[29]:

# Use ml instead of mlib for Dataframes
# http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

ranks = [2, 4, 8, 12, 16, 20, 24]
regParams = [0.01, 0.05, 0.1, 0.15, 0.2, 0.3]
minError = float('inf')
bestRank = -1
bestRegParam = -1
bestModel = None

# An RMSE evaluator using the rating and predicted rating columns
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
# Initialize the ASL(Alternating Least Squares)
als = ALS(userCol = "userId", itemCol = "movieId", ratingCol = "rating", seed = 123)

for regParam in regParams:
  for rank in ranks:
    # Build the recommendation model using ALS on the training data
    als.setParams(rank = rank, regParam = regParam)
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the validation data
    predictions = model.transform(validation)
    predictions = predictions.dropna() # drop all NaN prediction value to ensure not to have NaN RMSE (due to SPARK-14489)
    error = evaluator.evaluate(predictions)
    
    if error < minError:
      bestRank = rank
      bestRegParam = regParam
      minError = error
      bestModel = model
    print 'For rank %s, regParams %s, the RMSE is %s' % (rank, regParam, error)

print("Best Rank = %s, Best regParam = %s, with RMSE = %s"  % (bestRank, bestRegParam, minError))


# In[34]:

# After getting the best rank and RegParam, test the model on test dataset
predictions = bestModel.transform(testing)
predictions = predictions.dropna() # drop all NaN prediction value to ensure not to have NaN RMSE (due to SPARK-14489)
rmse = evaluator.evaluate(predictions)
print("The model had a RMSE of %s on test dataset"  % (rmse))


# In[35]:

# Train the full data set and calculate the time elapsed
MovieRatingsDF = loadDataFrame("ratings.csv", movieRatingSchema).cache()
MoviesDF = loadDataFrame("movies.csv", movieSchema).cache()


# In[36]:

# Data summary of the full dataset on movie rating
print "Number of ratings: %s" % (MovieRatingsDF.count())
print "Number of distinct users: %s" % (MovieRatingsDF.select('userId').distinct().count())
print "Number of rated distinct movies: %s" % (MovieRatingsDF.select('movieId').distinct().count())
print "Total number of movies: %s" % (MoviesDF.select('movieId').count())

MovieRatingsDF.select([mean('rating'), min('rating'), max('rating'), stddev('rating')]).show()

print "Distribution of ratings:"
MovieRatingsDF.groupBy('rating').count().orderBy('rating').show()
RatingsCountGroupByMovieId = MovieRatingsDF.groupBy('movieId').count()
print "Average number of ratings per movie: %s" % (RatingsCountGroupByMovieId.select(mean('count')).first())


# In[37]:

from time import time

als.setParams(rank = bestRank, regParam = bestRegParam)
print "Training full data set with Rank = %s, regParam = %s ..." % (bestRank, bestRegParam)

timeBegin = time()

model = als.fit(MovieRatingsDF) # use the full dataset for training

timeElapsed = time() - timeBegin

print "Final model trained in %s seconds" % round(timeElapsed, 2)


# In[38]:

# Evaluate the performance of the final model with the testing data
predictions = model.transform(testing)
predictions = predictions.dropna() # drop all NaN prediction value to ensure not to have NaN RMSE (due to SPARK-14489)
rmse = evaluator.evaluate(predictions)
print("The final model had a RMSE of %s"  % (rmse))


# In[39]:

from pyspark.sql.functions import lit
UserId = 1000
userWatchedList = MovieRatingsDF.filter(MovieRatingsDF.userId == UserId).join(MoviesDF, 'movieId').select(['movieId', 'userId', 'title', 'rating'])
watchedMovieList = []
for movie in userWatchedList.collect():
  watchedMovieList.append(movie.movieId)
print "User %s has watched and rated %s moive (sorted by rating):" % (UserId, len(watchedMovieList)) 
userWatchedList.orderBy('rating', ascending = False).show(20, False)

# find out the unwatched list and append with the userid
userUnwatchedList = MoviesDF.filter(MoviesDF.movieId.isin(watchedMovieList) == False).withColumn('userId', lit(UserId)).cache()
print "%s unwatched movie:" % (userUnwatchedList.count())
userUnwatchedList.show(20, False)

predictedMovies = model.transform(userUnwatchedList)
predictedMovies = predictedMovies.dropna().cache() # drop all NaN prediction value to ensure not to have NaN RMSE (due to SPARK-14489)


# In[40]:

print "Top 25 predicted movie with highest rating:"
top25Movies = predictedMovies.orderBy('prediction', ascending = False).show(25, False)

print "Top 25 commedy with highest rating:"
top25Comedy = predictedMovies.filter(predictedMovies.genres.like("%Comedy%")).orderBy('prediction', ascending = False).show(25, False)


# In[41]:

N = 20
MovieWithLessThanNRatings = RatingsCountGroupByMovieId.filter('count <' + str(N))
print "Movies with less than %s rating count: %s" % (N, MovieWithLessThanNRatings.count())

movieToBeExcluded = []
for movie in MovieWithLessThanNRatings.collect():
  movieToBeExcluded.append(movie.movieId)
  
userUnwatchedListWithAtLeastNRatings = userUnwatchedList.filter(userUnwatchedList.movieId.isin(movieToBeExcluded) == False).cache()


# In[42]:

predictedMovies = model.transform(userUnwatchedListWithAtLeastNRatings)
predictedMovies = predictedMovies.dropna().cache() # drop all NaN prediction value to ensure not to have NaN RMSE (due to SPARK-14489)


# In[43]:

print "Top 25 predicted movie with highest rating:"
top25Movies = predictedMovies.orderBy('prediction', ascending = False).show(25, False)

print "Top 25 commedy with highest rating:"
top25Comedy = predictedMovies.filter(predictedMovies.genres.like("%Comedy%")).orderBy('prediction', ascending = False).show(25, False)

print "Top 25 Science Fiction with highest rating:"
top25SciFi = predictedMovies.filter(predictedMovies.genres.like("%Sci-Fi%")).orderBy('prediction', ascending = False).show(25, False)


# In[46]:

# Save the trained model to S3 for AWS EMR
model.save("s3://%s/%s" % (AWS_BUCKET_NAME, "model2"))


# In[47]:

# Load model previous saved model
from pyspark.ml.recommendation import ALSModel
model = ALSModel.load("s3://%s/%s" % (AWS_BUCKET_NAME, "model2"))


# In[ ]:



