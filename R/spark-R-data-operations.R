
############# Notebook 1 ######################
# CREATING SPARK DATA FRAME
# -------------------------

# 1. Using local data frame

# library(SparkR)
# df <- createDataFrame(faithful)
# head(df)

# 2. Using data source API

#library(SparkR)
#diamondsDF <- read.df("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", source = "csv", header="true", inferSchema = "true")
#head(diamondsDF)
#printSchema(diamondsDF)
#display(diamondsDF)

#3. Using connector for specific data frame (ex- avro)

#require(SparkR)
#irisDF <- createDataFrame(iris)
### Write data frame in avro format
#write.df(irisDF, source = "com.databricks.spark.avro", path = "dbfs:/tmp/iris.avro", mode = "overwrite")
#### Read data in avro format
#irisDF2 <- read.df(path = "/tmp/iris.avro", source = "com.databricks.spark.avro")
#head(irisDF2)
### Write data in avro format
#write.df(irisDF2, path="dbfs:/tmp/iris.parquet", source="parquet", mode="overwrite")

#4. Using spark sql query

### Register earlier df as temp table
### printSchema(irisDF)
#registerTempTable(irisDF, "irisTemp")

### Create a df consisting of only the 'age' column using a Spark SQL query
#lengthDF <- sql("SELECT Sepal_Length,Petal_Length FROM irisTemp")
#head(lengthDF)
#str(lengthDF)

# OPERATIONS on SPARK DATA FRAME

#require(SparkR)
#faithDF <- createDataFrame(faithful)
#1. Select Operation
# head(select(faithDF,faithDF$eruptions))
#head(select(faithDF,"eruptions"))

#2. Filter operation
#head(filter(faithDF,faithDF$waiting < 50))

#3. Grouping operation
#head(count(groupBy(faithDF,faithDF$waiting)))

#4. Grouping and sorting
### Let us find most waiting common waiting time
#waitCountDF <- count(groupBy(faithDF, faithDF$waiting))
#head(arrange(waitCountDF,desc(waitCountDF$count)))

#5. Column operations
# Convert waiting time from hours to seconds.
# Note that we can assign this to a new column in the same DataFrame
#df$waiting_secs <- df$waiting * 60
#head(df)

## MACHINE LEARNING
#------------------
irisDf<- createDataFrame(iris)
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")
summary(model)




############ Notebook  2 #################
library(SparkR)
flightsCsvPath <- "http://s3-us-west-2.amazonaws.com/sparkr-data/flights.csv"
flights_df <- read.csv(flightsCsvPath, header = TRUE)

flightDF <- createDataFrame(flights_df)
#flightDF <- read.df(flightsCsvPath,source="csv",header="true")
#printSchema(flightDF)

cache(flightDF)
#showDF(flightDF,numRows=6)
#head(flightDF)
columns(flightDF)
count(flightDF)

destDF <- select(flightDF,flightDF$dest,flightDF$cancelled)
head(destDF)

# Use collect to create a local R data frame
#local_df <- collect(destDF)

createOrReplaceTempView(destDF,"destTmpView")
dstDF <- sql("select dest,cancelled from destTmpView")
head(dstDF)

count(filter(flightDF,flightDF$dest == "JFK"))

############## Notebook 3 ##################
library(SparkR)
df <- createDataFrame(faithful)
head(df)
str(df)

#head(max(groupBy(df,df$eruptions)))
str(mtcars)
head(mtcars)
mtcars[is.na(mtcars$cyl)]

mtcars[mtcars[is.na(mtcars$cyl)]] <- 0

#mtCarNas <- is.na(mtcars)
#head(mtCarNas,nunRows=32)
#mtcars[mtCarNas] <- 0

dfMtcars <- createDataFrame(mtcars)
head(dfMtcars)

head(agg(cube(dfMtcars, "cyl", "disp", "gear"), avg(dfMtcars$mpg)))

#head(n(df$waiting))

faithfulDF <- createDataFrame(faithful)

# dapply
schema1 <- structType(structField("eruptions", "double"), structField("waiting", "double"),structField("waiting_secs", "double"))
faithFulSecDF <- dapply(faithfulDF, function(df) {df<-cbind(df,df$waiting * 60)}, schema1)
head(faithFulSecDF)

# dapply collect
ldf <- dapplyCollect(faithfulDF,function(df) {df<-cbind(df, "waiting_secs" = df$waiting * 60)})
head(ldf,5)

# gapply
#Determine six waiting times with the largest eruption time in minutes.
schema2 <- structType(structField("waiting", "double"), structField("max_eruption", "double"))
result<- gapply(faithfulDF,"waiting", function(key,df) {data.frame(key,max(df$eruptions))},schema2)

head(collect(arrange(result,"max_eruption",decreasing=TRUE)))

################ Notebook 4 ##############