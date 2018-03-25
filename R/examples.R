# 예제 실행 : RStudio 사용시 스파크 홈과 스파크세션 생성 후 원하는 코드 라인을 선택하여 실행
#             R 또는 SparkR 셸 사용시 스파크 홈과 스파크세션 생성 후 원하는 코드를 붙여넣기 하여 실행

# set SPARK_HOME
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/Users/beginspark/Apps/spark")
}

# create SparkSession
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "1g"))

# Ex 9-1
df1 <- data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3))
head(df1)
df2 <- as.DataFrame(df1)
showDF(df2)

# Ex 9-2
showDF(df1) # Error!!

# Ex 9-3
class(mtcars)
head(mtcars)
df <- createDataFrame(mtcars)
showDF(df, 5, FALSE)

# Ex 9-4
file_path <- "file:///Users/beginspark/Apps/spark/examples/src/main/resources/users.parquet"
df <- read.df(path = file_path, source="parquet")
showDF(df)
df <- read.parquet(file_path)
showDF(df)

# Ex 9-5
rdf <- data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3))
df <- as.DataFrame(rdf)
head(df)

# Ex 9-6
rdf <- data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3))
df <- as.DataFrame(rdf)
head(df)
showDF(df)

# Ex 9-7
df1 <- as.DataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3), c3=c(4, 5, 6)))
head(select(df1, "c1", "c2"))
head(select(df1, "*"))

# Ex 9-8
df1 <- as.DataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3), c3=c(4, 5, 6)))
head(selectExpr(df1, "(c2 * c3) as c5"))

# Ex 9-9
rdf <- data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3))
df <- as.DataFrame(rdf)
first(df)

# Ex 9-10
df1 <- as.DataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3), c3=c(4, 5, 6)))
head(filter(df1, df1$c2 > 2))

# Ex 9-11
df1 <- as.DataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3), c3=c(4, 5, 6)))
showDF(df1)
rst <- subset(df1, df1$c2 > 2, c("c1", "c2"))
showDF(rst)

# Ex 9-12
file_path = "file:///Users/beginspark/Apps/spark/examples/src/main/resources/users.parquet"
df <- read.parquet(file_path)
head(collect(df))

# Ex 9-13
df <- createDataFrame(data.frame(c1=c(1, 8, 7, 6 ,0, 5)))
showDF(df)
showDF(arrange(df, df$c1))

# Ex 9-14
df <- createDataFrame(mtcars)
printSchema(df)
columns(df)
colnames(df)

# Ex 9-15
rdf <- data.frame(c1=c("a", "b", "c", "a", "a", "b"), v=c(1, 1, 1, 1, 1, 1))
df <- createDataFrame(rdf)
showDF(df)
result <- agg(df, v = "sum")
showDF(result)
result <- agg(df, new_col = sum(df$v))
showDF(result)

# Ex 9-16
c1 = c("a", "b", "c", "a", "a", "b")
c2 = c("P1", "P2", "P1", "P1", "P2", "P2")
c3 = c(10, 5, 5, 10, 10, 0)
c4 = c(10, 20, 90, 50, 0, 100)
df <- createDataFrame(data.frame(c1, c2, c3, c4))
showDF(df)
g <- groupBy(df, "c1", "c2")
rst <- agg(g, c3="sum", c4="max")
showDF(rst)
rst = sum(g)
showDF(rst)

# Ex 9-17
c1 = c("a", "b", "c", "a", "a", "b")
c2 = c("P1", "P2", "P1", "P1", "P2", "P2")
c3 = c(10, 5, 5, 10, 10, 0)
c4 = c(10, 20, 90, 50, 0, 100)
df <- createDataFrame(data.frame(c1, c2, c3, c4))
showDF(df)

g <- groupBy(df, "c1")
showDF(avg(g))
showDF(min(g))

# Ex 9-18
c1 = c("a", "b", "c", "a", "a", "b")
c2 = c("P1", "P2", "P1", "P1", "P2", "P2")
c3 = c(10, 5, 5, 10, 10, 0)
c4 = c(10, 20, 90, 50, 0, 100)
df <- createDataFrame(data.frame(c1, c2, c3, c4))
showDF(df)
showDF(select(df, count(df$c3)))
showDF(select(df, countDistinct(df$c1)))

# Ex 9-19
c1 = c("a", "b", "c", "a", "a", "b")
c2 = c("P1", "P2", "P1", "P1", "P2", "P2")
c3 = c(10, 5, 5, 10, 10, 0)
c4 = c(10, 20, 90, 50, 0, 100)
df <- createDataFrame(data.frame(c1, c2, c3, c4))
showDF(select(df, var(df$c3)))
showDF(select(df, skewness(df$c3)))

# Ex 9-20
df <- as.DataFrame(data.frame(c1=c("abcdef", "12345")))
head(df)
df$c2 <- substr(df$c1, 1, 2)
head(df)

# Ex 9-21
df <- as.DataFrame(data.frame(c1=c("abcdef", "12345")))
df$c2 <- substr(df$c1, 1, 2)
df <- select(df, alias(sum(df$c2), "sumOfC2"))
showDF(df)

# Ex 9-22
df <- createDataFrame(data.frame(c1=c(1, 8, 7, 6 ,0, 5)))
col_if <- ifelse(df$c1 > 5.0, "gt", "lt")
col_when_other <- otherwise(when(df$c1 > 5.0, "gt"), "lt")
showDF(select(df, df$c1, alias(col_if, "ifelse"), alias(col_when_other, "when-other")))

# Ex 9-23
df1 <- createDataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 3, 5)))
df2 <- createDataFrame(data.frame(c1=c("a", "b", "c"), c2=c(2, 4, 5)))
showDF(union(df1, df2))
showDF(intersect(df1, df2))
showDF(except(df1, df2))

# Ex 9-24
df1 <- createDataFrame(data.frame(c1=c("a", "b", "c"), c2=c(1, 2, 3)))
df2 <- createDataFrame(data.frame(c1=c("a", "b", "d", "e"), c2=c(1, 2, 3, 4)))
showDF( join(df1, df2, df1$c1 == df2$c1) )
showDF( join(df1, df2, df1$c1 == df2$c1, "left_outer") )
showDF( join(df1, df2, df1$c1 == df2$c1, "fullouter") )

# Ex 9-25
c1 = c("a", "b", "c", "a", "a", "b")
c2 = c(1, 2, 3, 1, 3, 1)
c3 = c(4, 5, 6, 1, 2, 3)
df <- createDataFrame(data.frame(c1, c2, c3))
head(df)
schema <- structType(structField("n1", "double"), structField("n2", "double"))
df1 <- dapply(df, function(x) { y <- cbind(x$c2, x$c2 * 2) }, schema)
head(df1)

# Ex 9-26
df <- as.DataFrame(data.frame(c1=c("a", "b", "c", "a", "a", "b"), c2=c(1, 2, 3, 1, 3, 1),
                              c3=c(4, 5, 6, 1, 2, 3)))
head(df)
schema <- structType(structField("c1", "string"), structField("sumOfC2", "double"))
result <- gapply(df, "c1", function(key, x) { y <- cbind(key, sum(x$c3))}, schema)
head(result)

# Ex 9-27
x1 <- list(k1 = c(1, 3, 5), k2 = c(2, 4, 8))
x2 <- spark.lapply(x1, function(x) { x * 2 })
head(x2)

# Ex 9-28
df <- as.DataFrame(data.frame(c1=c("a", "b", "c", "a", "a", "b"), c2=c(1, 2, 3, 1, 3, 1),
                              c3=c(4, 5, 6, 1, 2, 3)))
createOrReplaceTempView(df, "tempView")
result <- sql("select c1, c2 from tempView where c2 > 2.0")
head(result)

# Table
sparkR.session(enableHiveSupport = TRUE)
c1 = c("note", "bag", "note")
c2 = c(1500, 35000, 10000)
df <- createDataFrame(data.frame(c1, c2))
showDF(df)
saveAsTable(df, "prod", mode = "overwrite")
results <- sql("FROM prod SELECT c1, c2")
showDF(results)

# Ex 9-29 (아래 문서 참조)
# https://spark.apache.org/docs/latest/sparkr.html
# https://spark.apache.org/docs/latest/api/R/index.html(glm), 
df <- createDataFrame(iris)
printSchema(df)
dflist = randomSplit(df, c(0.7, 0.3))
training <- dflist[[1]]
test <- dflist[[2]]
count(training)
count(test)
model <- spark.glm(training, Petal_Width ~ Sepal_Width, family = "gaussian")
summary(model)
fitted <- predict(model, test)
showDF(fitted, 5, FALSE)