from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# SparkSession 객체를 하나 생성
# EMR Master Node에서 실행 중이라면
# 이 코드는 EMR 클러스터의 YARN 리소스 매니저와 자동으로 연결됨
spark = SparkSession.builder \
    .appName("MyEMRApp") \
    .getOrCreate()

# 아래 파일은 S3 VPC Endpoint를 통해 읽어오게 됨
# 지금 EMR은 Private subnet에서 실행이 되기에 웹에서 데이터를 읽어올 수 없음
csv_s3_url = "s3a://s3-geospatial/survey_results_public_2024.csv"

df = spark.read.csv(csv_s3_url, inferSchema=True, header=True)
print(df.show())

df_lang = df.select('ResponseId', 'LanguageHaveWorkedWith', 'LanguageWantToWorkWith')
print(df_lang.show())

# LanguageHaveWorkedWith 값을 트림하고 ;를 가지고 나눠서 리스트의 형태로 language_have 필드로 설정
# trim 함수에 이어 split 함수 사용
df_lang_have = df_lang.withColumn(
    "language_have",
    f.split(f.trim(f.col("LanguageHaveWorkedWith")), ";")
)
df_lang_have.printSchema()

# LanguageWantToWorkWith 값을 트림하고 ;를 가지고 나눠서 리스트의 형태로 language_want 필드로 설정
df_lang_have_want = df_lang_have.withColumn(
    "language_want",
    f.split(f.trim(f.col("LanguageWantToWorkWith")), ";")
)
print(df_lang_have_want.show())
df_lang_have_want.printSchema()

# 현재 많이 사용되는 언어들 찾기
df_language_have = df_lang_have_want.select(
    df_lang_have_want.ResponseId,
    f.explode(df_lang_have_want.language_have).alias("language")
)
print(df_language_have.show())
df_language_have.groupby("language").count().show(50)
df_language_have.groupby("language").count().sort(f.desc("count")).show()
df_language_have.groupby("language").count().orderBy('count', ascending=False).show()

df_language50_have = df_language_have.groupby("language")\
    .count() \
    .orderBy('count', ascending=False) \
    .limit(50)

df_language50_have.write.mode("overwrite").saveAsTable("language50_have")
df_language_want = df_lang_have_want.select(
    df_lang_have_want.ResponseId,
    f.explode(df_lang_have_want.language_want).alias("language")
)

df_language_want.groupby("language").count().show(50)
df_language_want.groupby("language").count().sort(f.desc("count")).show()
df_language_want.groupby("language").count().orderBy('count', ascending=False).show()

df_language50_want = df_language_want.groupby("language") \
    .count() \
    .orderBy('count', ascending=False) \
    .limit(50)
df_language50_want.write.mode("overwrite").saveAsTable("language50_want")
keeyong inflearn % cat stackoverflow_languages.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyEMRApp") \
    .getOrCreate()

csv_s3_url = "s3a://s3-geospatial/survey_results_public_2024.csv"

df = spark.read.csv(csv_s3_url, inferSchema=True, header=True)
print(df.show())

df_lang = df.select('ResponseId', 'LanguageHaveWorkedWith', 'LanguageWantToWorkWith')
print(df_lang.show())

# LanguageHaveWorkedWith 값을 트림하고 ;를 가지고 나눠서 리스트의 형태로 language_have 필드로 설정
# trim 함수에 이어 split 함수 사용
df_lang_have = df_lang.withColumn(
    "language_have",
    f.split(f.trim(f.col("LanguageHaveWorkedWith")), ";")
)
df_lang_have.printSchema()

# LanguageWantToWorkWith 값을 트림하고 ;를 가지고 나눠서 리스트의 형태로 language_want 필드로 설정
df_lang_have_want = df_lang_have.withColumn(
    "language_want",
    f.split(f.trim(f.col("LanguageWantToWorkWith")), ";")
)
print(df_lang_have_want.show())
df_lang_have_want.printSchema()

# 현재 많이 사용되는 언어들 찾기
df_language_have = df_lang_have_want.select(
    df_lang_have_want.ResponseId,
    f.explode(df_lang_have_want.language_have).alias("language")
)
print(df_language_have.show())
df_language_have.groupby("language").count().show(50)
df_language_have.groupby("language").count().sort(f.desc("count")).show()
df_language_have.groupby("language").count().orderBy('count', ascending=False).show()

df_language50_have = df_language_have.groupby("language")\
    .count() \
    .orderBy('count', ascending=False) \
    .limit(50)

df_language50_have.write.mode("overwrite").saveAsTable("language50_have")
df_language_want = df_lang_have_want.select(
    df_lang_have_want.ResponseId,
    f.explode(df_lang_have_want.language_want).alias("language")
)

df_language_want.groupby("language").count().show(50)
df_language_want.groupby("language").count().sort(f.desc("count")).show()
df_language_want.groupby("language").count().orderBy('count', ascending=False).show()

df_language50_want = df_language_want.groupby("language") \
    .count() \
    .orderBy('count', ascending=False) \
    .limit(50)
df_language50_want.write.mode("overwrite").saveAsTable("language50_want")
