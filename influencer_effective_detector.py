import findspark
findspark.init('')

from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
import datetime

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '84g'),
                                      ('spark.executor.cores', '5'),
                                      ('spark.cores.max', '15'),
                                      ('spark.driver.memory','100g'),
                                      ('spark.parallelize','5')])



import numpy
from neo4j import GraphDatabase
import pandas as pd
from sqlalchemy import create_engine
import statistics
from datetime import datetime
from datetime import timedelta
from datetime import date



url = "neo4j://..."
gdb = GraphDatabase.driver(url, auth=("neo4j", "password"))

session = gdb.session()

today = datetime.now()
dater = today - timedelta(days=1)
dt_string = datetime.strftime(today, "%Y-%m-%dT%H:%M:%SZ")
yesterday = datetime.strftime(dater,"%Y-%m-%dT%H:%M:%SZ")

#yesterday = '2021-04-08'

print(dt_string)
print(yesterday)



post=session.run("MATCH(p:Post)-[:ACCOUNT_POST]->(a:Account) "
                 "MATCH (p)-[:DEFINITION_POST]->(d:Definition)"
                 " where a.IsBlackList = False and  "
                 "p.CreatedDate<>'0001-01-01T00:00:00Z' and a.CreatedDate<>'0001-01-01T00:00:00Z' "
                 "and a.AccountType = 'Twitter' and (d.Value='#turkey')  "
                 "and d.DefinitionType='Keyword' "
                 "and p.IsRetweet = False "
                 "and p.CreatedDate >= $yesterday AND p.CreatedDate < $dt_string  "
                 "return a.UserName AS User,a.Country AS Country,a.Location AS Location,a.StatusesCount AS statuses,a.FollowerCount AS follower,a.FollowingCount AS following  , a.FavouritesCount AS favourite, a.CreatedDate AS created_date,a.ListedCount AS ListedCount, a.Verified AS verified,p.AccountId AS AccountId , p.Id AS id,p.ReplyCount AS reply,p.RetweetCount AS Retweeted,p.IsRetweet AS is_retweeted,p.CreatedDate AS created,p.FavoriteCount AS favorite,a.ProfileImageUrl AS Image", yesterday = yesterday,dt_string = dt_string)

posting=[]
for record_3 in post:
    posting.append([record_3["User"],
                    record_3["Country"],
                    record_3["Location"],
                    record_3["statuses"],
                    record_3["follower"],
                    record_3["following"],
                    record_3["favourite"],
                    record_3["created_date"],
                    record_3["ListedCount"],
                    record_3["verified"],
                    record_3["AccountId"],
                    record_3["id"],
                    record_3["reply"],
                    record_3["Retweeted"],
                    record_3["is_retweeted"],
                    record_3["created"],
                    record_3["favorite"],
                    record_3["Image"]
                    ])

if posting:
    post=pd.DataFrame(posting)
    #post=spark.createDataFrame(post)

post['date_filter'] = '1'


post.columns=['User',
              'Country',
              'Location',
              'statuses',
              'follower',
              'following',
              'favourite',
              'created_date',
              'ListedCount',
              'verified',
              'AccountId',
              'id',
              'reply',
              'Retweeted',
              'is_retweeted',
              'created',
              'favorite',
              'Image',
              'date_filter'
              ]


retweet=session.run("MATCH(p:Post)-[:ACCOUNT_POST]->(a:Account)"
                    " MATCH (p)-[:DEFINITION_POST]->(d:Definition) "
                    "where  a.IsBlackList = False and p.CreatedDate<>'0001-01-01T00:00:00Z'"
                    " and a.CreatedDate<>'0001-01-01T00:00:00Z' "
                    "and a.AccountType = 'Twitter'"
                    " and d.DefinitionType='Keyword'"
                    " and (d.Value='#turkey')"
                    "  and p.IsRetweet = True"
                    " and p.CreatedDate >= $yesterday AND p.CreatedDate < $dt_string"
                    "  return a.UserName AS User,a.Country AS Country,a.Location AS Location,a.StatusesCount AS statuses,a.FollowerCount AS follower,a.FollowingCount AS following  , a.FavouritesCount AS favourite, a.CreatedDate AS created_date,a.ListedCount AS ListedCount, a.Verified AS verified, p.AccountId AS AccountId, p.Id AS id,p.ReplyCount AS reply,p.RetweetCount AS Retweeted,p.IsRetweet AS is_retweeted,p.CreatedDate AS created,p.FavoriteCount AS favorite,a.ProfileImageUrl AS Image",yesterday = yesterday, dt_string= dt_string)

retweet_2=[]
for record_7 in retweet:
    retweet_2.append([record_7["User"],
                      record_7["Country"],
                      record_7["Location"],
                      record_7["statuses"],
                      record_7["follower"],
                      record_7["following"],
                      record_7["favourite"],
                      record_7["created_date"],
                      record_7["ListedCount"],
                      record_7["verified"],
                      record_7["AccountId"],
                      record_7["id"],
                      record_7["reply"],
                      record_7["Retweeted"],
                      record_7["is_retweeted"],
                      record_7["created"],
                      record_7["favorite"],
                      record_7["Image"]
                    ])


if retweet_2:
    retweet=pd.DataFrame(retweet_2)


retweet['date_filter'] = '1'


retweet.columns=['User',
              'Country',
              'Location',
              'statuses',
              'follower',
              'following',
              'favourite',
              'created_date',
              'ListedCount',
              'verified',
              'AccountId',
              'id',
              'reply',
              'Retweeted',
              'is_retweeted',
              'created',
              'favorite',
              'Image',
              'date_filter'
              ]

Spark_poster = SQLContext(spark)

post= Spark_poster.createDataFrame(post)
retweet=Spark_poster.createDataFrame(retweet)


account_post =  post.union(retweet)

unretweet = account_post.filter(account_post.is_retweeted == False )

retweet = account_post.filter(account_post.is_retweeted == True )


retweet.createOrReplaceTempView("retweet")

unretweet.createOrReplaceTempView("unretweet")


execute = spark.sql("SELECT first(User) as User,first(Country) as Country,first(Location) as Location,AccountId,first(statuses) as statuses,first(following) as following,first(follower) as follower,first(favourite) as Likes,first(created_date) as created_date,first(is_retweeted) as is_retweet,SUM(reply) as Sum_reply,SUM(Retweeted) as Retweeted,SUM(favorite) as favorites,first(ListedCount) as ListedCount,first(verified) as verified,first(Image) as Image "
                    "FROM unretweet GROUP BY AccountId")

execute_retweeter= spark.sql("SELECT first(User) as User,first(Country) as Country,first(Location) as Location,AccountId,first(statuses) as statuses,first(following) as following,first(follower) as follower,first(favourite) as Likes,first(created_date) as created_date,first(is_retweeted) as is_retweet,SUM(reply) as Sum_reply,COUNT(Retweeted) as Retweeted,SUM(favorite) as favorites,first(ListedCount) as ListedCount,first(verified) as verified,first(Image) as Image"
                             " FROM retweet GROUP BY AccountId")

tweets=execute.union(execute_retweeter)


tweet=tweets.select("AccountId","is_retweet","Retweeted")


personal = tweet.groupBy("AccountId").pivot("is_retweet").sum("Retweeted")

personal = personal.withColumnRenamed("false" , "Retweet_by" ).withColumnRenamed("true" , "Retweeted")



persons=tweets.select("AccountId","User","Country","Location","statuses","following","follower","Likes","created_date","Sum_reply","favorites","ListedCount","verified","Image")



set=personal.join(persons,on='AccountId')

set=set.dropDuplicates()

set=set.fillna(0)


set.createOrReplaceTempView("set")

set = spark.sql("SELECT first(AccountId) as AccountId,first(Retweet_by) as Retweet_by,first(Retweeted) as Retweeted,first(User) as User,first(Country) as Country,first(Location) as Location,first(statuses) as statuses,first(following) as following,first(follower) as follower,first(Likes) as Likes,first(created_date) as created_date,SUM(favorites) as favorites,first(ListedCount) as ListedCount,first(verified) as verified,first(Image) as Image"
                " from set GROUP BY AccountId")

set.show()

retweeted_followers_p = session.run("MATCH(p1:Post)-[:POST_RETWEET]->(p2:Post)"
                                    " MATCH(p1)-[:ACCOUNT_POST]->(a1:Account)"
                                    " MATCH(p2)-[:ACCOUNT_POST]->(a2:Account)"
                                    " MATCH(p2)-[:DEFINITION_POST]->(d:Definition)"
                                    " WHERE a2.IsBlackList = False"
                                    " and p2.CreatedDate<>'0001-01-01T00:00:00Z' and a2.CreatedDate<>'0001-01-01T00:00:00Z'"
                                    " and a2.AccountType = 'Twitter'"
                                    " and p2.IsRetweet = False"
                                    "  and p2.CreatedDate >= $yesterday AND p2.CreatedDate < $dt_string"
                                    "  and d.Value in ['#turkey']"
                                    " RETURN a2.Id AS AccountId,a2.UserName AS User,p2.Id AS Id,a1.UserName,a1.FollowerCount AS Follower_count,a2.FollowerCount AS Follower order by a2.UserName,p2.Id desc", yesterday=yesterday, dt_string= dt_string)

retweeted_followers = []
for record_retweet in retweeted_followers_p:
    retweeted_followers.append([record_retweet["AccountId"],
                                record_retweet["User"],
                                record_retweet["Id"],
                                record_retweet["Follower_count"],
                                record_retweet["Follower"]])

if retweeted_followers:
    retweeted_f = pd.DataFrame(retweeted_followers)

retweeted_f.columns = ['AccountId',
                        'User_name',
                        'Id',
                        'Follower_count',
                        'Follower']

retweeted_f = Spark_poster.createDataFrame(retweeted_f)

retweeted_f.createOrReplaceTempView("retweeted_f")


executer=spark.sql("SELECT first(AccountId) as AccountId,first(User_name) as User_name,first(Id) as Id,sum(Follower_count) AS SUM_follower,first(Follower) as follower "
                   "from retweeted_f group by User_name,Id order by User_name,Id desc")


executer = executer.withColumn('reache',executer['SUM_follower'] + executer['follower'])

executer.createOrReplaceTempView("reach")



execute_ret = spark.sql("SELECT first(AccountId) as AccountId,first(User_name) as User_name,(sum(reache)/count(distinct Id)) AS REACH"
                        " from reach group by User_name")



set = set.join(execute_ret,on='AccountId')



from pyspark.sql.functions import abs

abs_Retweet = set.withColumn('reach',abs((set.follower) - (set.following)))


mean_retweet = set.groupBy().avg('Retweet_by').collect()


today =datetime.today()

system_date= today.strftime("%Y-%m-%d %H:%M:%S")

system_date=pd.to_datetime(system_date)

print(system_date)

print(type(system_date))

select = set

from pyspark.sql import functions as F

select = select.withColumn("system_date",F.current_timestamp())


from pyspark.sql.functions import unix_timestamp, from_unixtime

select_created=select.withColumn("created_date",
                          to_timestamp(col("created_date"),"yyyy-MM-dd'T'HH:mm:ss'Z'"))



select_created = select_created.withColumn("day" , datediff(col("system_date"),col("created_date")))


result = select_created

result = result.withColumn("activity", ((col("Retweet_by") + col("follower")) + (col("following")))/(col("day")))

result = result.fillna(value = 0 , subset=["activity"])


mean_activity = result.groupBy().avg("activity").take(1)[0][0]


result = result.withColumn("mean_activity" , lit(mean_activity))


result = result.withColumn("status" ,
                  expr("case when activity <= mean_activity then 'passive' " +
                                  "when activity > mean_activity then 'active' " +
                                  "else '' end") )


active = result.filter(result.status == 'active')

active = active.dropDuplicates()

interactivity = active.select (  col("AccountId"),
                               (( ( ( ( col("Retweeted")+1) * (col("ListedCount")+1)) + (col("Likes")+1)) * ( ( (col("Likes")+1) * (col("following")+1) / (col("Retweeted")+1) )    ) / (col("following")+1) )).alias("interactivity"))


interactivity = interactivity.dropDuplicates()


effectivity = active.select (  col("AccountId"),
                                 (( ( ( ( col("Retweet_by")+1) * (col("REACH")+1) )  + (col("favorites")+1)) * ( ( (col("favorites")+1) * (col("follower")+1) / (col("Retweet_by")+1) )    ) / (col("follower")+1) )).alias("effectivity"))


effectivity =  effectivity.dropDuplicates()




active = active.join(interactivity , on="AccountId")


active = active.join(effectivity , on="AccountId")


minimum_interactive = active.groupBy().min("interactivity").take(1)[0][0]


active = active.withColumn("minimum_interactive", lit(minimum_interactive))

maximum_interactive = active.groupBy().max("interactivity").take(1)[0][0]

active = active.withColumn("maximum_interactive", lit(maximum_interactive))


minimum_effective = active.groupBy().min("effectivity").take(1)[0][0]

active = active.withColumn("minimum_effectivity" , lit(minimum_effective))


maximum_effective = active.groupBy().max("effectivity").take(1)[0][0]

active = active.withColumn("maximum_effectivity" , lit(maximum_effective))






active = active.withColumn("Range_interactive" ,
                           ((col("interactivity") * (100)) / (col("maximum_interactive") - col("minimum_interactive") ) ) )

active.fillna(value = 0 , subset = ["Range_interactive"])

active = active.withColumn("Range_effective" ,
                           ((col("effectivity") * (100)) / (col("maximum_effectivity") - col("minimum_effectivity") ) ) )

active.fillna(value = 0 , subset = ["Range_effective"])




active = active.withColumn("status",
                           expr("case when Range_effective < Range_interactive then '2' " +
                                "when Range_effective >= Range_interactive then '1' "
                                "else '' end"))


active = active.withColumn("date_filter" , lit(1))

active.toPandas().to_excel('result.xlsx')

active.createOrReplaceTempView("active")

active_result = spark.sql("SELECT first(User) as title,first(Image) as Image,first(AccountId) as accountId,first(status) as activeType,first(date_filter) as dateFilter,first(Country) as countryCode,first(Location) as Location,first(Range_effective) as rangeInteractive,first(Range_interactive) as rangeEffective"
                          " from active GROUP BY AccountId")


active_result = active_result.dropDuplicates()


active_result.toPandas().to_excel('active.xlsx')

active_result = active_result.select("*").toPandas()

import pickle

active_result.to_pickle("active.pkl")



