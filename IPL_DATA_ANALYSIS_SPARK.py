# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,BooleanType,DateType,DecimalType


# COMMAND ----------

#creating session
spark=SparkSession.builder.appName("Ipl_data_analysis").getOrCreate()

# COMMAND ----------

AWS_SECRET_ACCESS_KEY="yoursecretacceskey"
encoded_secret_key = AWS_SECRET_ACCESS_KEY.replace("/", "%2F")
AWS_ACCESS_KEY_ID="youraccesskey"
AWS_BUCKET_NAME="bucketname"
mount_name = "yourmountnameanynewname"


# COMMAND ----------

dbutils.fs.mount(f"s3a://{AWS_ACCESS_KEY_ID}:{encoded_secret_key}@{AWS_BUCKET_NAME}", f"/mnt/{mount_name}")


# COMMAND ----------

# MAGIC  %fs ls /mnt/PKUNAS3BUCKET

# COMMAND ----------


 ball_by_ball_df=spark.read.format("csv").option("header","true").option("inferSchema",True).load("dbfs:/mnt/PKUNAS3BUCKET/Ball_By_Ball.csv")



# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

 ball_by_ball_df=spark.read.format("csv").option("header","true").schema(ball_by_ball_schema).load("dbfs:/mnt/PKUNAS3BUCKET/Ball_By_Ball.csv")


# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])


     


# COMMAND ----------

match_df = spark.read.schema(match_schema).format("csv").option("header","true").schema(match_schema).load("dbfs:/mnt/PKUNAS3BUCKET/Match.csv")

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

player_df = spark.read.schema(player_schema).format("csv").option("header","true").schema(player_schema).load("dbfs:/mnt/PKUNAS3BUCKET/Player.csv")


# COMMAND ----------


player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

player_match_df = spark.read.schema(player_match_schema).format("csv").option("header","true").schema(player_match_schema).load("dbfs:/mnt/PKUNAS3BUCKET/Player_match.csv")


# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])




# COMMAND ----------

team_df = spark.read.schema(team_schema).format("csv").option("header","true").schema(team_schema).load("dbfs:/mnt/PKUNAS3BUCKET/Team.csv")
 

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,avg,row_number

# COMMAND ----------

#Filter to include only valid deliveries
ball_by_ball_df=ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))

#Lazy Evaluation spark will not run transformations until it hit action
#Aggregation

total_avg_runs=ball_by_ball_df.groupBy("match_id","innings_no").agg(sum("runs_scored").alias("total_runs"),
                                                                    avg("runs_scored").alias("average_runs")).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

#window function calculate running total of runs in each match for each over
windowSpecifications=Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df=ball_by_ball_df.withColumn("running_total_runs",sum("runs_scored").over(windowSpecifications))

# COMMAND ----------

#Conditional column:Flag for high impact balls (either a wicket or more than 6 runs including extras)
ball_by_ball_df=ball_by_ball_df.withColumn("high_impact",when((col("runs_scored")+col("extra_runs")>6 )| (col("bowler_wicket")==True),True).otherwise(False))    

# COMMAND ----------

ball_by_ball_df.display()

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth,when


# COMMAND ----------

match_df=match_df.withColumn("year",year("match_date"))
match_df=match_df.withColumn("month",month("match_date"))
match_df=match_df.withColumn("day",dayofmonth("match_date"))

#High margin win , categorise win margins into high medium and low

match_df=match_df.withColumn("win_margin_category",when(col("win_margin")>=100,"High")
                             .when((col("win_margin")>=50)&(col("win_margin")<100),"Medium")
                             .otherwise("Low"))

match_df=match_df.withColumn("toss_match_winner",when((col("toss_winner"))==col("match_winner"),"Yes").otherwise("No"))

match_df.display()

                            

# COMMAND ----------

player_df.display()

# COMMAND ----------

from pyspark.sql.functions import lower,regexp_replace

player_df=player_df.withColumn("player_name",lower(regexp_replace("player_name","[^a-zA-Z0-9]","")))

player_df=player_df.na.fill({"batting_hand":"unknown","bowling_skill":"unknown"})

player_df=player_df.withColumn("batting_style",when(col("batting_hand").contains("left"),"left-handed").otherwise("right-handed"))

player_df.display()

# COMMAND ----------

player_match_df.display()

# COMMAND ----------

from pyspark.sql.functions import current_date,expr

# COMMAND ----------

player_match_df=player_match_df.withColumn("veteran_status",when(col("age_as_on_match")>=35,"veteran").otherwise("non-veteran"))

player_match_df=player_match_df.withColumn("years_since_debut",(year(current_date())-col("season_year")))

player_match_df.display()

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")
match_df.createOrReplaceTempView("match")

# COMMAND ----------

top_scoring_batsmen_per_season=spark.sql("""
select p.player_name,m.season_year,sum(runs_scored) as total_runs
from ball_by_ball b
join match m  on b.match_id=m.match_id
join player_match pm on m.match_id=pm.match_id
and b.striker=pm.player_id
join player p on p.player_id=pm.player_id 
group by p.player_name,m.season_year 
order by m.season_year,total_runs desc""")

# COMMAND ----------

top_scoring_batsmen_per_season.display()

# COMMAND ----------

economical_bowlers_in_powerplay=spark.sql("""
select     p.player_name,avg(b.runs_scored) as avg_runs_per_ball,count(b.bowler_wicket) as total_wickets
from ball_by_ball b
join player_match pm on b.match_id=pm.match_id and b.bowler=pm.player_id
join player p on pm.player_id=p.player_id
where b.over_id<=6
group by p.player_name
having count(*)>120
order by avg_runs_per_ball ,total_wickets desc                            
                             """)

# COMMAND ----------

economical_bowlers_in_powerplay.show(5)

# COMMAND ----------

toss_impact=spark.sql("""
  select m.match_id,m.toss_winner,m.toss_name,m.match_winner,
  case when m.toss_winner=m.match_winner then "Won" else "Lost" end as match_outcome
  from match m
  where m.toss_name is not null
  order by m.match_id                    
                      
                      
                      
                      """)

# COMMAND ----------

toss_impact.show(5)
