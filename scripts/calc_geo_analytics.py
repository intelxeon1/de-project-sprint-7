import datetime
import sys

import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from .const import r,cities_tz,rec_dist,home_depth,Mart_Paths


def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    base_input_path = sys.argv[3]
    geo_path = sys.argv[4]
    base_output_path = sys.argv[5]

    conf = SparkConf().setAppName(f"GeoAnalytics-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    paths = input_paths(dt,depth,base_input_path)
       
    cities = get_cities(f"{geo_path}/geo.csv",sql)   
       
    df = sql.read.parquet(*paths)
    
    all_events = df.selectExpr(['COALESCE(event.user,event.message_from,event.reaction_from) as user' \
                                ,'COALESCE(event.datetime,event.message_ts) as datetime',"lat","lon",'event_type']) \
                                .filter( df['lat'].isNotNull() & df['lon'].isNotNull() ) \
                                .withColumn("date",F.to_date(F.col("datetime"))) \
                                .withColumn("lat_rad",F.radians(F.col('lat'))) \
                                .withColumn("lon_rad",F.radians(F.col('lon'))) \
                                .drop('lat').drop('lon')
    
    window_dist = Window().partitionBy(['user','datetime','event_type']).orderBy(F.asc('dist'))
    all_events_with_city = all_events.crossJoin(cities).withColumn("dist", \
        2* r * F.asin(F.sqrt( \
            F.pow(F.sin((F.col('lat_rad') - F.col('lat_rad_c'))/F.lit(2)),2) + \
            F.cos(F.col('lat_rad'))*F.cos(F.col('lat_rad_c'))* \
            F.pow( (F.sin(F.col('lng_rad_c')-F.col('lon_rad'))) / F.lit(2),2) \
            )) \
        ) \
        .withColumn('row_number',F.row_number().over(window_dist)).filter(F.col('row_number')==F.lit('1')) \
        .select(['user','datetime','event_type','date','city',F.col('id').alias('city_id'),'tz']) # type: ignore #.persist()

    
    def calc_user_mart():
        all_events_agg = all_events_with_city.select(['user','date','city','tz']).distinct()
        
        window_usr_date =  Window().partitionBy(['user']).orderBy(F.asc('date'))
        
        events_with_next = all_events_agg.withColumn("next_city",F.lead('city',1).over(window_usr_date))\
                                            .filter(F.col('next_city').isNull() | (F.col('city') != F.col('next_city')) ) \
                                            .withColumn('next_date',F.lead('date',1).over(window_usr_date))
                                                    
        cities_array = events_with_next.select(['user','city']).orderBy(F.asc('date')).groupBy('user'). \
            agg(F.collect_list('city').alias('travel_array')) \
            .withColumn('travel_count',F.size(F.col('travel_array')))
        
        window_usr_date_desc = Window().partitionBy(['user']).orderBy(F.desc('date'))
        home_cities = events_with_next.withColumn("date_diff",F.datediff(F.col('next_date'),F.col('date'))).filter(F.col('date_diff') >= home_depth). \
            withColumn('row_num',F.row_number().over(window_usr_date_desc)).filter(F.col('row_num')==1).select(['user',F.col('city').alias('home_city')])
            
        
        act_cities = all_events_agg.withColumn('row_num',F.row_number().over(window_usr_date_desc)). \
            filter(F.col('row_num')==1).select('user',F.col('city').alias('act_city'),'tz')
        
        act_ts = all_events_with_city.select('user','datetime').groupBy('user').agg(F.date_trunc('second',F.max('datetime')).alias('max_dt'))  
        
        return act_cities.join(act_ts,how='left',on='user').join(home_cities,how='left',on='user').join(cities_array,how='left',on='user') \
            .withColumn('local_time',F.from_utc_timestamp(F.col("max_dt"),F.col('tz'))) \
            .select([ F.col('user').alias('user_id') ,'act_city' ,'home_city','travel_count','travel_array','local_time']) \
    
    mart_usr = calc_user_mart()
    
    
    def calc_zone_mart():
        zone_df_temp = all_events_with_city.select(['user',F.col('datetime').alias('dt'),'event_type',F.col('city_id').alias('zone_id'),'event_type'])
        window_reg = Window().partitionBy(['user','zone_id']).orderBy(F.asc('dt'))
        window_month = Window().partitionBy('zone_id','month')
        
        registration_df = zone_df_temp.filter(F.col('event_type')== F.lit('message')) \
            .withColumn("row_number",F.row_number().over(window_reg)) \
            .filter(F.col('row_number') == F.lit('1'))
        
        mart_reg = registration_df.withColumn("month",F.concat(F.year(F.col("dt")),F.format_string("%02d",F.month(F.col("dt"))))) \
                .withColumn("week",F.weekofyear(F.col("dt"))) \
                .select(['month','week','zone_id','row_number']) \
                .groupBy(['month','week','zone_id']).agg(F.sum(F.col('row_number')).alias(f'week_user')) \
                .withColumn('month_user',F.sum(F.col(f"week_user")).over(window_month))
        
        return zone_df_temp.withColumn("month",F.concat(F.year(F.col("dt")),F.format_string("%02d",F.month(F.col("dt"))))) \
            .withColumn("week",F.weekofyear(F.col("dt"))) \
            .select(['event_type','month','week','zone_id']) \
            .groupBy(['month','week','zone_id']) \
            .agg(F.sum(F.expr("CASE WHEN event_type = 'message' then 1 else 0 END")).alias('week_message'),\
                 F.sum(F.expr("CASE WHEN event_type = 'subscription' then 1 else 0 END")).alias('week_subscription'), \
                 F.sum(F.expr("CASE WHEN event_type = 'reaction' then 1 else 0 END")).alias('week_reaction')) \
            .withColumn('month_message',F.sum(F.col("week_message")).over(window_month)) \
            .withColumn('month_subscription',F.sum(F.col("week_subscription")).over(window_month)) \
            .withColumn('month_reaction',F.sum(F.col("week_reaction")).over(window_month)) \
            .join(mart_reg,on = ['month','week','zone_id'],how='left',) \
            .select(['month','week','zone_id','week_message','week_reaction','week_subscription','week_user','month_message','month_reaction','month_subscription','month_user'])                                                
        
    mart_zone = calc_zone_mart()
    
    def calc_rec_mar():
        users_writed = df.selectExpr(['event.message_from','event.message_to']) \
            .withColumn('user1',F.least('message_from','message_to')) \
            .withColumn('user2',F.greatest('message_from','message_to')) \
            .select(['user1','user2']).distinct()
        
        subscripted_usrs = df.select(([F.col('event.subscription_channel').alias('channel'),'event.user'])).filter( (F.col('event_type') == F.lit('subscription') ) & \
            F.col('event.user').isNotNull() & \
            F.col('event.subscription_channel').isNotNull()).distinct()
        
        users_pairs = subscripted_usrs.select(['channel',F.col('user').alias('f_user')]) \
            .join(subscripted_usrs.select(['channel',F.col('user').alias('s_user')]), how='inner',on='channel') \
            .filter(F.col('f_user') != F.col('s_user')) \
            .withColumn('user1',F.least('f_user','s_user')) \
            .withColumn('user2',F.greatest('f_user','s_user')) \
            .select(['user1','user2']).distinct() \
            .alias("tdf").join(users_writed,how='leftanti',on= ( F.col('tdf.user1')== users_writed['user1'] ) & ( F.col('tdf.user1') == users_writed['user2']) )
        usr_last_pos = all_events.select('user','lat_rad','lon_rad','datetime').distinct() \
            .withColumn('rn',F.row_number().over(Window().partitionBy('user').orderBy(F.desc('datetime')))) \
            .filter(F.col('rn')==F.lit('1')).select(['user','lat_rad','lon_rad'])
        
        return users_pairs.join(usr_last_pos,how='inner',on=F.col('user1') == usr_last_pos['user']) \
            .withColumnRenamed('lat_rad','lat_rad1').withColumnRenamed('lon_rad','lon_rad1').drop('user') \
            .join(usr_last_pos,how='inner',on=F.col('user2') == usr_last_pos['user']) \
            .withColumnRenamed('lat_rad','lat_rad2').withColumnRenamed('lon_rad','lon_rad2').drop('user') \
            .withColumn('distance', \
            2* r * F.asin(F.sqrt( \
                F.pow(F.sin((F.col('lat_rad1') - F.col('lat_rad2'))/F.lit(2)),2) + \
                F.cos(F.col('lat_rad1'))*F.cos(F.col('lat_rad2'))* \
                F.pow( (F.sin(F.col('lon_rad1')-F.col('lon_rad2'))) / F.lit(2),2) \
                )) \
            ).filter(F.col('distance') < rec_dist) \
            .join(mart_usr,how='left',on=F.col('user1') == mart_usr['user_id']) \
            .join(cities,how='left',on=F.col('act_city')==cities['city'] ) \
            .withColumn('processed_dttm',F.date_trunc('second',F.current_timestamp())) \
            .selectExpr(['user1 as user_left','user2 as user_right','processed_dttm','id as zone_id','local_time'])
        
    mart_rec = calc_rec_mar()
    
    mart_usr.write.parquet(path=f"{base_output_path}/{Mart_Paths.usr}-{dt}-{depth}",mode='overwrite')
    mart_rec.write.parquet(path=f"{base_output_path}/{Mart_Paths.rec}-{dt}-{depth}",mode='overwrite')
    mart_zone.write.parquet(path=f"{base_output_path}/{Mart_Paths.zone}-{dt}-{depth}",mode='overwrite')
        


def input_paths(date, depth,base_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [
        f"{base_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/*.parquet"
        for x in range(depth)
    ]

def get_cities(geo_path,sql:SQLContext):
    cities = sql.read.options(delimiter=';',header=True).csv("./geo.csv") \
                    .withColumn("lat_rad_c",F.radians(F.regexp_replace(F.col('lat'),',','.' ))) \
                    .withColumn("lng_rad_c",F.radians(F.regexp_replace(F.col('lng'),',','.' ))) \
                    .drop('lat').drop('lng')
    
    tz = sql.createDataFrame([cities_tz],schema=['id','tz'])

    return cities.join(tz,how='left',on='id')

if __name__ == "__main__":
    main()
