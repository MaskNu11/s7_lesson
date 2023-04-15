
from datetime import datetime, timedelta
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


def input_path(date, day_interval, base_input_path):
    end_date = datetime.strptime(date, '%Y-%m-%d').date()

    rows = [f'{base_input_path}/date={end_date - timedelta(days=interval)}' for interval in range(day_interval)]
    return rows


def tag_tops(date, day_interval, spark):
    base_input_path = '/user/masknu11/data/events'
    dir_paths = input_path(date, day_interval, base_input_path)
    
    df_reaction = (
        spark
        .read
        .option('basePath', base_input_path)
        .parquet(*dir_paths)
        .where("event_type='reaction'")
        .selectExpr(['event.message_id', 'event.reaction_from as user_id', 'event.reaction_type'])
    )
    df_reaction.printSchema()
    
    df_message = (
        spark
        .read
        .option('basePath', base_input_path)
        .parquet(*dir_paths)
        .where("event_type='message'")
        .selectExpr(['event.message_id', "explode(event.tags) as tag"])
    )
    df_message.printSchema()
    
    window = Window.partitionBy('user_id', 'reaction_type').orderBy(F.desc('tag'), F.desc('count_tag'))
    
    start_data = df_reaction.join(df_message, ['message_id'], 'left')
    top_reactions = (
                    start_data
                    .groupBy('user_id', 'reaction_type', 'tag')
                    .agg(F.count('tag').alias('count_tag'))
                    .withColumn('rate', F.row_number().over(window))
                    .where(F.col('rate') <= 3)
                    .groupBy('user_id', 'reaction_type')
                    .pivot('rate')
                    .agg(F.first('tag'))
                               )
    top_reactions.show(10)
    
    top_reactions_like = (top_reactions
                          .where(F.col('reaction_type') == 'like')
                          .drop('reaction_type')
                          .withColumnRenamed('1', 'like_tag_top_1')
                          .withColumnRenamed('2', 'like_tag_top_2')
                          .withColumnRenamed('3', 'like_tag_top_3')
                         )
    
    
    top_reactions_dislike = (top_reactions
                             .where(F.col('reaction_type') == 'dislike')
                             .drop('reaction_type')
                             .withColumnRenamed('1', 'dislike_tag_top_1')
                             .withColumnRenamed('2', 'dislike_tag_top_2')
                             .withColumnRenamed('3', 'dislike_tag_top_3')
                            )
    
    return top_reactions_like.join(top_reactions_dislike, ['user_id'], 'full_outer')
    
def main():
    spark = SparkSession.builder\
            .master('yarn')\
            .appName('tag_top_user')\
            .getOrCreate()

#     date = '2022-06-04'
#     day_interval = 5
    
    tag_tops('2022-04-04', 5, spark).show(10)
    tag_tops('2022-05-04', 5, spark).write.parquet('/user/masknu11/data/tmp/reaction_tag_tops_05_04_5')
    tag_tops('2022-04-04', 5, spark).write.parquet('/user/masknu11/data/tmp/reaction_tag_tops_04_04_5')
    tag_tops('2022-04-04', 1, spark).write.parquet('/user/masknu11/data/tmp/reaction_tag_tops_04_04_1')
    # tag_tops(date, day_interval, spark).show(10)


if __name__ == '__main__':
    main()
