FOLDER=$1
hdfs dfs -rm "/$FOLDER/archive/$(date --date="3 days ago" +"*%Y_%m_%d_*")"