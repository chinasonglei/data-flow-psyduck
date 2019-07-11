## 大数据推荐系统-可达鸭

### 离线命令
#spark提交命令(线上cdh-s3)：
sudo -u hdfs spark-submit --class com.sxkj.offline.TopRecommender \
--master yarn \
--deploy-mode cluster \
--queue default \
--driver-memory 1g \
--executor-memory 2g \
--num-executors 7 \
--executor-cores 2 \
--files /opt/Apps/psyduck/conf/psyduck.properties \
/opt/Apps/psyduck/libs/data-flow-psyduck-1.0-SNAPSHOT-jar-with-dependencies.jar