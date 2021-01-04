# SparkKafkaConsumerStreaming
Spark streaming consumer example using cloudera products

This test was on CDH 6.3.3

Create a jar file and run below command:
sudo -su spark spark-submit --master yarn --driver-memory 512m --executor-memory 512m --class com.github.mrodriguez.sparkStreaming.KafkaStreaming ./SparkStreamingKafka.jar brokerHost:brokerPort groupName topicName batch_duration

E.g:
sudo -su spark spark-submit --master yarn --driver-memory 512m --executor-memory 512m --class com.github.mrodriguez.sparkStreaming.KafkaStreaming ./SparkStreamingKafka.jar c489-node2:9092 test_group1 test1 30
