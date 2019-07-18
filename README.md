# Kafka2SparkStreaming
## version 1
spark streaming对接Kafka，实现zookeeper读取Kafka的每个partition最新offset，消费完毕后提交offset至zookeeper

## 需求：
以图搜图：
摄像头上传的图像进行解析，经过数据去重放入 Kafka 队列，redis 则放置布控人员信息
SparkStreaming 将 Kafka 传过来的 JSON 数据与 redis 中的数据进行比对，如果确定为该目标则将对象放入ES
