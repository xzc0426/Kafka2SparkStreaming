# Kafka2SparkStreaming
## version 1.0
sparkstreaming 对接 Kafka，读取 zookeeper 中的 offset，进行校准，消息处理完毕后将每个 partition 最新 offset 放到 zookeeper 的相应路径。

## 需求：
### 以图搜图：
将上传的图像进行解析，经过数据去重放入 Kafka 队列，redis 则放置布控人员信息，SparkStreaming 将 Kafka 传过来的 JSON 数据与 redis 中的布控数据进行比对，如果确定为该目标则将对象放入 ES
