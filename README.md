# Kafka2SparkStreaming
## version 1.0
sparkstreaming 对接 Kafka，读取 zookeeper 中的 offset，进行校准，消息处理完毕后将每个 partition 最新 offset 放到 zookeeper 的相应路径。

