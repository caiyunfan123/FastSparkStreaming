# FastSparkStreaming

快速创建kafka-SparkStreaming业务

现已更新2.0版本，实现如下目标：

1.plan分为两种模式：缓存结果到kafka模式（放弃窗口）、直接进行窗口操作模式（放弃中间结果安全性）

--两者均实现偏移量与消费幂等

2.两种模式均支持无限延伸，可以任意处理变形数据，其中：

​	缓存模式要求最终结果类型为String（因为要把结果缓存到kafka）

​	窗口模式要求在使用plan方法时需要额外指定window操作的参数类型（批处理的最终类型）