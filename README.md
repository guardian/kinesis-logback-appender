# LOGBack Appender for Amazon Kinesis

This is an implementation of the [AWS - Labs log4j appender](https://github.com/awslabs/kinesis-log4j-appender) for LOGBack.

Supports both Kinesis and Kinesis Firehose streams.

## Sample Configuration

```xml
<configuration>
  <appender name="KINESIS" class="com.gu.logback.appender.kinesis.KinesisAppender">
    <bufferSize>1000</bufferSize>
    <threadCount>20</threadCount>
    <endpoint>kinesis.us-east-1.amazonaws.com</endpoint><!-- Specify endpoint OR region -->
    <region>us-east-1</region>
    <roleToAssumeArn>foo</roleToAssumeArn><!-- Optional: ARN of role for cross account access -->
    <maxRetries>3</maxRetries>
    <shutdownTimeout>30</shutdownTimeout>
    <streamName>testStream</streamName>
    <encoding>UTF-8</encoding>
    <layout class="ch.qos.logback.classic.PatternLayout">
      <pattern>%m</pattern>
    </layout>
  </appender>
  <appender name="stdout" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%5p [%t] (%F:%L) - %m%n</pattern>
    </encoder>
  </appender>
  <logger name="KinesisLogger" additivity="false" level="INFO">
    <appender-ref ref="KINESIS"/>
  </logger>
  <root level="INFO">
    <appender-ref ref="stdout"/>
  </root>
</configuration>
```

Use `com.gu.logback.appender.kinesis.KinesisAppender` for Kinesis or `com.gu.logback.appender.kinesis.FirehoseAppender` for Kinesis Firehose.
