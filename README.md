[![Maven Central](https://img.shields.io/maven-central/v/com.gu/kinesis-logback-appender.svg)](https://mvnrepository.com/artifact/com.gu/kinesis-logback-appender)

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

## Performance and reliability notes

This appender is performant but will block if the Kinesis stream throughput is exceeded. In order to guard against this you might want to consider:
  * ensure you have calculated how many shards you need based on your expected throughput 
  * alerting on write throughput exceeded on the Kinesis stream(s)
  * setting up an autoscaling approach that will automatically scale your shards up and down appropriately [AWS docs](https://aws.amazon.com/about-aws/whats-new/2016/11/amazon-kinesis-streams-scaling-and-shard-limit-monitoring-using-new-apis/)
  * configuring the AWS client to not retry on failure so that log lines are discarded when stream throughput is exceeded rather than backing up and causing a cascading failure
  * wrapping the appender in `AsyncAppender`, which can be configured to automatically drop overflowing messages on blocking

## Testing locally

In order to test this you can simply use `mvn install` (to deploy to your local machine).

## Releasing

Some notes for Guardian employees shipping updates to this.

First of all confirm that your `pom.xml` has a `SNAPSHOT` version in it (e.g. https://github.com/guardian/kinesis-logback-appender/blob/08de9295a41ef99f72fb0d75d7717d61b7c5f4f2/pom.xml#L22).
 
In order to release this to maven you'll need to have a settings file at `~/.m2/settings.xml` containing your sonatype credentials (you can probably find these in `.sbt/0.13/sonatype.sbt` if you've shipped Scala libraries):
```xml
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>username</username>
      <password>password</password>
    </server>
  </servers>
</settings>
```

You'll also need the mvn command installed.  You can do `brew install mvn` at the command line.

Once you've got that you can use `mvn clean deploy` to deploy your snapshot to sonatype. This will only release to the snapshot repo (you can add `resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"` to test resolution of this).

Finally when ready run `mvn release:clean release:prepare` and follow the prompts. Once this has completed you need to do one more step to actually release it on maven central: `mvn release:perform`.
