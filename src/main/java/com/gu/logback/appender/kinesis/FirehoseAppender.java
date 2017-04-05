/*******************************************************************************
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 ******************************************************************************/

package com.gu.logback.appender.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadPoolExecutor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamStatus;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.gu.logback.appender.kinesis.helpers.FirehoseStatsReporter;

import ch.qos.logback.core.spi.DeferredProcessingAware;

/**
 * LOGBack Appender implementation to support sending data from java
 * applications directly into a Kinesis Firehose stream.
 * 
 * More details are available
 * <a href="https://github.com/guardian/kinesis-logback-appender">here</a>
 * 
 * @since 1.4
 */
public class FirehoseAppender<Event extends DeferredProcessingAware>
    extends BaseKinesisAppender<Event, AmazonKinesisFirehoseAsyncClient> {

  private FirehoseStatsReporter asyncCallHandler = new FirehoseStatsReporter(this);

  @Override
  protected AmazonKinesisFirehoseAsyncClient createClient(AWSCredentialsProvider credentials,
      ClientConfiguration configuration, ThreadPoolExecutor executor) {
    return new AmazonKinesisFirehoseAsyncClient(credentials, configuration, executor);
  }

  @Override
  protected void validateStreamName(AmazonKinesisFirehoseAsyncClient client, String streamName) {
    DescribeDeliveryStreamResult describeResult = null;
    try {
      describeResult = getClient()
          .describeDeliveryStream(new DescribeDeliveryStreamRequest().withDeliveryStreamName(streamName));
      String streamStatus = describeResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
      if(!DeliveryStreamStatus.ACTIVE.name().equals(streamStatus)) {
        setInitializationFailed(true);
        addError("Stream " + streamName + " is not ready (in active status) for appender: " + name);
      }
    }
    catch(ResourceNotFoundException rnfe) {
      setInitializationFailed(true);
      addError("Stream " + streamName + " doesn't exist for appender: " + name, rnfe);
    }
  }

  @Override
  protected void putMessage(String message) throws Exception {
    ByteBuffer data = ByteBuffer.wrap(message.getBytes(getEncoding()));
    getClient().putRecordAsync(new PutRecordRequest().withDeliveryStreamName(getStreamName())
        .withRecord(new Record().withData(data)), asyncCallHandler);
  }

}
