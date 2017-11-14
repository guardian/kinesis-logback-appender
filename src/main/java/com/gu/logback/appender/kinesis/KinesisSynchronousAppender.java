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
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;

import ch.qos.logback.core.spi.DeferredProcessingAware;

/**
 * LOGBack Appender implementation to support sending data from java
 * applications directly into a Kinesis stream.
 * 
 * More details are available
 * <a href="https://github.com/guardian/kinesis-logback-appender">here</a>
 */
public class KinesisSynchronousAppender<Event extends DeferredProcessingAware>
    extends BaseKinesisAppender<Event, AmazonKinesisClient> {

  @Override
  protected AmazonKinesisClient createClient(AWSCredentialsProvider credentials, ClientConfiguration configuration,
      ThreadPoolExecutor executor) {
    return new AmazonKinesisClient(credentials, configuration);
  }

  @Override
  protected void validateStreamName(AmazonKinesisClient client, String streamName) {
    DescribeStreamResult describeResult = null;
    try {
      describeResult = getClient().describeStream(streamName);
      String streamStatus = describeResult.getStreamDescription().getStreamStatus();
      if(!StreamStatus.ACTIVE.name().equals(streamStatus) && !StreamStatus.UPDATING.name().equals(streamStatus)) {
        setInitializationFailed(true);
        addError("Stream " + streamName + " is not ready (in active/updating status) for appender: " + name);
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
    getClient().putRecord(new PutRecordRequest().withPartitionKey(UUID.randomUUID().toString())
        .withStreamName(getStreamName()).withData(data));
  }

}
