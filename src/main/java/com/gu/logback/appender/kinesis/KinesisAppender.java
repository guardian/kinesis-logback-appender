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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import com.gu.logback.appender.kinesis.helpers.KinesisStatsReporter;

import ch.qos.logback.core.spi.DeferredProcessingAware;

/**
 * LOGBack Appender implementation to support sending data from java
 * applications directly into a Kinesis stream.
 * 
 * More details are available
 * <a href="https://github.com/guardian/kinesis-logback-appender">here</a>
 */
public class KinesisAppender<Event extends DeferredProcessingAware>
    extends BaseKinesisAppender<Event, KinesisAsyncClient> {

  private KinesisStatsReporter asyncCallHandler = new KinesisStatsReporter(this);

  @Override
  protected KinesisAsyncClient createClient(AwsCredentialsProvider credentials, ClientOverrideConfiguration configuration,
      ThreadPoolExecutor executor, Region region, Optional<URI> endpointOverride) {
    KinesisAsyncClientBuilder builder = KinesisAsyncClient.builder()
      .credentialsProvider(credentials)
      .region(region)
      .overrideConfiguration(configuration)
      .asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor));
    
    endpointOverride.ifPresent(endpoint -> builder.endpointOverride(endpoint));
    
    return builder.build();
  }

  @Override
  protected void validateStreamName(KinesisAsyncClient client, String streamName) {
    DescribeStreamResponse describeResult;
    try {
      describeResult = getClient().describeStream(b -> b.streamName(streamName).build()).get();
      StreamStatus streamStatus = describeResult.streamDescription().streamStatus();
      if(!StreamStatus.ACTIVE.equals(streamStatus) && !StreamStatus.UPDATING.equals(streamStatus)) {
        setInitializationFailed(true);
        addError("Stream " + streamName + " is not ready (in active/updating status) for appender: " + name);
      }
    }
    catch(InterruptedException ie) {
      setInitializationFailed(true);
      addError("Interrupted while attempting to describe " + streamName, ie);
    }
    catch(ExecutionException ee) {
      setInitializationFailed(true);
      addError("Error executing the operation", ee);
    }
    catch(ResourceNotFoundException rnfe) {
      setInitializationFailed(true);
      addError("Stream " + streamName + " doesn't exist for appender: " + name, rnfe);
    }
    catch(AwsServiceException ase) {
      setInitializationFailed(true);
      addError("Error connecting to AWS to verify stream " + streamName + " for appender: " + name, ase);
    }
  }

  @Override
  protected void putMessage(String message) throws Exception {
    SdkBytes data = SdkBytes.fromByteArray(message.getBytes(getEncoding()));
    getClient().putRecord(builder -> 
      builder
        .partitionKey(UUID.randomUUID().toString())
        .streamName(getStreamName())
        .data(data)
        .build()).whenCompleteAsync(asyncCallHandler);
  }

}
