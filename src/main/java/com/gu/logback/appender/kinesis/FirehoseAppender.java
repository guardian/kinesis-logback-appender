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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClientBuilder;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
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
    extends BaseKinesisAppender<Event, FirehoseAsyncClient> {

  private FirehoseStatsReporter asyncCallHandler = new FirehoseStatsReporter(this);

  @Override
  protected FirehoseAsyncClient createClient(AwsCredentialsProvider credentials, ClientOverrideConfiguration configuration, ThreadPoolExecutor executor, Region region, Optional<URI> endpointOverride) {
    FirehoseAsyncClientBuilder builder = FirehoseAsyncClient.builder()
      .credentialsProvider(credentials)
      .asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor))
      .overrideConfiguration(configuration)
      .region(region);
    
    endpointOverride.ifPresent(endpoint -> builder.endpointOverride(endpoint));
    
    return builder.build();
  }

  @Override
  protected void validateStreamName(FirehoseAsyncClient client, String streamName) {
    DescribeDeliveryStreamResponse describeResponse;
    try {
      describeResponse = getClient()
        .describeDeliveryStream(b -> b.deliveryStreamName(streamName).build())
        .get();
      DeliveryStreamStatus streamStatus = describeResponse.deliveryStreamDescription().deliveryStreamStatus();
      if(!DeliveryStreamStatus.ACTIVE.equals(streamStatus)) {
        setInitializationFailed(true);
        addError("Stream " + streamName + " is not ready (in active status) for appender: " + name);
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
        .deliveryStreamName(getStreamName())
        .record(b -> b.data(data).build())
        .build()).whenCompleteAsync(asyncCallHandler);
  }

}
