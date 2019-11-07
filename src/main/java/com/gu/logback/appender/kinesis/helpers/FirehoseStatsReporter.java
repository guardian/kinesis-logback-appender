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

package com.gu.logback.appender.kinesis.helpers;

import java.util.function.BiConsumer;
import software.amazon.awssdk.services.firehose.model.PutRecordResponse;
import com.gu.logback.appender.kinesis.FirehoseAppender;

/**
 * Gathers information on how many put requests made by AWS SDK's async client,
 * succeeded or failed since the beginning
 */
public final class FirehoseStatsReporter implements BiConsumer<PutRecordResponse,Throwable> {

  private final String appenderName;
  private long successfulRequestCount;
  private long failedRequestCount;
  private final FirehoseAppender<?> appender;

  public FirehoseStatsReporter(FirehoseAppender<?> appender) {
    this.appenderName = appender.getStreamName();
    this.appender = appender;
  }

  /**
   * This method is invoked when an operation to send a log record to Kinesis has
   * completed either successfully or not. These logs would end up in the application
   * log if configured properly.
   */
  @Override
  public final void accept(PutRecordResponse response, Throwable exception) {
    if (exception != null) {
      failedRequestCount++;
      appender.addError("Failed to publish a log entry to kinesis using appender: " + appenderName, exception);
    } else {
      successfulRequestCount++;
    }
  }

  public final long getSuccessfulRequestCount() {
    return successfulRequestCount;
  }

  public final long getFailedRequestCount() {
    return failedRequestCount;
  }
}
