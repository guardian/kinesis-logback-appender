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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * 
 * Custom credentials provider chain to look for AWS credentials.
 *
 * Implementation will look for credentials in the following priority:
 *  - AwsCredentials.properties file in classpath
 *  - Instance profile credentials delivered through the Amazon EC2 metadata
 *  - Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
 *  - Java System Properties - aws.accessKeyId and aws.secretKey
 *  - Profile Credentials - default profile unless AWS_PROFILE environment variable set
 */
public final class CustomCredentialsProviderChain {
  public final static AwsCredentialsProviderChain chain = AwsCredentialsProviderChain.builder().credentialsProviders(
    ContainerCredentialsProvider.builder().build(),
    InstanceProfileCredentialsProvider.create(),
    SystemPropertyCredentialsProvider.create(),
    EnvironmentVariableCredentialsProvider.create(),
    ProfileCredentialsProvider.create()
  ).build();
}
