/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_COPY_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_COPY_THRESHOLD_MAX;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Use {@link Constants#FAST_UPLOAD_BUFFER_ARRAY} for buffering.
 * This test also sets the multipart copy threshold to its maximum.
 * This provides a way of measuring the speedup which parallel part
 * copy delivers for the chosen test file size.
 * For the other tests, the configuration option
 * {@code fs.s3a.scale.test.huge.partitionsize} sets the part size;
 * this can be set on the maven command line along with the file size
 * <pre>
 *   mvn verify -T 1C -Dtest=none -Dit.test=ITestS3AHugeFilesArrayBlocks -Dscale -Dfs.s3a.scale.test.huge.filesize=256M
 * </pre>
 */
public class ITestS3AHugeFilesArrayBlocks extends AbstractSTestS3AHugeFiles {

  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    // set the multipart threshold.
    // this does not clear any bucket overrrides, to allow for more
    // experimentation, and to use a smaller copy factor when
    // doing a scale test with a file of many, many MB, which is
    // likely to time out
    // removeBaseAndBucketOverrides(conf, MULTIPART_COPY_THRESHOLD);
    conf.setLong(MULTIPART_COPY_THRESHOLD, MULTIPART_COPY_THRESHOLD_MAX);
    return conf;
  }
}
