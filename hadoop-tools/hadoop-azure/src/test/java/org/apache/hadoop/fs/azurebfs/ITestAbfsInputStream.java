/**
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

package org.apache.hadoop.fs.azurebfs;

import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;

/**
 * Test Abfs Input Stream readahead config/state.
 * This is an internal class exclusive to cloudera (why?) and was
 * renamed from ITestAbfsInputStream to avoid confusion with
 * {@link org.apache.hadoop.fs.azurebfs.services.ITestAbfsInputStream}.
 */

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {
  private static final Path TEST_PATH = new Path("/ITestAbfsInputStream");

  /**
   * what do we want from our readahead state?
   * Toggle depending on what the config is set to.
   */
  private static final boolean EXPECTED_READAHEAD_ENABLED_STATE = true;

  public ITestAbfsInputStream() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    final byte[] b = new byte[2 * DEFAULT_READ_BUFFER_SIZE];
    new Random().nextBytes(b);
    try (FSDataOutputStream stream = getFileSystem().create(TEST_PATH)) {
      stream.write(b);
    }
  }

  @Override
  public void teardown() throws Exception {
    getFileSystem().delete(TEST_PATH, true);
    super.teardown();
  }

  @Test
  public void testReadAheadDefault() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    final FileSystem.Statistics statistics = fs.getFsStatistics();
    final AbfsInputStream abfsInputStream = abfsStore.openFileForRead(
        TEST_PATH, statistics, getTestTracingContext(fs, false));
    Assertions.assertThat(abfsInputStream.isReadAheadEnabled())
        .describedAs("readahead should be %s with default conf.",
            EXPECTED_READAHEAD_ENABLED_STATE)
        .isEqualTo(EXPECTED_READAHEAD_ENABLED_STATE);
  }

  @Test
  public void testAbfsInputStreamReadAhead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();

    abfsConfiguration.setWriteBufferSize(DEFAULT_READ_BUFFER_SIZE);
    abfsConfiguration.setReadBufferSize(DEFAULT_READ_BUFFER_SIZE);
    testAbfsInputStreamReadAheadConfigDisable();
    testAbfsInputStreamReadAheadConfigEnable();
  }

  private void testAbfsInputStreamReadAheadConfigDisable() throws Exception {
    final Configuration config = new Configuration(getRawConfiguration());
    config.set(FS_AZURE_ENABLE_READAHEAD_V2,
        String.valueOf(Boolean.FALSE));

    AzureBlobFileSystem testAbfs =
        (AzureBlobFileSystem) FileSystem.newInstance(config);
    final AzureBlobFileSystemStore abfsStore = testAbfs.getAbfsStore();
    final FileSystem.Statistics statistics = testAbfs.getFsStatistics();
    final AbfsInputStream abfsInputStream = abfsStore.openFileForRead(
        TEST_PATH, statistics, getTestTracingContext(testAbfs, false));
    assertFalse("ReadAhead should be disabled if it's disabled in " +
        "the configuration.", abfsInputStream.isReadAheadEnabled());
    final byte[] readBuffer = new byte[2 * DEFAULT_READ_BUFFER_SIZE];
    abfsInputStream.read(readBuffer, 0, DEFAULT_READ_BUFFER_SIZE);
    abfsInputStream.read(readBuffer, DEFAULT_READ_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE);
    AbfsInputStreamStatisticsImpl abfsInputStreamStatistics =
            (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
    assertEquals(
            "If the readahead is disabled there should be no read from the readahead buffers.",
            0, abfsInputStreamStatistics.getReadAheadBytesRead());
  }

  private void testAbfsInputStreamReadAheadConfigEnable() throws Exception {
    final Configuration config = new Configuration(getRawConfiguration());
    config.set(FS_AZURE_ENABLE_READAHEAD_V2,
        String.valueOf(Boolean.TRUE));

    AzureBlobFileSystem testAbfs =
        (AzureBlobFileSystem) FileSystem.newInstance(config);
    final AzureBlobFileSystemStore abfsStore = testAbfs.getAbfsStore();
    final FileSystem.Statistics statistics = testAbfs.getFsStatistics();
    final AbfsInputStream abfsInputStream = abfsStore.openFileForRead(
        TEST_PATH, statistics, getTestTracingContext(testAbfs, false));
    assertTrue("ReadAhead should be enabled if it's enabled in " +
        "the configuration.", abfsInputStream.isReadAheadEnabled());

    final byte[] readBuffer = new byte[2 * DEFAULT_READ_BUFFER_SIZE];
    abfsInputStream.read(readBuffer, 0, (DEFAULT_READ_BUFFER_SIZE));
    abfsInputStream.read(readBuffer, DEFAULT_READ_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE);
    AbfsInputStreamStatisticsImpl abfsInputStreamStatistics =
            (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
    assertTrue(
            "If the readahead is enabled there should be read from the readahead buffers.",
        0 < abfsInputStreamStatistics.getReadAheadBytesRead());
  }

}
