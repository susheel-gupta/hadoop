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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;

/**
 * Test Abfs Input Stream.
 */

public class ITestAbfsInputStream extends AbstractAbfsIntegrationTest {
  private static final Path TEST_PATH = new Path("/ITestAbfsInputStream");

  public ITestAbfsInputStream() throws Exception {
  }

  /**
   * This test will create a 2 * DEFAULT_READ_BUFFER_SIZE = 8MB file, and
   * use that during the test, then remove it.
   * @throws Exception
   */
  @Test
  public void testAbfsInputStreamReadAhead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();

    abfsConfiguration.setWriteBufferSize(DEFAULT_READ_BUFFER_SIZE);
    abfsConfiguration.setReadBufferSize(DEFAULT_READ_BUFFER_SIZE);

    try {

      final byte[] b = new byte[2 * DEFAULT_READ_BUFFER_SIZE];
      new Random().nextBytes(b);
      try (FSDataOutputStream stream = fs.create(TEST_PATH)) {
        stream.write(b);
      }

      testAbfsInputStreamReadAheadConfigDisable(b);
      testAbfsInputStreamReadAheadConfigEnable();

    } finally {
      fs.delete(TEST_PATH, true);
    }
  }

  private void testAbfsInputStreamReadAheadConfigDisable(byte[] b)
          throws Exception {
    final Configuration config = new Configuration(getRawConfiguration());
    config.set(FS_AZURE_ENABLE_READAHEAD,
        String.valueOf(Boolean.FALSE));

    AzureBlobFileSystem testAbfs =
        (AzureBlobFileSystem) FileSystem.newInstance(config);
    final AzureBlobFileSystemStore abfsStore = testAbfs.getAbfsStore();
    final FileSystem.Statistics statistics = testAbfs.getFsStatistics();
    try (AbfsInputStream abfsInputStream = abfsStore.openFileForRead(
        TEST_PATH, statistics)) {
      assertFalse("ReadAhead should be disabled if it's disabled in " +
          "the configuration.", abfsInputStream.isReadAheadEnabled());

      final byte[] readBuffer = new byte[2 * DEFAULT_READ_BUFFER_SIZE];

      // Read first half of the file.
      int dataRead = abfsInputStream.read(readBuffer, 0,
          DEFAULT_READ_BUFFER_SIZE);
      assertEquals("Unexpected number of bytes read from stream",
          DEFAULT_READ_BUFFER_SIZE, dataRead);
      assertEquals("Incorrect stream position after read.",
          DEFAULT_READ_BUFFER_SIZE, abfsInputStream.getPos());
      assertBufferStatsAreZero(abfsInputStream);

      // Read second half of the file.
      dataRead = abfsInputStream.read(readBuffer, DEFAULT_READ_BUFFER_SIZE,
          DEFAULT_READ_BUFFER_SIZE);
      assertEquals("Unexpected number of bytes read from stream",
          DEFAULT_READ_BUFFER_SIZE, dataRead);
      assertEquals("Incorrect stream position after read.",
         2 * DEFAULT_READ_BUFFER_SIZE, abfsInputStream.getPos());
      assertBufferStatsAreZero(abfsInputStream);

      // Seek to the beginning of the file and re-read the first quarter of the
      // file.
      abfsInputStream.seek(0);
      assertEquals(
         "Seeking to the beginning of the file should cause the position to "
          + "be 0.", 0, abfsInputStream.getPos());
      dataRead = abfsInputStream.read(readBuffer, 0,
         DEFAULT_READ_BUFFER_SIZE / 2);
      assertEquals("Unexpected number of bytes read from stream",
         DEFAULT_READ_BUFFER_SIZE / 2, dataRead);
      assertEquals("Incorrect stream position after read.",
         DEFAULT_READ_BUFFER_SIZE / 2, abfsInputStream.getPos());
      assertBufferStatsAreZero(abfsInputStream);

      // Validate the final state of the stream.
      assertTrue(
         "If readaheads are disabled, the readahead buffer should be null.",
          abfsInputStream.isBufferNull());
      assertArrayEquals("Disabling readaheads caused data corruption. The "
          + "data read did not match the data written for file " + TEST_PATH,
          b, readBuffer);
      assertBufferStatsAreZero(abfsInputStream);
    }
  }

  private void testAbfsInputStreamReadAheadConfigEnable()
          throws Exception {
    final Configuration config = new Configuration(getRawConfiguration());
    config.set(FS_AZURE_ENABLE_READAHEAD,
        String.valueOf(Boolean.TRUE));

    AzureBlobFileSystem testAbfs =
        (AzureBlobFileSystem) FileSystem.newInstance(config);
    final AzureBlobFileSystemStore abfsStore = testAbfs.getAbfsStore();
    final FileSystem.Statistics statistics = testAbfs.getFsStatistics();
    try (AbfsInputStream abfsInputStream = abfsStore.openFileForRead(
        TEST_PATH, statistics)) {
      assertTrue("ReadAhead should be enabled if it's enabled in " +
          "the configuration.", abfsInputStream.isReadAheadEnabled());

      final byte[] readBuffer = new byte[2 * DEFAULT_READ_BUFFER_SIZE];
      abfsInputStream.read(readBuffer, 0, (DEFAULT_READ_BUFFER_SIZE));
      abfsInputStream.read(readBuffer, DEFAULT_READ_BUFFER_SIZE,
          DEFAULT_READ_BUFFER_SIZE);
      AbfsInputStreamStatisticsImpl abfsInputStreamStatistics =
          (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
      assertTrue(
          "If the buffer is enabled there should be read from the buffer.",
          0 < abfsInputStreamStatistics.getBytesReadFromBuffer());
    }
  }

  private void assertBufferStatsAreZero(AbfsInputStream abfsInputStream) {
    AbfsInputStreamStatisticsImpl abfsInputStreamStatistics =
        (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
    assertEquals("Unexpected seekInBuffer stats count", 0,
              abfsInputStreamStatistics.getSeekInBuffer());
    assertEquals("Unexpected bytesReadFromBuffer stats count", 0,
              abfsInputStreamStatistics.getBytesReadFromBuffer());
  }
}
