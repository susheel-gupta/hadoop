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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contract.ABFSContractTestBinding;
import org.apache.hadoop.fs.azurebfs.contract.AbfsFileSystemContract;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Contract test for seek operation.
 */
public class ITestAbfsFileSystemContractSeek extends AbstractContractSeekTest{
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  public ITestAbfsFileSystemContractSeek() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    // set buffer size to min (16KB)
    conf.setInt(AZURE_READ_BUFFER_SIZE, MIN_BUFFER_SIZE);
    return new AbfsFileSystemContract(conf, isSecure);
  }

  @Test
  public void testSeekResettingBuffer() throws Throwable {
    describe("verify that seeks are not resetting buffers");

    // Create 100KB file
    Path testSeekFile = path("bigseekfile.txt");
    byte[] block = dataset(100 * 1024, 0, 255);
    createFile(getFileSystem(), testSeekFile, true, block);

    FSDataInputStream in = getFileSystem().open(testSeekFile);
    AbfsInputStream instream = ((AbfsInputStream) in.getWrappedStream());
    assertEquals(MIN_BUFFER_SIZE, instream.getBufferSize());
    assertEquals(0, instream.getPos());

    byte[] b = new byte[3];
    instream.read(b);
    assertEquals(3, instream.getPos());

    // Seek should not reset buffer position
    instream.seek(16 * 1024);
    assertEquals(3, instream.getCursorWithinBuffer());
    instream.seek(0);
    assertEquals(3, instream.getCursorWithinBuffer());

    // seek back and read from buffer
    instream.seek(101);
    instream.read();
    assertEquals(102, instream.getCursorWithinBuffer());
  }

  @Test
  public void testPositionalReads() throws Throwable {
    describe("verify that positional reads are not resetting buffers");

    // Create 100KB file
    Path testSeekFile = path("bigseekfile.txt");
    byte[] block = dataset(100 * 1024, 0, 255);
    createFile(getFileSystem(), testSeekFile, true, block);

    FSDataInputStream in = getFileSystem().open(testSeekFile);
    AbfsInputStream instream = ((AbfsInputStream) in.getWrappedStream());

    assertEquals(MIN_BUFFER_SIZE, instream.getBufferSize());
    assertEquals(0, instream.getPos());

    byte[] b = new byte[10];
    // pread 10 bytes @100
    instream.readFully(100, b, 0, b.length);
    assertEquals(0, instream.getPos());
    assertEquals(10, instream.getCursorWithinBuffer());

    // pread 10 bytes @110, it should still read from buffer
    instream.readFully(110, b, 0, b.length);
    assertEquals(0, instream.getPos());
    assertEquals(20, instream.getCursorWithinBuffer());

    // move further and read
    b = new byte[3];
    instream.readFully(200, b, 0, b.length);
    assertEquals(0, instream.getPos());
    assertEquals(103, instream.getCursorWithinBuffer());

    // read a byte (@0) should reset to buffer cursor to 1
    instream.read();
    assertEquals(1, instream.getPos());
    assertEquals(1, instream.getCursorWithinBuffer());

    // read 3 bytes
    instream.read(b);
    assertEquals(4, instream.getCursorWithinBuffer());
    assertEquals(4, instream.getPos());

    // seek should not change buffer position
    instream.seek(1004);
    assertEquals(4, instream.getCursorWithinBuffer());
  }
}
