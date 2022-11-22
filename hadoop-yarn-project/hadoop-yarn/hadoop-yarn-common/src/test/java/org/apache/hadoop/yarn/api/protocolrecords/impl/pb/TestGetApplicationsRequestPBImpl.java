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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestGetApplicationsRequestPBImpl {
  @SuppressWarnings("checkstyle:visibilitymodifier")
  public GetApplicationsRequestPBImpl impl;

  @MethodSource("data")
  @ParameterizedTest
  void testAppTagsLowerCaseConversionDefault(
      GetApplicationsRequestPBImpl applicationsRequestPBImpl) {
    initTestGetApplicationsRequestPBImpl(applicationsRequestPBImpl);
    applicationsRequestPBImpl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    applicationsRequestPBImpl.getApplicationTags().forEach(s -> assertEquals(s, s.toLowerCase()));
  }

  @MethodSource("data")
  @ParameterizedTest
  void testAppTagsLowerCaseConversionDisabled(
      GetApplicationsRequestPBImpl applicationsRequestPBImpl) {
    initTestGetApplicationsRequestPBImpl(applicationsRequestPBImpl);
    GetApplicationsRequestPBImpl.setForceLowerCaseTags(false);
    applicationsRequestPBImpl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    applicationsRequestPBImpl.getApplicationTags()
        .forEach(s -> assertNotEquals(s, s.toLowerCase()));
  }

  @MethodSource("data")
  @ParameterizedTest
  void testAppTagsLowerCaseConversionEnabled(
      GetApplicationsRequestPBImpl applicationsRequestPBImpl) {
    initTestGetApplicationsRequestPBImpl(applicationsRequestPBImpl);
    GetApplicationsRequestPBImpl.setForceLowerCaseTags(true);
    applicationsRequestPBImpl.setApplicationTags(Sets.newHashSet("ABcd", "efgH"));
    applicationsRequestPBImpl.getApplicationTags().forEach(s -> assertEquals(s, s.toLowerCase()));
  }

  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<>();
    list.add(new Object[]{new GetApplicationsRequestPBImpl()});
    list.add(new Object[]{new GetApplicationsRequestPBImpl(
        GetApplicationsRequestProto.newBuilder().build())});

    return list;
  }

  public void initTestGetApplicationsRequestPBImpl(
      GetApplicationsRequestPBImpl applicationsRequestPBImpl) {
    this.impl = impl;
  }
}
