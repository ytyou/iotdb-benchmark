/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.ticktock;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OpenTSDBPlainPutModel implements Serializable {
  private static final long serialVersionUID = 1L;
  private String metric;
  private long timestamp;
  private Object value;
  private Map<String, String> tags = new HashMap<String, String>();

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public void toLines(StringBuilder builder) {
    builder.append("put ");
    builder.append(metric);
    builder.append(" ");
    builder.append(Long.toString(timestamp));
    builder.append(" ");
    builder.append(value.toString());
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      builder.append(" ");
      builder.append(entry.getKey());
      builder.append("=");
      builder.append(entry.getValue());
    }
    builder.append("\n");
  }
}
