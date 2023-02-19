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

package cn.edu.tsinghua.iotdb.benchmark.tsdb.enums;

public enum DBInsertMode {
  INSERT_USE_JDBC("JDBC"),
  INSERT_USE_SESSION("SESSION"),
  INSERT_USE_SESSION_TABLET("SESSION_BY_TABLET"),
  INSERT_USE_SESSION_RECORD("SESSION_BY_RECORD"),
  INSERT_USE_SESSION_RECORDS("SESSION_BY_RECORDS"),
  INSERT_USE_SESSION_POOL("SESSION_POOL"),
  TICKTOCK_INSERT_USE_HTTP_LINE("HTTP_LINE"), // TickTock uses Http and influxdb line protocol.
  TICKTOCK_INSERT_USE_HTTP_PLAIN(
      "HTTP_PLAIN"), // TickTock uses Http and opentsdb plain put protocol.
  TICKTOCK_INSERT_USE_TCP_LINE("TCP_LINE"), // TickTock uses Tcp and influxdb line protocol.
  TICKTOCK_INSERT_USE_TCP_PLAIN("TCP_PLAIN"), // TickTock uses Tcp and opentsdb plain put protocol.
  INFLUX_INSERT_USE_LINE("LINE"); // InfluxDB v1 uses line protocol for write.

  String insertType;

  DBInsertMode(String insertType) {
    this.insertType = insertType;
  }

  @Override
  public String toString() {
    return insertType;
  }
}
