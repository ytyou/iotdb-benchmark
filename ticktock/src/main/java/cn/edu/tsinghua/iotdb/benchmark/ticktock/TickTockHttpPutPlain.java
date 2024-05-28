package cn.edu.tsinghua.iotdb.benchmark.ticktock;

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

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class TickTockHttpPutPlain extends TickTockReadPlain implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockHttpPutPlain.class);
  protected String writeUrl;

  /** constructor. */
  public TickTockHttpPutPlain(DBConfig dbConfig) {
    super(dbConfig);
    writeUrl = "http://" + host + ":" + writePort + "/api/put";
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      // create dataModel
      LinkedList<OpenTSDBPlainPutModel> models = createOpenTSDBDataModelByBatch(batch);
      StringBuilder builder = new StringBuilder();
      for (OpenTSDBPlainPutModel model : models) {
        model.toLines(builder);
      }
      HttpRequest.sendPost(writeUrl, builder.toString());
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    try {
      // create dataModel
      LinkedList<OpenTSDBPlainPutModel> models = createOpenTSDBDataModelByBatch(batch);
      StringBuilder builder = new StringBuilder();
      for (OpenTSDBPlainPutModel model : models) {
        model.toLines(builder);
      }
      HttpRequest.sendPost(writeUrl, builder.toString());
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }
}
