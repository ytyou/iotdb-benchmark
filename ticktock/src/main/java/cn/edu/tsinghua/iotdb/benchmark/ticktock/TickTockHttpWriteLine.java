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

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TickTockHttpWriteLine extends TickTockHttpPutPlain implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockHttpWriteLine.class);

  /** constructor. */
  public TickTockHttpWriteLine(DBConfig dbConfig) {
    super(dbConfig);

    if (writePort.equals("8428")) {
      // It is for VictoriaMetrics API.
      writeUrl = "http://" + host + ":" + writePort + "/write";
    } else {
      writeUrl = "http://" + host + ":" + writePort + "/api/write";
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDBModel> influxDBModels = createInfluxDBModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDBModel influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }

      HttpRequest.sendPost(writeUrl, String.join("\n", lines) + "\n");
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    return insertOneBatch(batch);
  }

  @Override
  protected String getQuerySensorField() {
    return "_field";
  }

  static LinkedList<InfluxDBModel> createInfluxDBModelByBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    List<Record> records = batch.getRecords();
    List<String> sensors = deviceSchema.getSensors();
    LinkedList<InfluxDBModel> models = new LinkedList<>();

    for (Record record : records) {
      InfluxDBModel model =
          createModel(deviceSchema.getGroup(), deviceSchema.getDevice(), record, sensors);
      models.addLast(model);
    }
    return models;
  }

  static InfluxDBModel createModel(
      String metric, String device, Record record, List<String> sensors) {
    InfluxDBModel model = new InfluxDBModel();
    model.setMetric(metric);
    model.setTimestamp(record.getTimestamp());

    model.addTag("device", device);

    for (int i = 0; i < record.getRecordDataValue().size(); i++) {
      model.addField(sensors.get(i), record.getRecordDataValue().get(i));
    }
    return model;
  }
}
