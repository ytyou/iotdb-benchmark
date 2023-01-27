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
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TickTockHttpWriteLine extends TickTockHttpPutPlain implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockHttpWriteLine.class);
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  /** constructor. */
  public TickTockHttpWriteLine(DBConfig dbConfig) {
    super(dbConfig);

    writeUrl =
        "http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0) + "/api/write";
  }

  @Override
  public void init() throws TsdbException {
    HttpRequest.init();
  }

  @Override
  public void cleanup() throws TsdbException {}

  // no need for ticktock and opentsdb
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {}

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDBModel> influxDBModels = createDataModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDBModel influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }

      HttpRequest.sendPost(writeUrl, String.join("\n", lines));
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    return insertOneBatch(batch);
  }

  private String model2write(InfluxDBModel influxDBModel) {
    StringBuffer result = new StringBuffer(influxDBModel.getMetric());
    if (influxDBModel.getTags() != null) {
      for (Map.Entry<String, String> pair : influxDBModel.getTags().entrySet()) {
        result.append(",");
        result.append(pair.getKey());
        result.append("=");
        result.append(pair.getValue());
      }
    }
    result.append(" ");
    if (influxDBModel.getFields() != null) {
      boolean first = true;
      for (Map.Entry<String, Object> pair : influxDBModel.getFields().entrySet()) {
        if (first) {
          first = false;
        } else {
          result.append(",");
        }
        result.append(pair.getKey());
        result.append("=");
        // get value
        String type =
            typeMap(
                baseDataSchema.getSensorType(influxDBModel.getTags().get("device"), pair.getKey()));
        switch (type) {
          case "BOOLEAN":
            result.append(((boolean) pair.getValue()) ? "true" : "false");
            break;
          case "INT32":
            result.append((int) pair.getValue());
            break;
          case "INT64":
            result.append((long) pair.getValue());
            break;
          case "FLOAT":
            result.append((float) pair.getValue());
            break;
          case "DOUBLE":
            result.append((double) pair.getValue());
            break;
          case "TEXT":
            result.append("\"").append(pair.getValue()).append("\"");
            break;
          default:
            LOGGER.error("Unsupported data type {}, use default data type: BINARY.", type);
            return "TEXT";
        }
      }
    }
    result.append(" ");
    result.append(influxDBModel.getTimestamp());
    return result.toString();
  }

  private LinkedList<InfluxDBModel> createDataModelByBatch(Batch batch) {
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

  private InfluxDBModel createModel(
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
