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
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class TickTockReadPlain implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockHttpPutPlain.class);
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  protected final String queryUrl;
  protected final Random sensorRandom;

  /** constructor. */
  public TickTockReadPlain(DBConfig dbConfig) {
    sensorRandom = new Random(1 + config.getQUERY_SEED());
    queryUrl =
        "http://" + dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0) + "/api/query";
  }

  @Override
  public void init() throws TsdbException {
    HttpRequest.init();
  }

  @Override
  public void cleanup() throws TsdbException {}

  // no need for opentsdb
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {}

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", preciseQuery.getTimestamp() - 1);
    queryMap.put("end", preciseQuery.getTimestamp() + 1);
    list = getSubQueries(preciseQuery.getDeviceSchema(), "none");
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", rangeQuery.getStartTimestamp() - 1);
    queryMap.put("end", rangeQuery.getEndTimestamp() + 1);
    list = getSubQueries(rangeQuery.getDeviceSchema(), "none");
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", aggRangeQuery.getStartTimestamp() - 1);
    queryMap.put("end", aggRangeQuery.getEndTimestamp() + 1);
    list = getSubQueries(aggRangeQuery.getDeviceSchema(), "none");
    for (Map<String, Object> subQuery : list) {
      subQuery.put("downsample", "0all-" + aggRangeQuery.getAggFun());
    }
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    Exception e = new TsdbException("OpenTSDB don't support this kind of query");
    return new Status(false, 0, e, e.getMessage());
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", groupByQuery.getStartTimestamp() - 1);
    queryMap.put("end", groupByQuery.getEndTimestamp() + 1);
    list = getSubQueries(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    for (Map<String, Object> subQuery : list) {
      subQuery.put("downsample", groupByQuery.getGranularity() + "ms-" + groupByQuery.getAggFun());
    }
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, false);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    Map<String, Object> queryMap = new HashMap<>();
    List<Map<String, Object>> list = null;
    queryMap.put("msResolution", true);
    queryMap.put("start", latestPointQuery.getStartTimestamp() - 1);
    queryMap.put("end", latestPointQuery.getEndTimestamp() + 1);
    list = getSubQueries(latestPointQuery.getDeviceSchema(), "none");
    for (Map<String, Object> subQuery : list) {
      subQuery.put("downsample", "0all-last");
    }
    queryMap.put("queries", list);
    String sql = JSON.toJSONString(queryMap);
    return executeQueryAndGetStatus(sql, true);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public void close() {
    try {
      HttpRequest.close();
    } catch (Exception e) {
    }
  }

  protected LinkedList<OpenTSDBPlainPutModel> createOpenTSDBDataModelByBatch(Batch batch)
      throws TsdbException {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    String device = deviceSchema.getDevice();
    List<Record> records = batch.getRecords();
    List<String> sensors = deviceSchema.getSensors();
    int sensorNum = sensors.size();
    int recordNum = records.size();
    LinkedList<OpenTSDBPlainPutModel> models = new LinkedList<>();

    for (int i = 0; i < recordNum; i++) {
      Record record = records.get(i);
      if (batch.getColIndex() != -1) {
        // 只插入一列
        OpenTSDBPlainPutModel model = new OpenTSDBPlainPutModel();
        model.setMetric(deviceSchema.getGroup());
        model.setTimestamp(record.getTimestamp());
        model.setValue(record.getRecordDataValue().get(0));
        Map<String, String> tags = new HashMap<>();
        tags.put("device", device);
        tags.put("sensor", deviceSchema.getSensors().get(batch.getColIndex()));
        model.setTags(tags);
        models.addLast(model);
      } else {
        // 插入对齐数据
        for (int j = 0; j < sensorNum; j++) {
          OpenTSDBPlainPutModel model = new OpenTSDBPlainPutModel();
          model.setMetric(deviceSchema.getGroup());
          model.setTimestamp(record.getTimestamp());
          model.setValue(record.getRecordDataValue().get(j));
          Map<String, String> tags = new HashMap<>();
          tags.put("device", device);
          tags.put("sensor", deviceSchema.getSensors().get(j));
          model.setTags(tags);
          models.addLast(model);
        }
      }
    }

    return models;
  }

  private Status executeQueryAndGetStatus(String sql, boolean isLatestPoint) {
    LOGGER.debug("{} query SQL: {}", Thread.currentThread().getName(), sql);
    try {
      String response;
      response = HttpRequest.sendPost(queryUrl, sql);
      int pointNum = getOneQueryPointNum(response, isLatestPoint);
      LOGGER.debug("{} 查到数据点数: {}", Thread.currentThread().getName(), pointNum);
      return new Status(true, pointNum);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, sql);
    }
  }

  private int getOneQueryPointNum(String str, boolean isLatestPoint) {
    int pointNum = 0;
    if (!isLatestPoint) {
      JSONArray jsonArray = JSON.parseArray(str);
      for (int i = 0; i < jsonArray.size(); i++) {
        JSONObject json = (JSONObject) jsonArray.get(i);
        pointNum += json.getJSONObject("dps").size();
      }
    } else {
      JSONArray jsonArray = JSON.parseArray(str);
      pointNum += jsonArray.size();
    }
    return pointNum;
  }

  private List<Map<String, Object>> getSubQueries(List<DeviceSchema> devices, String aggreFunc) {
    List<Map<String, Object>> list = new ArrayList<>();

    List<String> sensorList = new ArrayList<>();
    for (String sensor : devices.get(0).getSensors()) {
      sensorList.add(sensor);
    }
    Collections.shuffle(sensorList, sensorRandom);
    // group2device
    Map<String, List<String>> metric2devices = new HashMap<>();
    for (DeviceSchema d : devices) {
      String m = d.getGroup();
      metric2devices.putIfAbsent(m, new ArrayList());
      metric2devices.get(m).add(d.getDevice());
    }

    for (Map.Entry<String, List<String>> queryMetric : metric2devices.entrySet()) {
      Map<String, Object> subQuery = new HashMap<>();
      subQuery.put("aggregator", aggreFunc);
      subQuery.put("metric", queryMetric.getKey());

      Map<String, String> tags = new HashMap<>();
      String deviceStr = "";
      for (String d : queryMetric.getValue()) {
        deviceStr += "|" + d;
      }
      deviceStr = deviceStr.substring(1);

      String sensorStr = sensorList.get(0);
      for (int i = 1; i < config.getQUERY_SENSOR_NUM(); i++) {
        sensorStr += "|" + sensorList.get(i);
      }
      tags.put("sensor", sensorStr);
      tags.put("device", deviceStr);
      subQuery.put("tags", tags);
      list.add(subQuery);
    }
    return list;
  }

  protected String model2write(InfluxDBModel influxDBModel) {
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
}
