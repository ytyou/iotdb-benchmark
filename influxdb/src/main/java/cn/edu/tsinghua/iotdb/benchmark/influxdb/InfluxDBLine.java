package cn.edu.tsinghua.iotdb.benchmark.influxdb;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** Influxdb V1, but using v2 line protocol for write. Read still uses v1 API. */
public class InfluxDBLine extends InfluxDB {
  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBLine.class);
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  private String V2_URL = "http://%s/api/v2/write?org=%s&bucket=%s&precision=%s";
  private final String token;
  private final String org;

  /** constructor. */
  public InfluxDBLine(DBConfig dbConfig) {
    super(dbConfig);
    token = dbConfig.getTOKEN();
    org = dbConfig.getDB_NAME();
    V2_URL =
        String.format(
            V2_URL,
            dbConfig.getHOST().get(0) + ":" + dbConfig.getPORT().get(0),
            org,
            influxDbName,
            config.getTIMESTAMP_PRECISION());
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDataModelV2> influxDBModels = createDataModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDataModelV2 influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }
      HttpRequestUtil.sendPost(
          V2_URL, String.join("\n", lines), "text/plain; version=0.0.4; charset=utf-8", token);
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    return insertOneBatch(batch);
  }

  private String model2write(InfluxDataModelV2 influxDBModel) {
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

  private LinkedList<InfluxDataModelV2> createDataModelByBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    List<Record> records = batch.getRecords();
    List<String> sensors = deviceSchema.getSensors();
    LinkedList<InfluxDataModelV2> models = new LinkedList<>();

    for (Record record : records) {
      InfluxDataModelV2 model =
          createModel(deviceSchema.getGroup(), deviceSchema.getDevice(), record, sensors);
      models.addLast(model);
    }
    return models;
  }

  private InfluxDataModelV2 createModel(
      String metric, String device, Record record, List<String> sensors) {
    InfluxDataModelV2 model = new InfluxDataModelV2();
    model.setMetric(metric);
    model.setTimestamp(record.getTimestamp());

    model.addTag("device", device);

    for (int i = 0; i < record.getRecordDataValue().size(); i++) {
      model.addField(sensors.get(i), record.getRecordDataValue().get(i));
    }
    return model;
  }
}
