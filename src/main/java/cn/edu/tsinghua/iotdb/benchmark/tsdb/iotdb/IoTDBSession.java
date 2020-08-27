package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSession extends IoTDB {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Session session;

  public IoTDBSession() {
    super();
    session = new Session(config.HOST, config.PORT, Constants.USER, Constants.PASSWD);
    try {
      if (config.ENABLE_THRIFT_COMPRESSION) {
        session.open(true);
      } else {
        session.open();
      }
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    int count = 0;
    if (!config.OPERATION_PROPORTION.split(":")[0].equals("0")) {
      try {
        // get all storage groups
        Set<String> groups = new HashSet<>();
        for (DeviceSchema schema : schemaList) {
          groups.add(schema.getGroup());
        }
        // register storage groups
        for (String group : groups) {
          session.setStorageGroup(Constants.ROOT_SERIES_NAME + "." + group);
        }
      } catch (StatementExecutionException e) {
        e.printStackTrace();
      } catch (IoTDBConnectionException e) {
        e.printStackTrace();
      }
      // create time series
      List<String> paths = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      List<TSEncoding> encodings = new ArrayList<>();
      List<CompressionType> compressors = new ArrayList<>();
      for (DeviceSchema deviceSchema : schemaList) {
        int sensorIndex = 0;
        for (String sensor : deviceSchema.getSensors()) {
          paths.add(Constants.ROOT_SERIES_NAME
              + "." + deviceSchema.getGroup()
              + "." + deviceSchema.getDevice()
              + "." + sensor);
          String typeString = getNextDataTypeString(sensorIndex);
          TSDataType type = TSDataType.valueOf(typeString);
          dataTypes.add(type);
          TSEncoding.valueOf(getEncodingType(typeString));
          compressors.add(CompressionType.findByShortName(config.COMPRESSOR));
          count++;
          sensorIndex++;
          if (count % 5000 == 0) {
            try {
              session
                  .createMultiTimeseries(paths, dataTypes, encodings, compressors,
                      Collections.emptyList(),
                      Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            }
            paths = new ArrayList<>();
            dataTypes = new ArrayList<>();
            encodings = new ArrayList<>();
            compressors = new ArrayList<>();
          }
        }
      }
      try {
        session
            .createMultiTimeseries(paths, dataTypes, encodings, compressors,
                Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
          LOGGER.error("Register IoTDB schema failed because ", e);
          throw new TsdbException(e);
        }
      }
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    int sensorIndex = 0;
    for (String sensor : batch.getDeviceSchema().getSensors()) {
      String dataType = getNextDataTypeString(sensorIndex);
      schemaList.add(new MeasurementSchema(sensor, Enum.valueOf(TSDataType.class, dataType),
          Enum.valueOf(TSEncoding.class, getEncodingType(dataType))));
      sensorIndex++;
    }
    String deviceId =
        Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." + batch
            .getDeviceSchema().getDevice();
    Tablet tablet = new Tablet(deviceId, schemaList, batch.getRecords().size());
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      tablet.rowSize++;
      Record record = batch.getRecords().get(recordIndex);
      sensorIndex = 0;
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0; recordValueIndex < record.getRecordDataValue().size();
          recordValueIndex++) {
        switch (getNextDataTypeString(sensorIndex)) {
          case "BOOLEAN":
            boolean[] sensorsBool = (boolean[]) values[recordValueIndex];
            sensorsBool[recordIndex] = (boolean) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "INT32":
            int[] sensorsInt = (int[]) values[recordValueIndex];
            sensorsInt[recordIndex] = (int) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "INT64":
            long[] sensorsLong = (long[]) values[recordValueIndex];
            sensorsLong[recordIndex] = (long) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "FLOAT":
            float[] sensorsFloat = (float[]) values[recordValueIndex];
            sensorsFloat[recordIndex] = (float) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "DOUBLE":
            double[] sensorsDouble = (double[]) values[recordValueIndex];
            sensorsDouble[recordIndex] = (double) record.getRecordDataValue().get(
                recordValueIndex);
            break;
          case "TEXT":
            Binary[] sensorsText = (Binary[]) values[recordValueIndex];
            sensorsText[recordIndex] = Binary
                .valueOf((String) record.getRecordDataValue().get(recordValueIndex));
            break;
        }
        sensorIndex++;
      }
    }
    try {
      session.insertTablet(tablet);
      tablet.reset();
      return new Status(true);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

}
