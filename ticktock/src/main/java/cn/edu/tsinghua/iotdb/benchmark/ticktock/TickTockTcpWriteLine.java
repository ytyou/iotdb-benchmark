package cn.edu.tsinghua.iotdb.benchmark.ticktock;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TickTockTcpWriteLine extends TickTockTcpPutPlain {

  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockHttpWriteLine.class);
  private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

  /** constructor. */
  public TickTockTcpWriteLine(DBConfig dbConfig) {
    super(dbConfig);
    LOGGER.info("Constructor of TickTockTcpWriteLine");
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try {
      LinkedList<InfluxDBModel> influxDBModels =
          TickTockHttpWriteLine.createInfluxDBModelByBatch(batch);
      List<String> lines = new ArrayList<>();
      for (InfluxDBModel influxDBModel : influxDBModels) {
        lines.add(model2write(influxDBModel));
      }

      PrintWriter writer = threadLocalWriter.get();
      if (writer == null) {
        writer = setupWriter();
      }
      writer.print(String.join("\n", lines));
      writer.flush();
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.getMessage());
    }
  }
}
