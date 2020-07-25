package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.fakedb.FakeDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDBSession;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.kairosdb.KairosDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.opentsdb.OpenTSDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.summarystore.SummaryStoreDB;
import com.samsung.sra.datastore.SummaryStore;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  public IDatabase getDatabase() throws SQLException {

    switch (config.DB_SWITCH) {
      case Constants.DB_IOT:
        switch (config.INSERT_MODE) {
          case Constants.INSERT_USE_JDBC:
            return new IoTDB();
          case Constants.INSERT_USE_SESSION:
            return new IoTDBSession();
        }
      case Constants.DB_INFLUX:
        return new InfluxDB();
      case Constants.DB_KAIROS:
        return new KairosDB();
      case Constants.DB_TIMESCALE:
        return new TimescaleDB();
      case Constants.DB_FAKE:
        return new FakeDB();
      case Constants.DB_OPENTS:
        return new OpenTSDB();
      case Constants.DB_SUMMARYSTORE:
        return new SummaryStoreDB(getSummaryStoreDB());
      default:
        LOGGER.error("unsupported database {}", config.DB_SWITCH);
        throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
  }

  private static AtomicInteger cnt = new AtomicInteger(0);
  private SummaryStore store = null;
  private volatile boolean isInited = false;

  public SummaryStore getSummaryStoreDB() throws SQLException{
    if(isInited){
      return store;
    }
    synchronized (this){
      if(!isInited){
        try {
          cnt.getAndIncrement();
          LOGGER.info("summary store init number:{}", cnt.get());
          store = new SummaryStore(SummaryStoreDB.storeLoc, new SummaryStore.StoreOptions().setKeepReadIndexes(true));
          isInited = true;
        } catch (Exception e) {
          e.printStackTrace();
          throw new SQLException(
              "Init SummaryStoreDB client failed, the Message is " + e.getMessage());
        }
      }
      return store;
    }
  }

}
