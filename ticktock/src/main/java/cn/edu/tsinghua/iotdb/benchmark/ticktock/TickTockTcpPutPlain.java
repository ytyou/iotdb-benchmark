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

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;

public class TickTockTcpPutPlain extends TickTockReadPlain implements IDatabase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TickTockTcpPutPlain.class);

  protected String writeHost = null;
  protected int writePort;
  protected ThreadLocal<Socket> threadLocalSocket = null;
  protected ThreadLocal<PrintWriter> threadLocalWriter = null;

  /** constructor. */
  public TickTockTcpPutPlain(DBConfig dbConfig) {
    super(dbConfig);
    writeHost = dbConfig.getHOST().get(0);
    writePort = Integer.parseInt(dbConfig.getPORT().get(1));
    threadLocalSocket = new ThreadLocal<>();
    threadLocalWriter = new ThreadLocal<>();
  }

  @Override
  public void cleanup() throws TsdbException {
    // example JDBC_URL:
    // http://host:4242/api/query?start=2016/02/16-00:00:00&end=2016/02/17-23:59:59&m=avg:1ms-avg:metricname
    for (int i = 0; i < config.getGROUP_NUMBER(); i++) {
      String metric = "";
      String metricName = metric + "group_" + i;
      String DELETE_METRIC_URL = "%s?start=%s&m=sum:1ms-sum:%s";
      String deleteMetricURL =
          String.format(DELETE_METRIC_URL, queryUrl, Constants.START_TIMESTAMP, metricName);
      String response;
      try {
        response = HttpRequest.sendDelete(deleteMetricURL, "");
        LOGGER.info("Delete old data of {} ...", metricName);
        LOGGER.debug("Delete request response: {}", response);
      } catch (IOException e) {
        LOGGER.error("Delete old TickTock metric {} failed. Error: {}", metricName, e.getMessage());
        throw new TsdbException(e);
      }
    }
  }

  protected PrintWriter setupWriter() throws Exception {
    Socket socket = new Socket(InetAddress.getByName(writeHost), writePort);
    socket.setSoLinger(true, 10);
    PrintWriter writer = new PrintWriter(socket.getOutputStream(), false);
    threadLocalSocket.set(socket);
    threadLocalWriter.set(writer);
    return writer;
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
      PrintWriter writer = threadLocalWriter.get();
      if (writer == null) {
        writer = setupWriter();
      }
      writer.print(builder.toString());
      writer.flush();
      return new Status(true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    return insertOneBatch(batch);
  }
}
