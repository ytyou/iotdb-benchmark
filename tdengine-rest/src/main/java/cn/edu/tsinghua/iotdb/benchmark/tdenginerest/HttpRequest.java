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

package cn.edu.tsinghua.iotdb.benchmark.tdenginerest;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

/** From https://www.cnblogs.com/zhuawang/archive/2012/12/08/2809380.html */
public class HttpRequest {
  private static PoolingHttpClientConnectionManager cm;
  private static CloseableHttpClient httpClient = null;
  private static ThreadLocal<Long> requestId = null;
  private static String usr;
  private static String pwd;

  public static void init(String usrname, String passwd) {
    cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(1024);
    cm.setDefaultMaxPerRoute(1024);
    httpClient =
        HttpClients.custom().setConnectionManager(cm).setConnectionManagerShared(true).build();
    requestId = ThreadLocal.withInitial(() -> 0L);
    usr = usrname;
    pwd = passwd;
  }

  /**
   * 向指定URL发送GET方法的请求
   *
   * @param url 发送请求的URL
   * @param param 请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
   * @return JDBC_URL 所代表远程资源的响应结果
   */
  public static String sendGet(String url, String param) throws IOException {
    String result = "";
    BufferedReader in = null;
    InputStream inputStream = null;
    CloseableHttpResponse response = null;
    try {
      String urlNameString = url;
      if (param != null) urlNameString = urlNameString + "?" + param;
      HttpGet get = new HttpGet(urlNameString);
      Long id = requestId.get();
      String reqId = Thread.currentThread().getName() + "-" + id;
      requestId.set(id + 1);
      get.addHeader("X-Request-ID", reqId);
      response = httpClient.execute(get);
      Header[] headers = response.getHeaders("X-Request-ID");
      if (headers != null && headers.length > 0 && !reqId.equals(headers[0].getValue())
          || response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IOException("Bad HTTP response received. Code:" + response.getStatusLine().getStatusCode());
      }

      String encoding = Base64.getEncoder().encodeToString((usr + ":" + pwd).getBytes());
      get.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

      HttpEntity entity = response.getEntity();
      /* // 获取所有响应头字段
      Map<String, List<String>> map = connection.getHeaderFields();
      // 遍历所有的响应头字段
      for (String key : map.keySet()) {
          System.out.println(key + "--->" + map.get(key));
      }*/
      // 定义 BufferedReader输入流来读取URL的响应
      if (entity != null) {
        inputStream = entity.getContent();
        in = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = in.readLine()) != null) {
          result += line;
        }
      }
    } catch (Exception e) {
      throw e;
    }
    // 使用finally块来关闭输入流
    finally {
      try {
        release(response, inputStream, in);
      } catch (Exception e2) {
        throw e2;
      }
    }
    return result;
  }

  /**
   * 向指定 JDBC_URL 发送POST方法的请求
   *
   * @param url 发送请求的 JDBC_URL
   * @param param 请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
   * @return 所代表远程资源的响应结果
   */
  public static String sendPost(String url, String param) throws IOException {
    BufferedReader in = null;
    String result = "";
    CloseableHttpResponse response = null;
    InputStream inputStream = null;
    try {
      HttpPost post = new HttpPost(url);
      post.setEntity(new StringEntity(param, "UTF-8"));
      //Long id = requestId.get();
      //String reqId = Thread.currentThread().getName() + "-" + id;
      //requestId.set(id + 1);
      //post.addHeader("X-Request-ID", reqId);

      String encoding = Base64.getEncoder().encodeToString((usr + ":" + pwd).getBytes());
      post.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

      response = httpClient.execute(post);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IOException("Bad HTTP response received. code:" + response.getStatusLine().getStatusCode());
      }
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        // 定义BufferedReader输入流来读取URL的响应
        inputStream = entity.getContent();
        in = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = in.readLine()) != null) {
          result += line;
        }
      }
    } catch (Exception e) {
      throw e;
    }
    // 使用finally块来关闭输出流、输入流
    finally {
      release(response, inputStream, in);
    }
    return result;
  }

  public static String sendDelete(String url, String param) throws IOException {
    PrintWriter out = null;
    BufferedReader in = null;
    String result = "";
    try {
      URL realUrl = new URL(url);
      // 打开和URL之间的连接
      HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
      // 设置通用的请求属性
      conn.setRequestProperty("accept", "*/*");
      conn.setRequestProperty("connection", "Keep-Alive");
      conn.setRequestProperty(
          "user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestMethod("DELETE");
      // 获取URLConnection对象对应的输出流
      out = new PrintWriter(conn.getOutputStream());
      // 发送请求参数
      if (param != null) out.print(param);
      // flush输出流的缓冲
      out.flush();
      // 定义BufferedReader输入流来读取URL的响应
      in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        result += line;
      }
    } catch (Exception e) {
      throw e;
    }
    // 使用finally块来关闭输出流、输入流
    finally {
      close(out, in);
    }
    return result;
  }

  public static void close() throws IOException {
    if (httpClient != null) httpClient.close();
  }

  private static void close(PrintWriter out, BufferedReader in) throws IOException {
    try {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      throw e;
    }
  }

  private static void release(
      CloseableHttpResponse response, InputStream inputStream, BufferedReader reader) {
    if (reader != null) {
      try {
        reader.close();
      } catch (Throwable t) {
      }
    }
    boolean streamClosed = true;
    try {
      if (inputStream != null) {
        inputStream.close();
      } else {
        streamClosed = false;
      }
    } catch (IOException ioex) {
      streamClosed = false;
    } finally {
      if (!streamClosed && response != null) {
        try {
          response.close();
        } catch (IOException ioex) {
        }
      }
    }
  }
}
