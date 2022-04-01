package cn.edu.tsinghua.iotdb.benchmark.influxdb2;

import org.apache.http.HttpEntity;
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

public class HttpRequestUtil {
  private static PoolingHttpClientConnectionManager cm;
  private static CloseableHttpClient httpClient = null;

  public static void init() {
    cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(1024);
    cm.setDefaultMaxPerRoute(1024);
    httpClient =
        HttpClients.custom().setConnectionManager(cm).setConnectionManagerShared(true).build();
  }

  /**
   * Send Get Request to target URL
   *
   * @param url
   * @return
   */
  public static String sendGet(String url) throws Exception {
    StringBuffer result = new StringBuffer();
    BufferedReader in = null;
    InputStream inputStream = null;
    CloseableHttpResponse response = null;
    try {
      HttpGet get = new HttpGet(url);
      response = httpClient.execute(get);
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        inputStream = entity.getContent();
        in = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = in.readLine()) != null) {
          result.append(line);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      try {
        release(response, inputStream, in);
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
    return result.toString();
  }

  /**
   * Send Post request to target url
   *
   * @param url
   * @param body
   * @param contentType
   * @return
   */
  public static String sendPost(String url, String body, String contentType, String token)
      throws Exception {
    PrintWriter out = null;
    BufferedReader in = null;
    StringBuffer result = new StringBuffer();
    CloseableHttpResponse response = null;
    InputStream inputStream = null;
    try {
      HttpPost post = new HttpPost(url);
      if (body != null) {
        post.setEntity(new StringEntity(body, "UTF-8"));
      }
      post.setHeader("Content-Type", contentType);
      post.setHeader("Authorization", "Token " + token);
      response = httpClient.execute(post);
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        inputStream = entity.getContent();
        // define BufferReader to read response from url
        in = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = in.readLine()) != null) {
          result.append(line);
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      release(response, inputStream, in);
    }
    return result.toString();
  }

  public static void close() throws IOException {
    if (httpClient != null) httpClient.close();
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
