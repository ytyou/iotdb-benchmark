package cn.edu.tsinghua.iotdb.benchmark.victoriametrics;

import cn.edu.tsinghua.iotdb.benchmark.schema.enums.Type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class VictoriaMetricsModel implements Serializable {
  private static final long serialVersionUID = 1L;
  private String metric;
  private long timestamp;
  private Object value;
  private Type type;
  private Map<String, String> tags = new HashMap<String, String>();

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer(metric);
    result.append("{");
    if (tags != null) {
      boolean first = true;
      for (Map.Entry<String, String> pair : tags.entrySet()) {
        if (!first) {
          result.append(",");
        } else {
          first = false;
        }
        result.append(pair.getKey());
        result.append("=\"");
        result.append(pair.getValue());
        result.append("\"");
      }
    }
    result.append("} ");
    if (type == Type.BOOLEAN) {
      result.append((boolean) value ? 1 : 0);
    } else {
      result.append(value);
    }
    result.append(" ");
    result.append(timestamp);
    return result.toString();
  }
}
