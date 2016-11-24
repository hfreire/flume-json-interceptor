/*
 * Copyright (c) 2016, Hugo Freire <hugo@exec.sh>.
 *
 * This source code is licensed under the license found in the
 * LICENSE file in the root directory of this source tree.
 */

package sh.exec.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class JsonInterceptor implements Interceptor {
  private Set<String> extractHeaderProperties;
  private String extractBodyProperty;
  private ObjectMapper objectMapper;

  public JsonInterceptor(Set<String> extractHeaderProperties, String extractBodyProperty) {
    this.extractHeaderProperties = extractHeaderProperties;
    this.extractBodyProperty = extractBodyProperty;

    this.objectMapper = new ObjectMapper();
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    if (extractHeaderProperties.isEmpty() && extractBodyProperty == null) {
      return event;
    }

    String body = new String(event.getBody());

    if (body.length() == 0) {
      return event;
    }

    ObjectNode rootNode;
    try {
      rootNode = (ObjectNode) objectMapper.readTree(body);
    } catch (IOException e) {
      return event;
    }

    if (!extractHeaderProperties.isEmpty()) {
      Map<String, String> headers = event.getHeaders();

      for (String fullPath : extractHeaderProperties) {
        JsonNode node = getNodeByPath(rootNode, fullPath);
        if (node == null || node.isMissingNode()) {
          continue;
        }

        String propertyValue = valueToString(node);
        if (propertyValue != null) {
          String path;
          if (fullPath.contains(".")) {
            String[] paths = fullPath.split("\\.");
            path = paths[paths.length - 1];
          } else {
            path = fullPath;
          }
          headers.put(path, propertyValue);
        }
      }
    }

    if (extractBodyProperty != null) {
      JsonNode node = getNodeByPath(rootNode, extractBodyProperty);
      if (node != null && !node.isMissingNode()) {
        String propertyValue = valueToString(node);
        event.setBody(propertyValue.getBytes());
      }
    }

    return event;
  }

  private JsonNode getNodeByPath(JsonNode rootNode, String fullPath) {
    if (rootNode == null) {
      return null;
    }

    if (!fullPath.contains(".")) {
      return rootNode.path(fullPath);
    }

    String[] paths = fullPath.split("\\.");
    JsonNode node = rootNode;
    for (String path : paths) {
      node = node.path(path);
    }

    return node;
  }

  private String valueToString(JsonNode value) {
    if (value == null) {
      return null;
    }

    String valueString = null;

    if (value.isTextual()) {
      valueString = value.getTextValue();
    } else if (value.isNumber()) {
      JsonParser.NumberType numberType = value.getNumberType();
      switch (numberType) {
        case INT:
          valueString = String.valueOf(value.getIntValue());
          break;
        case LONG:
          valueString = String.valueOf(value.getLongValue());
          break;
        case BIG_INTEGER:
          valueString = String.valueOf(value.getBigIntegerValue());
          break;
        case FLOAT:
          break;
        case DOUBLE:
          valueString = String.valueOf(value.getDoubleValue());
          break;
        case BIG_DECIMAL:
          valueString = String.valueOf(value.getDecimalValue());
          break;
      }
    } else if (value.isBoolean()) {
      valueString = String.valueOf(value.getBooleanValue());
    }

    if (valueString == null) {
      valueString = value.toString();
    }

    return valueString;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> interceptedEvents = new ArrayList<>(events.size());
    for (Event event : events) {
      Event interceptedEvent = intercept(event);
      interceptedEvents.add(interceptedEvent);
    }

    return interceptedEvents;
  }

  @Override
  public void close() {
  }

  public static class Builder implements Interceptor.Builder {
    private Set<String> extractHeaderProperties = new HashSet<>();
    private String extractBodyProperty = null;

    @Override
    public void configure(Context context) {
      String extractHeaderProperties = context.getString("extractHeaderProperties");
      if (extractHeaderProperties != null) {
        StringTokenizer stringTokenizer = new StringTokenizer(extractHeaderProperties.replace(" ", ""), ",");
        while (stringTokenizer.hasMoreTokens()) {
          this.extractHeaderProperties.add(stringTokenizer.nextToken());
        }
      }

      extractBodyProperty = context.getString("extractBodyProperty");
    }

    @Override
    public Interceptor build() {
      return new JsonInterceptor(extractHeaderProperties, extractBodyProperty);
    }
  }
}
