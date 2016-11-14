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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class JsonInterceptor implements Interceptor {

  private Set<String> extractProperties;
  private ObjectMapper objectMapper;

  public JsonInterceptor(Set<String> extractProperties) {
    this.extractProperties = extractProperties;

    this.objectMapper = new ObjectMapper();
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    if (extractProperties.isEmpty()) {
      return event;
    }

    String body = new String(event.getBody());

    if (body.length() == 0) {
      return event;
    }

    Map<String, String> headers = event.getHeaders();

    ObjectNode bodyNode;
    try {
      bodyNode = (ObjectNode) objectMapper.readTree(body);
    } catch (IOException e) {
      return event;
    }

    Iterator<Map.Entry<String, JsonNode>> iterator = bodyNode.getFields();
    while(iterator.hasNext()) {
      Map.Entry<String, JsonNode> next = iterator.next();
      String propertyName = next.getKey();

      if (!extractProperties.contains(propertyName)) {
        continue;
      }

      JsonNode value = next.getValue();
      String propertyValue = null;

      if (value.isTextual()) {
        propertyValue = next.getValue().getTextValue();
      } else if (value.isNumber()) {
        JsonParser.NumberType numberType = value.getNumberType();
        switch (numberType) {
          case INT:
            propertyValue = String.valueOf(value.getIntValue());
            break;
          case LONG:
            propertyValue = String.valueOf(value.getLongValue());
            break;
          case BIG_INTEGER:
            propertyValue = String.valueOf(value.getBigIntegerValue());
            break;
          case FLOAT:
            break;
          case DOUBLE:
            propertyValue = String.valueOf(value.getDoubleValue());
            break;
          case BIG_DECIMAL:
            propertyValue = String.valueOf(value.getDecimalValue());
            break;
        }
      } else if (value.isBoolean()) {
        propertyValue = String.valueOf(value.getBooleanValue());
      }

      if (propertyValue == null) {
        continue;
      }

      headers.put(propertyName, propertyValue);
    }

    return event;
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

    private Set<String> extractProperties = new HashSet<>();

    @Override
    public void configure(Context context) {
      String extractProperties = context.getString("extractProperties");
      StringTokenizer stringTokenizer = new StringTokenizer(extractProperties, ",");
      while(stringTokenizer.hasMoreTokens()) {
        this.extractProperties.add(stringTokenizer.nextToken());
      }
    }

    @Override
    public Interceptor build() {
      return new JsonInterceptor(extractProperties);
    }
  }
}
