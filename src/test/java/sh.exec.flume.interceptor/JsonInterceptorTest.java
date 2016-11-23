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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonInterceptorTest {
  @Mock
  private Event event;

  @Mock
  private Context context;

  @Test
  public void shouldBuildJsonInterceptorInstance() {
    JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

    builder.configure(context);
    Interceptor interceptor = builder.build();

    assertThat(interceptor)
        .isNotNull()
        .isInstanceOf(JsonInterceptor.class);
  }

  @Test
  public void shouldBuildInterceptorWithContextProperties() {
    JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

    builder.configure(context);
    builder.build();

    verify(context).getString("extractHeaderProperties");
    verify(context).getString("extractBodyProperty");
  }

  @Test
  public void shouldExtractHeaderProperty() {
    String body = "{\"key1\":\"value1\"}";
    Map<String, String> headers = new HashMap<>();

    JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

    when(context.getString(eq("extractHeaderProperties")))
        .thenReturn("key1");

    builder.configure(context);
    Interceptor interceptor = builder.build();

    when(event.getBody())
        .thenReturn(body.getBytes());
    when(event.getHeaders())
        .thenReturn(headers);

    interceptor.intercept(event);

    assertThat(headers)
        .containsKeys("key1");
  }

  @Test
  public void shouldExtractMultipleHeaderProperties() {
    String body = "{\"key1\":\"value1\",\"key2\":false}";
    Map<String, String> headers = new HashMap<>();

    JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

    when(context.getString(eq("extractHeaderProperties")))
        .thenReturn("key1,key2");

    builder.configure(context);
    Interceptor interceptor = builder.build();

    when(event.getBody())
        .thenReturn(body.getBytes());
    when(event.getHeaders())
        .thenReturn(headers);

    interceptor.intercept(event);

    assertThat(headers)
        .containsKeys("key1", "key2")
        .containsValues("value1", "false");
  }

  @Test
  public void shouldExtractBodyProperties() {
    String body = "{\"key1\":{\"key2\":2}}";

    JsonInterceptor.Builder builder = new JsonInterceptor.Builder();

    when(context.getString(eq("extractBodyProperty")))
        .thenReturn("key1");

    builder.configure(context);
    Interceptor interceptor = builder.build();

    when(event.getBody())
        .thenReturn(body.getBytes());

    interceptor.intercept(event);

    verify(event).setBody(eq("{\"key2\":2}".getBytes()));
  }
}
