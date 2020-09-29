/*
 * Copyright (c) 2011-Present VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.http.client;

import brave.test.http.ITHttpClient;
import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Mono;
import reactor.netty.ConnectionObserver;

import java.time.Duration;

public class ITBraveHttpClientTracingTest extends ITHttpClient<HttpClient> {

	@Override
	protected HttpClient newClient(int port) {
		ConnectionObserver observer = BraveHttpClientTracing.create(httpTracing, s -> null);

		return HttpClient.create()
		                 .host("127.0.0.1")
		                 .port(port)
		                 .wiretap(true)
		                 .followRedirect(true)
		                 .disableRetry(true)
		                 .observe(observer);
	}

	@Override
	protected void closeClient(HttpClient client) {
		// noop
	}

	@Override
	protected void options(HttpClient client, String path) {
		execute(client, HttpMethod.OPTIONS, path);
	}

	@Override
	protected void get(HttpClient client, String pathIncludingQuery) {
		execute(client, HttpMethod.GET, pathIncludingQuery);
	}

	@Override
	protected void post(HttpClient client, String pathIncludingQuery, String body) {
		execute(client, HttpMethod.POST, pathIncludingQuery, body);
	}

	void execute(HttpClient client, HttpMethod method, String pathIncludingQuery) {
		execute(client, method, pathIncludingQuery, null);
	}

	void execute(HttpClient client, HttpMethod method, String pathIncludingQuery, String body) {
		String uri = pathIncludingQuery.isEmpty() ? "/" : pathIncludingQuery;
		client.request(method)
		      .uri(uri)
		      .send((req, out) -> {
		          if (body != null) {
		              return out.sendString(Mono.just(body));
		          }
		          return out;
		      })
		      .responseContent()
		      .aggregate()
		      .block(Duration.ofSeconds(30));
	}
}
