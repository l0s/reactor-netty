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

import brave.Span;
import brave.http.HttpClientHandler;
import brave.http.HttpClientRequest;
import brave.http.HttpClientResponse;
import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static reactor.netty.http.client.HttpClientState.RESPONSE_INCOMPLETE;
import static reactor.netty.http.client.HttpClientState.RESPONSE_RECEIVED;

/**
 * Brave instrumentation for {@link HttpClient}.
 *
 * {@code
 *     ConnectionObserver observer = BraveHttpClientTracing.create(httpTracing);
 *
 *     HttpClient.create()
 *               .port(0)
 *               .observe(observer);
 * }
 *
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class BraveHttpClientTracing implements ConnectionObserver, MapConnect {

	/**
	 * Create a new {@link ConnectionObserver} using a preconfigured {@link HttpTracing} instance.
	 *
	 * @param httpTracing a preconfigured {@link HttpTracing} instance
	 * @return a new {@link ConnectionObserver}
	 */
	public static ConnectionObserver create(HttpTracing httpTracing) {
		return create(httpTracing, Function.identity());
	}

	/**
	 * Create a new {@link ConnectionObserver} using a preconfigured {@link HttpTracing} instance.
	 * <p>{@code uriMapping} function receives the actual uri and returns the target uri value
	 * that will be used for the tracing.
	 * For example instead of using the actual uri {@code "/users/1"} as uri value, templated uri
	 * {@code "/users/{id}"} can be used.
	 *
	 * @param httpTracing a preconfigured {@link HttpTracing} instance
	 * @param uriMapping a function that receives the actual uri and returns the target uri value
	 * that will be used for the tracing
	 * @return a new {@link ConnectionObserver}
	 */
	public static ConnectionObserver create(HttpTracing httpTracing, Function<String, String> uriMapping) {
		return new BraveHttpClientTracing(httpTracing, uriMapping);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onStateChange(Connection connection, State state) {
		if (connection instanceof HttpClientOperations) {
			HttpClientOperations ops = (HttpClientOperations) connection;
			TracingContext.of(ops.currentContext())
			              .ifPresent(tracingContext -> {
			                  HttpClientRequest request = new DelegatingHttpRequest(ops, uriMapping);
			                  if (state == HttpClientState.REQUEST_PREPARED) {
			                      TraceContext parent = ops.currentContext().getOrDefault(TraceContext.class, null);
			                      Span span = handler.handleSendWithParent(request, parent);
			                      SocketAddress remoteAddress = ops.channel().remoteAddress();
			                      if (remoteAddress instanceof InetSocketAddress) {
			                          InetSocketAddress address = (InetSocketAddress) remoteAddress;
			                          span.remoteIpAndPort(address.getHostString(), address.getPort());
			                      }
			                      Span oldSpan = ((AtomicReference<Span>) tracingContext).getAndSet(span);
			                      if (oldSpan != null) {
			                          oldSpan.abandon();
			                      }
			                  }
			                  else if (state == RESPONSE_RECEIVED) {
			                      handleReceive((AtomicReference<Span>) tracingContext, ops, request, null);
			                  }
			                  else if (state == RESPONSE_INCOMPLETE) {
			                      Throwable error = new PrematureCloseException("Connection prematurely closed DURING response");
			                      handleReceive((AtomicReference<Span>) tracingContext, ops, request, error);
			                  }
			              });
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onUncaughtException(Connection connection, Throwable error) {
		if (connection instanceof HttpClientOperations) {
			HttpClientOperations ops = (HttpClientOperations) connection;
			TracingContext.of(ops.currentContext())
			              .ifPresent(tracingContext -> {
			                  if (error instanceof RedirectClientException) {
			                      handleReceive((AtomicReference<Span>) tracingContext,
			                              ops, new DelegatingHttpRequest(ops, uriMapping), null);
			                  }
			                  else if (ops.retrying && ops.responseState == null) {
			                      handleReceive((AtomicReference<Span>) tracingContext,
			                              ops, new DelegatingHttpRequest(ops, uriMapping), error);
			                  }
			                  else if (ops.responseState != null) {
			                      handleReceive((AtomicReference<Span>) tracingContext,
			                              ops, new DelegatingHttpRequest(ops, uriMapping), error);
			                  }
			              });
		}
	}

	void handleReceive(AtomicReference<Span> tracingContext, HttpClientOperations ops, HttpClientRequest request, @Nullable Throwable throwable) {
		Span span = tracingContext.getAndSet(null);
		if (span != null) {
			handler.handleReceive(new DelegatingHttpResponse(ops, request, throwable), span);
		}
	}

	final CurrentTraceContext currentTraceContext;
	final HttpClientHandler<HttpClientRequest, HttpClientResponse> handler;
	final Function<String, String> uriMapping;

	BraveHttpClientTracing(HttpTracing httpTracing, Function<String, String> uriMapping) {
		requireNonNull(httpTracing, "httpTracing");
		this.currentTraceContext = httpTracing.tracing().currentTraceContext();
		this.handler = HttpClientHandler.create(httpTracing);
		this.uriMapping = requireNonNull(uriMapping, "uriMapping");
	}

	@Override
	public Mono<? extends Connection> apply(Mono<? extends Connection> connectionMono) {
		BraveHttpClientMapConnect  mapConnect = new BraveHttpClientMapConnect();
		return connectionMono.doOnError(error -> {
		                         Span span = mapConnect.getAndSet(null);
		                         if (span != null) {
		                             span.error(error).finish();
		                         }
		                     })
		                     .doOnCancel(() -> {
		                         Span span = mapConnect.getAndSet(null);
		                         if (span != null) {
		                             span.annotate("cancel").finish();
		                         }
		                     })
		                     .contextWrite(ctx -> {
		                         TraceContext invocationContext = currentTraceContext.get();
		                         if (invocationContext != null) {
		                             ctx = ctx.put(TraceContext.class, invocationContext);
		                         }
		                         return ctx.put(KEY_TRACING_CONTEXT, mapConnect);
		                     });
	}

	static final String KEY_TRACING_CONTEXT = "reactor.netty.http.client.TracingContext";

	static final class BraveHttpClientMapConnect extends AtomicReference<Span> implements TracingContext {

		@Override
		public TracingContext span(Consumer<Span> consumer) {
			requireNonNull(consumer, "consumer");
			Span span = get();
			if (span != null) {
				consumer.accept(span);
			}
			return this;
		}
	}

	static final class DelegatingHttpRequest extends HttpClientRequest {

		final reactor.netty.http.client.HttpClientRequest delegate;
		final Function<String, String> uriMapping;

		DelegatingHttpRequest(reactor.netty.http.client.HttpClientRequest delegate, Function<String, String> uriMapping) {
			this.delegate = delegate;
			this.uriMapping = uriMapping;
		}

		@Override
		@Nullable
		public String header(String name) {
			requireNonNull(name, "name");
			return delegate.requestHeaders().get(name);
		}

		@Override
		public void header(String name, String value) {
			requireNonNull(name, "name");
			requireNonNull(value, "value");
			delegate.header(name, value);
		}

		@Override
		public String method() {
			return delegate.method().name();
		}

		@Override
		public String path() {
			return delegate.fullPath();
		}

		@Override
		public String route() {
			return uriMapping.apply(path());
		}

		@Override
		@Nullable
		public Object unwrap() {
			if (delegate instanceof HttpClientOperations) {
				return ((HttpClientOperations) delegate).nettyRequest;
			}
			return null;
		}

		@Override
		@Nullable
		public String url() {
			return delegate.resourceUrl();
		}
	}

	static final class DelegatingHttpResponse extends HttpClientResponse {

		final HttpClientOperations delegate;
		final Throwable error;
		final HttpClientRequest request;

		DelegatingHttpResponse(HttpClientOperations delegate, HttpClientRequest request, @Nullable Throwable error) {
			this.delegate = delegate;
			this.request = request;
			this.error = error;
		}

		@Override
		public int statusCode() {
			HttpClientOperations.ResponseState responseState = delegate.responseState;
			if (responseState != null) {
				return responseState.response.status().code();
			}
			return 0;
		}

		@Override
		@Nullable
		public Object unwrap() {
			HttpClientOperations.ResponseState responseState = delegate.responseState;
			if (responseState != null) {
				return responseState.response;
			}
			return null;
		}

		@Override
		public HttpClientRequest request() {
			return request;
		}

		@Override
		@Nullable
		public Throwable error() {
			return error;
		}
	}
}