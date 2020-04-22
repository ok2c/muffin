/*
 * Copyright 2021, OK2 Consulting Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ok2c.muffin.client

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.ok2c.hc5.json.http.JsonObjectEntityConsumer
import com.ok2c.hc5.json.http.JsonObjectEntityProducer
import com.ok2c.muffin.LoggingConnPoolListener
import com.ok2c.muffin.LoggingExceptionCallback
import com.ok2c.muffin.LoggingH2StreamListener
import com.ok2c.muffin.LoggingHttp1StreamListener
import com.ok2c.muffin.LoggingIOSessionDecorator
import com.ok2c.muffin.LoggingIOSessionListener
import com.ok2c.muffin.message.Request
import com.ok2c.muffin.message.Response
import org.apache.hc.core5.concurrent.CancellableDependency
import org.apache.hc.core5.concurrent.ComplexFuture
import org.apache.hc.core5.concurrent.DefaultThreadFactory
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.function.Resolver
import org.apache.hc.core5.http.ConnectionClosedException
import org.apache.hc.core5.http.EntityDetails
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.ProtocolException
import org.apache.hc.core5.http.config.CharCodingConfig
import org.apache.hc.core5.http.config.Http1Config
import org.apache.hc.core5.http.impl.DefaultAddressResolver
import org.apache.hc.core5.http.impl.DefaultConnectionReuseStrategy
import org.apache.hc.core5.http.impl.DefaultContentLengthStrategy
import org.apache.hc.core5.http.impl.HttpProcessors
import org.apache.hc.core5.http.impl.nio.ClientHttp1StreamDuplexerFactory
import org.apache.hc.core5.http.impl.nio.DefaultHttpRequestWriterFactory
import org.apache.hc.core5.http.impl.nio.DefaultHttpResponseParserFactory
import org.apache.hc.core5.http.nio.AsyncClientExchangeHandler
import org.apache.hc.core5.http.nio.AsyncEntityConsumer
import org.apache.hc.core5.http.nio.AsyncEntityProducer
import org.apache.hc.core5.http.nio.AsyncPushConsumer
import org.apache.hc.core5.http.nio.HandlerFactory
import org.apache.hc.core5.http.nio.command.RequestExecutionCommand
import org.apache.hc.core5.http.nio.command.ShutdownCommand
import org.apache.hc.core5.http.nio.entity.AsyncEntityProducers
import org.apache.hc.core5.http.nio.entity.StringAsyncEntityConsumer
import org.apache.hc.core5.http.nio.support.BasicClientExchangeHandler
import org.apache.hc.core5.http.protocol.HttpContext
import org.apache.hc.core5.http.protocol.HttpCoreContext
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.apache.hc.core5.http2.config.H2Config
import org.apache.hc.core5.http2.impl.H2Processors
import org.apache.hc.core5.http2.impl.nio.ClientH2StreamMultiplexerFactory
import org.apache.hc.core5.http2.impl.nio.ClientHttpProtocolNegotiatorFactory
import org.apache.hc.core5.http2.ssl.H2ClientTlsStrategy
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.pool.ConnPoolControl
import org.apache.hc.core5.pool.DefaultDisposalCallback
import org.apache.hc.core5.pool.ManagedConnPool
import org.apache.hc.core5.pool.PoolEntry
import org.apache.hc.core5.pool.PoolReusePolicy
import org.apache.hc.core5.pool.StrictConnPool
import org.apache.hc.core5.reactor.Command
import org.apache.hc.core5.reactor.DefaultConnectingIOReactor
import org.apache.hc.core5.reactor.IOReactorConfig
import org.apache.hc.core5.reactor.IOReactorService
import org.apache.hc.core5.reactor.IOSession
import org.apache.hc.core5.ssl.SSLContexts
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.SSLContext

class ClientEndpoint internal constructor(
    poolEntry: PoolEntry<HttpHost, IOSession?>,
    connPool: ManagedConnPool<HttpHost, IOSession>,
    objectMapper: ObjectMapper) {

    private val poolEntryRef = AtomicReference(poolEntry)
    private val connPool = connPool
    private val objectMapper = objectMapper

    fun execute(
        exchangeHandler: AsyncClientExchangeHandler,
        pushHandlerFactory: HandlerFactory<AsyncPushConsumer>?,
        cancellableDependency: CancellableDependency?,
        context: HttpContext
    ) {
        val poolEntry = poolEntryRef.get() ?: throw IllegalStateException("Endpoint has already been released")
        val ioSession = poolEntry.connection ?: throw IllegalStateException("I/O session is invalid")
        ioSession.enqueue(
            RequestExecutionCommand(exchangeHandler, pushHandlerFactory, cancellableDependency, context),
            Command.Priority.NORMAL
        )
        if (!ioSession.isOpen) {
            try {
                exchangeHandler.failed(ConnectionClosedException())
            } finally {
                exchangeHandler.releaseResources()
            }
        }
    }

    fun <I, O> execute(
        request: Request<I>,
        supplyEntityProducer: (entity: I) -> AsyncEntityProducer,
        supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<O>,
        resultCallback: FutureCallback<Response<O>>?
    ): Future<Response<O>> {
        val poolEntry = poolEntryRef.get()
        val future = ComplexFuture(resultCallback)
        execute(
            BasicClientExchangeHandler(
                KotRequestProducer(poolEntry?.route!!, request, supplyEntityProducer),
                KotResponseConsumer(supplyEntityConsumer), object : FutureCallback<Response<O>> {

                    override fun completed(result: Response<O>) {
                        future.completed(result)
                    }

                    override fun cancelled() {
                        future.cancel()
                    }

                    override fun failed(ex: Exception) {
                        future.failed(ex)
                    }

                }), null, future, HttpCoreContext.create()
        )
        return future
    }

    fun execute(
        request: Request<String>,
        resultCallback: FutureCallback<Response<String>>?
    ): Future<Response<String>> {
        return execute(
            request,
            { s -> AsyncEntityProducers.create(s) },
            { StringAsyncEntityConsumer() },
            resultCallback)
    }

    fun <I, O> executeJson(
        request: Request<I>,
        responseDataClass: Class<O>,
        resultCallback: FutureCallback<Response<O>>?
    ): Future<Response<O>> {
        return execute(
            request,
            { obj -> JsonObjectEntityProducer(obj, objectMapper ) },
            { JsonObjectEntityConsumer(objectMapper, responseDataClass) },
            resultCallback)
    }

    fun isConnected(): Boolean {
        val poolEntry = poolEntryRef.get()
        if (poolEntry != null) {
            val ioSession = poolEntry.connection
            if (ioSession != null && ioSession.isOpen) {
                return true
            }
        }
        return false
    }

    fun releaseAndReuse() {
        val poolEntry = poolEntryRef.getAndSet(null)
        if (poolEntry != null) {
            val ioSession = poolEntry.connection
            connPool.release(poolEntry, ioSession != null && ioSession.isOpen)
        }
    }

    fun releaseAndDiscard(closeMode: CloseMode) {
        val poolEntry = poolEntryRef.getAndSet(null)
        if (poolEntry != null) {
            poolEntry.discardConnection(closeMode)
            connPool.release(poolEntry, false)
        }
    }

}

class Client internal constructor(
    private val ioReactor: DefaultConnectingIOReactor,
    private val connPool: ManagedConnPool<HttpHost, IOSession>,
    private val addressResolver: Resolver<HttpHost, InetSocketAddress>,
    private val defaultTimeout: Timeout,
    private val objectMapper: ObjectMapper
) : IOReactorService by ioReactor, ConnPoolControl<HttpHost> by connPool {

    private fun connectSession(
        host: HttpHost,
        localAddress: SocketAddress?,
        timeout: Timeout,
        attachment: Any?,
        callback: FutureCallback<IOSession>?
    ): Future<IOSession> {
        return ioReactor.connect(host, addressResolver.resolve(host), localAddress, timeout, attachment, callback)
    }

    fun connect(
        host: HttpHost,
        timeout: Timeout,
        attachment: Any?,
        callback: FutureCallback<ClientEndpoint>?
    ): Future<ClientEndpoint> {
        val resultFuture = ComplexFuture(callback)
        val leaseFuture = connPool.lease(
            host, null, timeout, object : FutureCallback<PoolEntry<HttpHost, IOSession?>> {

                override fun completed(poolEntry: PoolEntry<HttpHost, IOSession?>) {
                    val endpoint = ClientEndpoint(poolEntry, connPool, objectMapper)
                    val ioSession = poolEntry.connection
                    if (ioSession != null && !ioSession.isOpen) {
                        poolEntry.discardConnection(CloseMode.IMMEDIATE)
                    }
                    if (poolEntry.hasConnection()) {
                        resultFuture.completed(endpoint)
                    } else {
                        val future: Future<IOSession> = connectSession(
                            host, null, timeout, attachment, object : FutureCallback<IOSession> {

                                override fun completed(session: IOSession) {
                                    session.socketTimeout = timeout
                                    poolEntry.assignConnection(session)
                                    resultFuture.completed(endpoint)
                                }

                                override fun failed(cause: Exception) {
                                    try {
                                        resultFuture.failed(cause)
                                    } finally {
                                        endpoint.releaseAndDiscard(CloseMode.IMMEDIATE)
                                    }
                                }

                                override fun cancelled() {
                                    try {
                                        resultFuture.cancel()
                                    } finally {
                                        endpoint.releaseAndDiscard(CloseMode.IMMEDIATE)
                                    }
                                }

                            })
                        resultFuture.setDependency(future)
                    }
                }

                override fun failed(ex: Exception) {
                    resultFuture.failed(ex)
                }

                override fun cancelled() {
                    resultFuture.cancel()
                }

            })
        resultFuture.setDependency(leaseFuture)
        return resultFuture
    }

    fun <I, O> execute(
        host: HttpHost,
        request: Request<I>,
        supplyEntityProducer: (entity: I) -> AsyncEntityProducer,
        supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<O>,
        resultCallback: FutureCallback<Response<O>>?
    ): Future<Response<O>> {
        val resultFuture = ComplexFuture(resultCallback)
        val connectFuture = connect(host, defaultTimeout, null, object : FutureCallback<ClientEndpoint> {

            override fun completed(endpoint: ClientEndpoint) {
                val executeFuture = endpoint.execute(
                    request,
                    supplyEntityProducer,
                    supplyEntityConsumer,
                    object : FutureCallback<Response<O>> {

                        override fun completed(result: Response<O>) {
                            endpoint.releaseAndReuse()
                            resultFuture.completed(result)
                        }

                        override fun failed(ex: Exception) {
                            endpoint.releaseAndDiscard(CloseMode.IMMEDIATE)
                            resultFuture.failed(ex)
                        }

                        override fun cancelled() {
                            endpoint.releaseAndDiscard(CloseMode.IMMEDIATE)
                            resultFuture.cancel()
                        }

                    })
                resultFuture.setDependency(executeFuture)
            }

            override fun failed(ex: Exception) {
                resultFuture.failed(ex)
            }

            override fun cancelled() {
                resultFuture.cancel()
            }

        })
        resultFuture.setDependency(connectFuture)
        return resultFuture
    }

    fun execute(
        host: HttpHost,
        request: Request<String>,
        resultCallback: FutureCallback<Response<String>>?
    ): Future<Response<String>> {
        return execute(
            host,
            request,
            { s -> AsyncEntityProducers.create(s) },
            { StringAsyncEntityConsumer() },
            resultCallback)
    }

    fun <I, O> execute(
        request: Request<I>,
        supplyEntityProducer: (entity: I) -> AsyncEntityProducer,
        supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<O>,
        resultCallback: FutureCallback<Response<O>>?
    ): Future<Response<O>> {
        val uri = request.uri
        if (!uri.isAbsolute) {
            throw ProtocolException("Request URI must be absolute")
        }
        val host = HttpHost(uri.scheme, uri.host, uri.port)
        return execute(host, request, supplyEntityProducer, supplyEntityConsumer, resultCallback)
    }

    fun execute(
        request: Request<String>,
        resultCallback: FutureCallback<Response<String>>?
    ): Future<Response<String>> {
        return execute(
            request,
            { s -> AsyncEntityProducers.create(s) },
            { StringAsyncEntityConsumer() },
            resultCallback)
    }

    fun <I, O> executeJson(
        request: Request<I>,
        responseDataClass: Class<O>,
        resultCallback: FutureCallback<Response<O>>?
    ): Future<Response<O>> {
        return execute(
            request,
            { obj -> JsonObjectEntityProducer(obj, objectMapper ) },
            { JsonObjectEntityConsumer(objectMapper, responseDataClass) },
            resultCallback)
    }

}

class ClientBootstrap {

    var ioReactorConfig: IOReactorConfig? = null
    var http1Config: Http1Config? = null
    var h2Config: H2Config? = null
    var charCodingConfig: CharCodingConfig? = null
    var sslContext: SSLContext? = null
    var jsonFactory: JsonFactory? = null

    fun create(): Client {
        val http1StreamHandlerFactory = ClientHttp1StreamDuplexerFactory(
            HttpProcessors.client(),
            if (http1Config != null) http1Config else Http1Config.DEFAULT,
            if (charCodingConfig != null) charCodingConfig else CharCodingConfig.DEFAULT,
            DefaultConnectionReuseStrategy.INSTANCE,
            DefaultHttpResponseParserFactory.INSTANCE,
            DefaultHttpRequestWriterFactory.INSTANCE,
            DefaultContentLengthStrategy.INSTANCE,
            DefaultContentLengthStrategy.INSTANCE,
            LoggingHttp1StreamListener.INSTANCE_CLIENT
        )
        val h2StreamHandlerFactory = ClientH2StreamMultiplexerFactory(
            H2Processors.client(),
            null,
            if (h2Config != null) h2Config else H2Config.DEFAULT,
            if (charCodingConfig != null) charCodingConfig else CharCodingConfig.DEFAULT,
            LoggingH2StreamListener.INSTANCE
        )
        val ioEventHandlerFactory = ClientHttpProtocolNegotiatorFactory(
            http1StreamHandlerFactory,
            h2StreamHandlerFactory,
            HttpVersionPolicy.NEGOTIATE,
            H2ClientTlsStrategy(if (sslContext != null) sslContext else SSLContexts.createSystemDefault()),
            ioReactorConfig?.soTimeout
        )
        val connPool = StrictConnPool<HttpHost, IOSession>(
            20,
            50,
            TimeValue.ofMinutes(1),
            PoolReusePolicy.LIFO,
            DefaultDisposalCallback(),
            LoggingConnPoolListener.INSTANCE
        )

        val ioReactor = DefaultConnectingIOReactor(
            ioEventHandlerFactory,
            ioReactorConfig,
            DefaultThreadFactory("muffin-client", true),
            LoggingIOSessionDecorator.INSTANCE,
            LoggingExceptionCallback.INSTANCE,
            LoggingIOSessionListener.INSTANCE,
            ShutdownCommand.GRACEFUL_IMMEDIATE_CALLBACK
        )

        val defaultTimeout = ioReactorConfig?.soTimeout ?: Timeout.ofMinutes(1)

        val objectMapper = ObjectMapper(jsonFactory)

        return Client(ioReactor, connPool, DefaultAddressResolver.INSTANCE, defaultTimeout, objectMapper)
    }

}