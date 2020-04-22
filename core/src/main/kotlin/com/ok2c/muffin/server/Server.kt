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
package com.ok2c.muffin.server

import com.ok2c.muffin.LoggingExceptionCallback
import com.ok2c.muffin.LoggingH2StreamListener
import com.ok2c.muffin.LoggingHttp1StreamListener
import com.ok2c.muffin.LoggingIOSessionDecorator
import com.ok2c.muffin.LoggingIOSessionListener
import com.ok2c.muffin.message.Request
import org.apache.hc.core5.concurrent.DefaultThreadFactory
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.function.Supplier
import org.apache.hc.core5.http.EntityDetails
import org.apache.hc.core5.http.config.CharCodingConfig
import org.apache.hc.core5.http.config.Http1Config
import org.apache.hc.core5.http.config.NamedElementChain
import org.apache.hc.core5.http.impl.DefaultConnectionReuseStrategy
import org.apache.hc.core5.http.impl.DefaultContentLengthStrategy
import org.apache.hc.core5.http.impl.HttpProcessors
import org.apache.hc.core5.http.impl.bootstrap.StandardFilter
import org.apache.hc.core5.http.impl.nio.DefaultHttpRequestParserFactory
import org.apache.hc.core5.http.impl.nio.DefaultHttpResponseWriterFactory
import org.apache.hc.core5.http.impl.nio.ServerHttp1StreamDuplexerFactory
import org.apache.hc.core5.http.nio.AsyncEntityConsumer
import org.apache.hc.core5.http.nio.AsyncEntityProducer
import org.apache.hc.core5.http.nio.AsyncFilterHandler
import org.apache.hc.core5.http.nio.AsyncServerExchangeHandler
import org.apache.hc.core5.http.nio.command.ShutdownCommand
import org.apache.hc.core5.http.nio.entity.AsyncEntityProducers
import org.apache.hc.core5.http.nio.entity.StringAsyncEntityConsumer
import org.apache.hc.core5.http.nio.support.AsyncServerExpectationFilter
import org.apache.hc.core5.http.nio.support.AsyncServerFilterChainElement
import org.apache.hc.core5.http.nio.support.AsyncServerFilterChainExchangeHandlerFactory
import org.apache.hc.core5.http.nio.support.DefaultAsyncResponseExchangeHandlerFactory
import org.apache.hc.core5.http.nio.support.TerminalAsyncServerFilter
import org.apache.hc.core5.http.protocol.RequestHandlerRegistry
import org.apache.hc.core5.http.protocol.UriPatternType
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.apache.hc.core5.http2.config.H2Config
import org.apache.hc.core5.http2.impl.H2Processors
import org.apache.hc.core5.http2.impl.nio.ServerH2StreamMultiplexerFactory
import org.apache.hc.core5.http2.impl.nio.ServerHttpProtocolNegotiatorFactory
import org.apache.hc.core5.http2.ssl.H2ServerTlsStrategy
import org.apache.hc.core5.net.InetAddressUtils
import org.apache.hc.core5.reactor.DefaultListeningIOReactor
import org.apache.hc.core5.reactor.IOReactorConfig
import org.apache.hc.core5.reactor.IOReactorService
import org.apache.hc.core5.reactor.ListenerEndpoint
import org.apache.hc.core5.ssl.SSLContexts
import java.net.SocketAddress
import java.util.concurrent.Future
import javax.net.ssl.SSLContext

class Server internal constructor(
    private val ioReactor: DefaultListeningIOReactor
) : IOReactorService by ioReactor {

    fun listen(address: SocketAddress, callback: FutureCallback<ListenerEndpoint?>?): Future<ListenerEndpoint> {
        return ioReactor.listen(address, callback)
    }

    fun listen(address: SocketAddress): Future<ListenerEndpoint> {
        return ioReactor.listen(address, null)
    }

}

class ServerBootstrap {

    var canonicalHostName: String? = null
    var ioReactorConfig: IOReactorConfig? = null
    var http1Config: Http1Config? = null
    var h2Config: H2Config? = null
    var charCodingConfig: CharCodingConfig? = null
    var sslContext: SSLContext? = null
    private val handlerList: MutableList<HandlerEntry<Supplier<AsyncServerExchangeHandler>>> = mutableListOf()

    fun registerHandler(hostname: String, uriPattern: String, supplier: Supplier<AsyncServerExchangeHandler>) {
        handlerList.add(HandlerEntry(hostname, uriPattern, supplier))
    }

    fun registerHandler(uriPattern: String, supplier: Supplier<AsyncServerExchangeHandler>) {
        handlerList.add(HandlerEntry(null, uriPattern, supplier))
    }

    fun <I, O> register(
        uriPattern: String,
        supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<I>,
        handler: (request: Request<I>, responseTrigger: ResponseTrigger<O>) -> Unit,
        supplyEntityProducer: (entity: O) -> AsyncEntityProducer
    ) {
        registerHandler(uriPattern,
            Supplier { KotServerExchangeHandler(supplyEntityConsumer, handler, supplyEntityProducer) })
    }

    fun <I, O> register(
        hostname: String,
        uriPattern: String,
        supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<I>,
        handler: (request: Request<I>, responseTrigger: ResponseTrigger<O>) -> Unit,
        supplyEntityProducer: (entity: O) -> AsyncEntityProducer
    ) {
        registerHandler(hostname,
            uriPattern,
            Supplier { KotServerExchangeHandler(supplyEntityConsumer, handler, supplyEntityProducer) })
    }

    fun register(
        uriPattern: String,
        handler: (request: Request<String>, responseTrigger: ResponseTrigger<String>) -> Unit
    ) {
        registerHandler(
            uriPattern,
            Supplier {
                KotServerExchangeHandler(
                    { StringAsyncEntityConsumer() },
                    handler,
                    { s -> AsyncEntityProducers.create(s) })
            })
    }

    fun register(
        hostname: String,
        uriPattern: String,
        handler: (request: Request<String>, responseTrigger: ResponseTrigger<String>) -> Unit
    ) {
        registerHandler(hostname,
            uriPattern,
            Supplier {
                KotServerExchangeHandler(
                    { StringAsyncEntityConsumer() },
                    handler,
                    { s -> AsyncEntityProducers.create(s) })
            })
    }

    fun create(): Server {
        val registry = RequestHandlerRegistry<Supplier<AsyncServerExchangeHandler>>(
            if (canonicalHostName != null) canonicalHostName else InetAddressUtils.getCanonicalLocalHostName(),
            UriPatternType.URI_PATTERN
        )
        for (entry in handlerList) {
            registry.register(entry.hostname, entry.uriPattern, entry.handler)
        }
        val filterChainDefinition = NamedElementChain<AsyncFilterHandler>()
        val exchangeHandlerFactory = DefaultAsyncResponseExchangeHandlerFactory(registry)
        filterChainDefinition.addLast(
            TerminalAsyncServerFilter(exchangeHandlerFactory), StandardFilter.MAIN_HANDLER.name
        )
        filterChainDefinition.addFirst(
            AsyncServerExpectationFilter(), StandardFilter.EXPECT_CONTINUE.name
        )

        var execChain: AsyncServerFilterChainElement? = null
        var current = filterChainDefinition.last
        while (current != null) {
            execChain = AsyncServerFilterChainElement(current.getValue(), execChain)
            current = current.previous
        }
        val handlerFactory = AsyncServerFilterChainExchangeHandlerFactory(
            execChain!!,
            LoggingExceptionCallback.INSTANCE
        )

        val http1StreamHandlerFactory = ServerHttp1StreamDuplexerFactory(
            HttpProcessors.server(),
            handlerFactory,
            if (http1Config != null) http1Config else Http1Config.DEFAULT,
            if (charCodingConfig != null) charCodingConfig else CharCodingConfig.DEFAULT,
            DefaultConnectionReuseStrategy.INSTANCE,
            DefaultHttpRequestParserFactory.INSTANCE,
            DefaultHttpResponseWriterFactory.INSTANCE,
            DefaultContentLengthStrategy.INSTANCE,
            DefaultContentLengthStrategy.INSTANCE,
            LoggingHttp1StreamListener.INSTANCE_SERVER
        )
        val h2StreamHandlerFactory = ServerH2StreamMultiplexerFactory(
            H2Processors.server(),
            handlerFactory,
            if (h2Config != null) h2Config else H2Config.DEFAULT,
            if (charCodingConfig != null) charCodingConfig else CharCodingConfig.DEFAULT,
            LoggingH2StreamListener.INSTANCE
        )
        val ioEventHandlerFactory = ServerHttpProtocolNegotiatorFactory(
            http1StreamHandlerFactory,
            h2StreamHandlerFactory,
            HttpVersionPolicy.NEGOTIATE,
            H2ServerTlsStrategy(if (sslContext != null) sslContext else SSLContexts.createSystemDefault()),
            ioReactorConfig?.soTimeout
        )

        val ioReactor = DefaultListeningIOReactor(
            ioEventHandlerFactory,
            ioReactorConfig,
            DefaultThreadFactory("muffin-server", true),
            DefaultThreadFactory("muffin-listener", true),
            LoggingIOSessionDecorator.INSTANCE,
            LoggingExceptionCallback.INSTANCE,
            LoggingIOSessionListener.INSTANCE,
            ShutdownCommand.GRACEFUL_IMMEDIATE_CALLBACK
        )

        return Server(ioReactor)
    }

}

private class HandlerEntry<T>(val hostname: String?, val uriPattern: String, val handler: T)
