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

import com.ok2c.muffin.message.Request
import com.ok2c.muffin.message.Response
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.http.EntityDetails
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpRequest
import org.apache.hc.core5.http.message.BasicHttpResponse
import org.apache.hc.core5.http.nio.AsyncEntityConsumer
import org.apache.hc.core5.http.nio.AsyncEntityProducer
import org.apache.hc.core5.http.nio.AsyncRequestConsumer
import org.apache.hc.core5.http.nio.AsyncResponseProducer
import org.apache.hc.core5.http.nio.AsyncServerExchangeHandler
import org.apache.hc.core5.http.nio.CapacityChannel
import org.apache.hc.core5.http.nio.DataStreamChannel
import org.apache.hc.core5.http.nio.ResponseChannel
import org.apache.hc.core5.http.protocol.HttpContext
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

internal class KotRequestConsumer<T>(
    private val supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<T>
) :
    AsyncRequestConsumer<Request<T>> {

    private val builder = Request.Builder<T>()
    private val entityConsumerRef = AtomicReference<AsyncEntityConsumer<T>>()

    override fun consumeRequest(
        request: HttpRequest,
        entityDetails: EntityDetails?,
        context: HttpContext,
        resultCallback: FutureCallback<Request<T>>
    ) {
        with(builder) {
            method = request.method
            uri = request.uri
            for (header in request.headerIterator()) {
                addHeader(header)
            }
        }
        if (entityDetails != null) {
            val entityConsumer = supplyEntityConsumer(entityDetails)
            entityConsumerRef.set(entityConsumer)
            entityConsumer.streamStart(entityDetails, object : FutureCallback<T> {

                override fun completed(result: T) {
                    builder.entity = result
                    resultCallback.completed(builder.build())
                }

                override fun failed(ex: Exception) {
                    resultCallback.failed(ex)
                }

                override fun cancelled() {
                    resultCallback.cancelled()
                }

            })
        } else {
            resultCallback.completed(builder.build())
        }
    }

    override fun updateCapacity(capacityChannel: CapacityChannel) {
        val entityConsumer = entityConsumerRef.get()
        entityConsumer?.updateCapacity(capacityChannel) ?: capacityChannel.update(Int.MAX_VALUE)
    }

    override fun consume(src: ByteBuffer) {
        val entityConsumer = entityConsumerRef.get()
        entityConsumer?.consume(src)
    }

    override fun streamEnd(trailers: MutableList<out Header>?) {
        val entityConsumer = entityConsumerRef.get()
        entityConsumer?.streamEnd(trailers)
    }

    override fun failed(cause: Exception) {
        val entityConsumer = entityConsumerRef.get()
        entityConsumer?.failed(cause)
    }

    override fun releaseResources() {
        val entityConsumer = entityConsumerRef.getAndSet(null)
        entityConsumer?.releaseResources()
    }

}

internal class KotResponseProducer<T>(
    private val response: Response<T>,
    private val supplyEntityProducer: (entity: T) -> AsyncEntityProducer
) : AsyncResponseProducer {

    private val entityProducerRef = AtomicReference<AsyncEntityProducer>()

    override fun sendResponse(channel: ResponseChannel, context: HttpContext) {
        val httpResponse = BasicHttpResponse(response.status)
        for (header in response.headers) {
            httpResponse.addHeader(header)
        }
        val entity = response.entity
        val entityProducer = if (entity != null) supplyEntityProducer(entity) else null
        entityProducerRef.set(entityProducer)
        channel.sendResponse(httpResponse, entityProducer, context)
    }

    override fun available(): Int {
        val entityProducer = entityProducerRef.get()
        return entityProducer?.available() ?: 0
    }

    override fun produce(channel: DataStreamChannel) {
        val entityProducer = entityProducerRef.get()
        entityProducer?.produce(channel)
    }

    override fun failed(cause: Exception) {
        val entityProducer = entityProducerRef.get()
        entityProducer?.failed(cause)
    }

    override fun releaseResources() {
        val entityProducer = entityProducerRef.getAndSet(null)
        entityProducer?.releaseResources()
    }

}

interface ResponseTrigger<T> {

    fun submitResponse(response: Response<T>)

}

internal class KotServerExchangeHandler<I, O>(
    supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<I>,
    private val handler: (request: Request<I>, responseTrigger: ResponseTrigger<O>) -> Unit,
    private val supplyEntityProducer: (entity: O) -> AsyncEntityProducer
) : AsyncServerExchangeHandler {

    private val requestConsumer = KotRequestConsumer(supplyEntityConsumer)
    private val responseProducerRef: AtomicReference<AsyncResponseProducer> = AtomicReference()

    override fun handleRequest(
        request: HttpRequest,
        entityDetails: EntityDetails?,
        responseChannel: ResponseChannel,
        context: HttpContext
    ) {
        requestConsumer.consumeRequest(request, entityDetails, context, object : FutureCallback<Request<I>> {

            override fun completed(request: Request<I>) {
                try {
                    handler(request, object : ResponseTrigger<O> {

                        override fun submitResponse(response: Response<O>) {
                            val responseProducer = KotResponseProducer(response, supplyEntityProducer)
                            responseProducerRef.set(responseProducer)
                            responseProducer.sendResponse(responseChannel, context)
                        }

                    })

                } catch (ex: Exception) {
                    failed(ex)
                }
            }

            override fun failed(ex: Exception) {
                try {
                    terminate(ex)
                } finally {
                    releaseResources()
                }
            }

            override fun cancelled() {
                releaseResources()
            }
        })
    }

    override fun updateCapacity(capacityChannel: CapacityChannel) {
        requestConsumer.updateCapacity(capacityChannel)
    }

    override fun consume(src: ByteBuffer) {
        requestConsumer.consume(src)
    }

    override fun streamEnd(trailers: MutableList<out Header>?) {
        requestConsumer.streamEnd(trailers)
    }

    override fun available(): Int {
        val dataProducer = responseProducerRef.get()
        return dataProducer?.available() ?: 0
    }

    override fun produce(channel: DataStreamChannel) {
        val dataProducer = responseProducerRef.get()
        dataProducer?.produce(channel)
    }

    private fun terminate(cause: Exception) {
        val dataProducer = responseProducerRef.get()
        if (dataProducer != null) {
            dataProducer.failed(cause)
        } else {
            requestConsumer.failed(cause)
        }
    }

    override fun failed(cause: Exception) {
        try {
            terminate(cause)
        } finally {
            releaseResources()
        }
    }

    override fun releaseResources() {
        requestConsumer.releaseResources()
        val dataProducer = responseProducerRef.getAndSet(null)
        dataProducer?.releaseResources()
    }

}
