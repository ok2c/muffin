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

import com.ok2c.muffin.message.Request
import com.ok2c.muffin.message.Response
import org.apache.hc.core5.concurrent.FutureCallback
import org.apache.hc.core5.http.EntityDetails
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.HttpResponse
import org.apache.hc.core5.http.message.BasicHttpRequest
import org.apache.hc.core5.http.nio.AsyncEntityConsumer
import org.apache.hc.core5.http.nio.AsyncEntityProducer
import org.apache.hc.core5.http.nio.AsyncRequestProducer
import org.apache.hc.core5.http.nio.AsyncResponseConsumer
import org.apache.hc.core5.http.nio.CapacityChannel
import org.apache.hc.core5.http.nio.DataStreamChannel
import org.apache.hc.core5.http.nio.RequestChannel
import org.apache.hc.core5.http.protocol.HttpContext
import org.apache.hc.core5.net.URIAuthority
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

internal class KotRequestProducer<T>(
    private val host: HttpHost,
    private val request: Request<T>,
    private val supplyEntityProducer: (entity: T) -> AsyncEntityProducer
) : AsyncRequestProducer {

    private val entityProducerRef = AtomicReference<AsyncEntityProducer>()

    override fun sendRequest(channel: RequestChannel, context: HttpContext) {
        val httpRequest = BasicHttpRequest(request.method, request.uri)
        if (httpRequest.authority == null) {
            httpRequest.authority = URIAuthority(host)
        }
        for (header in request.headers) {
            httpRequest.addHeader(header)
        }
        val entity = request.entity
        val entityProducer = if (entity != null) supplyEntityProducer(entity) else null
        entityProducerRef.set(entityProducer)
        channel.sendRequest(httpRequest, entityProducer, context)
    }

    override fun isRepeatable(): Boolean {
        val entityProducer = entityProducerRef.get()
        return entityProducer?.isRepeatable ?: true
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

internal class KotResponseConsumer<T>(
    private val supplyEntityConsumer: (entityDetails: EntityDetails) -> AsyncEntityConsumer<T>
) : AsyncResponseConsumer<Response<T>> {

    private val builder = Response.Builder<T>()
    private val entityConsumerRef = AtomicReference<AsyncEntityConsumer<T>>()

    override fun informationResponse(response: HttpResponse, context: HttpContext) {
    }

    override fun consumeResponse(
        response: HttpResponse,
        entityDetails: EntityDetails?,
        context: HttpContext,
        resultCallback: FutureCallback<Response<T>>
    ) {
        with(builder) {
            status = response.code
            for (header in response.headerIterator()) {
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
