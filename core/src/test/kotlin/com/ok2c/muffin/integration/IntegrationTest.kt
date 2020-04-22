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
package com.ok2c.muffin.integration

import com.ok2c.muffin.client.ClientBootstrap
import com.ok2c.muffin.message.Request
import com.ok2c.muffin.message.Response
import com.ok2c.muffin.server.ServerBootstrap
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.io.CloseMode
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Future

class IntegrationTest {

    private val serverBootstrap = ServerBootstrap().apply {
        canonicalHostName = "localhost"
    }

    private val server by lazy { serverBootstrap.create() }
    private val clientBootstrap = ClientBootstrap()
    private val client by lazy { clientBootstrap.create() }

    @AfterEach
    fun clientShutdown() {
        client.close(CloseMode.GRACEFUL)
    }

    @AfterEach
    fun serverShutdown() {
        server.close(CloseMode.GRACEFUL)
    }

    private fun startServer(): HttpHost {
        server.start();
        val listenerFuture = server.listen(InetSocketAddress(0))
        val endpoint = listenerFuture.get()
        val address = endpoint.address as InetSocketAddress
        return HttpHost("http", "localhost", address.port)
    }

    @ParameterizedTest(name = "{displayName} ({0} concurrent connections)")
    @ValueSource(ints = [2, 5, 10])
    fun `sequence of 20 requests`(connNum: Int) {
        serverBootstrap.apply {
            register("*") { request, trigger ->
                val response = Response.Builder<String>().apply {
                    status = 200
                    entity = "ACK: " + request.entity
                }.build()
                trigger.submitResponse(response)
            }
        }.create()

        val targetHost = startServer()
        client.start()
        client.defaultMaxPerRoute = connNum

        val queue = ConcurrentLinkedQueue<Future<Response<String>>>()
        for (n in 1..20) {
            val request = Request.Builder<String>().apply {
                method = "GET"
                uri = URI("/")
                entity = "Hi there"
            }.build()
            val future = client.execute(targetHost, request, null)
            queue.add(future)
        }

        while (!queue.isEmpty()) {
            val future = queue.remove()
            val response = future.get()
            Assertions.assertThat(response.status).isEqualTo(200)
            Assertions.assertThat(response.entity).isEqualTo("ACK: Hi there")
        }
    }

}
