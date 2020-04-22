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
package com.ok2c.muffin.message

import org.apache.hc.core5.http.Header
import java.net.URI

open class MutableHeaders internal constructor() {

    internal val headers = mutableListOf<Header>()

    fun removeHeaders(name: String): Boolean {
        var removed = false
        val iterator = headers.listIterator()
        while (iterator.hasNext()) {
            val header = iterator.next()
            if (header.name.equals(name, ignoreCase = true)) {
                iterator.remove()
                removed = true
            }
        }
        return removed
    }

    fun addHeader(header: Header) {
        headers.add(header)
    }

    fun setHeader(header: Header) {
        val iterator = headers.listIterator()
        while (iterator.hasNext()) {
            val current = iterator.next()
            if (current.name.equals(header.name, ignoreCase = true)) {
                iterator.set(header)
                return
            }
        }
        headers.add(header)
    }

}

open class Message internal constructor(private val headers: List<Header>) {

    fun getFirstHeader(name: String): Header? {
        for (header in headers) {
            if (header.name.equals(name, ignoreCase = true)) {
                return header
            }
        }
        return null
    }

    fun getLastHeader(name: String): Header? {
        for (header in headers.asReversed()) {
            if (header.name.equals(name, ignoreCase = true)) {
                return header
            }
        }
        return null
    }

    fun getHeaders(name: String): List<Header> {
        val result = mutableListOf<Header>()
        for (header in headers) {
            if (header.name.equals(name, ignoreCase = true)) {
                result.add(header)
            }
        }
        return result.toList()
    }

}

class Request<T>(val method: String, val uri: URI, val headers: List<Header>, val entity: T?): Message(headers) {

    class Builder<T>: MutableHeaders() {

        var method: String? = null
        var uri: URI? = null
        var entity: T? = null

        fun copy(request: Request<T>) {
            method = request.method
            uri = request.uri
            headers.clear()
            headers.addAll(request.headers)
            entity = request.entity
        }

        fun build(): Request<T> {
            return Request(
                if (method != null) method!! else "GET",
                if (uri != null) uri!! else URI("/"),
                headers.toList(),
                entity)
        }

    }

}

class Response<T>(val status: Int, val headers: List<Header>, val entity: T?): Message(headers) {

    class Builder<T>: MutableHeaders() {

        var status: Int? = null
        var entity: T? = null

        fun copy(response: Response<T>) {
            status = response.status
            headers.clear()
            headers.addAll(response.headers)
            entity = response.entity
        }

        fun build(): Response<T> {
            return Response(
                if (status != null) status!! else 200,
                headers.toList(),
                entity)
        }

    }

}
