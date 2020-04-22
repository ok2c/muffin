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

import com.ok2c.muffin.assertThat
import org.apache.hc.core5.http.message.BasicHeader
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.net.URI

class MessageTest {

    @Test
    fun `basic request & response message operations`() {
        val request1 = Request.Builder<String>().apply {
            method = "GET"
            uri = URI("http://somehost/stuff")
            addHeader(BasicHeader("h1", "aaaa"))
            addHeader(BasicHeader("h1", "bbbb"))
            addHeader(BasicHeader("h1", "cccc"))
            addHeader(BasicHeader("h2", "111"))
        }.build()

        Assertions.assertThat(request1.method).isEqualTo("GET")
        Assertions.assertThat(request1.uri).isEqualTo(URI("http://somehost/stuff"))

        assertThat(request1.getFirstHeader("h1")).sameAs("h1", "aaaa")
        assertThat(request1.getLastHeader("h1")).sameAs("h1", "cccc")
        assertThat(request1.getFirstHeader("h2")).sameAs("h2", "111")
        assertThat(request1.getLastHeader("h2")).sameAs("h2", "111")
        assertThat(request1.getFirstHeader("h3")).isNull()
        assertThat(request1.getLastHeader("h3")).isNull()

        Assertions.assertThat(request1.getHeaders("h1"))
            .extracting("value")
            .containsExactly("aaaa", "bbbb", "cccc")

        val request2 = Request.Builder<String>().apply {
            copy(request1)
            method = "HEAD"
            setHeader(BasicHeader("h1", "dddd"))
            addHeader(BasicHeader("h1", "eeee"))
            setHeader(BasicHeader("h2", "222"))
        }.build()

        Assertions.assertThat(request2.method).isEqualTo("HEAD")
        Assertions.assertThat(request2.uri).isEqualTo(URI("http://somehost/stuff"))

        assertThat(request2.getFirstHeader("h1")).sameAs("h1", "dddd")
        assertThat(request2.getLastHeader("h1")).sameAs("h1", "eeee")
        assertThat(request2.getFirstHeader("h2")).sameAs("h2", "222")
        assertThat(request2.getLastHeader("h2")).sameAs("h2", "222")
        assertThat(request2.getFirstHeader("h3")).isNull()
        assertThat(request2.getLastHeader("h3")).isNull()

        Assertions.assertThat(request2.getHeaders("h1"))
            .extracting("value")
            .containsExactly("dddd", "bbbb", "cccc", "eeee")

    }

}
