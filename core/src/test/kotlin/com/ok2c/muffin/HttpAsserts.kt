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
package com.ok2c.muffin

import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.message.BasicHeader
import org.assertj.core.api.AbstractObjectAssert
import org.assertj.core.error.ShouldBeEqual
import org.assertj.core.internal.Failures
import org.assertj.core.internal.StandardComparisonStrategy

fun assertThat(actual: Header?): HeaderAssert {
    return HeaderAssert(actual)
}

class HeaderAssert(actual: Header?) : AbstractObjectAssert<HeaderAssert, Header>(actual, HeaderAssert::class.java) {

    private val failures = Failures.instance()
    private val comparisonStrategy = StandardComparisonStrategy.instance()

    fun sameAs(name: String, value: String?): HeaderAssert {
        isNotNull
        val other = BasicHeader(name, value)
        if (!comparisonStrategy.areEqual(actual.name, other.name)
            || !comparisonStrategy.areEqual(actual.value, other.value)) {
            throw Failures.instance().failure(
                info,
                ShouldBeEqual.shouldBeEqual(actual, other, comparisonStrategy, info.representation())
            )
        }
        return this;

    }

}
