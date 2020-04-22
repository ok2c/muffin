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

import org.apache.hc.core5.function.Callback
import org.apache.hc.core5.function.Decorator
import org.apache.hc.core5.http.Chars
import org.apache.hc.core5.http.ConnectionClosedException
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpConnection
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.HttpRequest
import org.apache.hc.core5.http.HttpResponse
import org.apache.hc.core5.http.impl.Http1StreamListener
import org.apache.hc.core5.http.message.RequestLine
import org.apache.hc.core5.http.message.StatusLine
import org.apache.hc.core5.http2.frame.FramePrinter
import org.apache.hc.core5.http2.frame.RawFrame
import org.apache.hc.core5.http2.impl.nio.H2StreamListener
import org.apache.hc.core5.io.CloseMode
import org.apache.hc.core5.pool.ConnPoolListener
import org.apache.hc.core5.pool.ConnPoolStats
import org.apache.hc.core5.reactor.Command
import org.apache.hc.core5.reactor.IOEventHandler
import org.apache.hc.core5.reactor.IOSession
import org.apache.hc.core5.reactor.IOSessionListener
import org.apache.hc.core5.util.Identifiable
import org.apache.hc.core5.util.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.SelectionKey
import java.util.concurrent.locks.Lock

internal enum class Type {
    CLIENT, SERVER
}

internal fun getId(obj: Any?): String? {
    if (obj == null) {
        return null
    }
    return if (obj is Identifiable) {
        obj.id
    } else {
        obj.javaClass.simpleName + "-" + Integer.toHexString(System.identityHashCode(obj))
    }
}

internal class LoggingHttp1StreamListener(type: Type) : Http1StreamListener {

    companion object {
        val INSTANCE_CLIENT = LoggingHttp1StreamListener(Type.CLIENT)
        val INSTANCE_SERVER = LoggingHttp1StreamListener(Type.SERVER)
    }

    private val connLog = LoggerFactory.getLogger("com.ok2c.muffin.http.connection")
    private val headerLog = LoggerFactory.getLogger("com.ok2c.muffin.http.headers")
    private val requestDirection: String = if (type == Type.CLIENT) " >> " else " << "
    private val responseDirection: String = if (type == Type.CLIENT) " << " else " >> "

    override fun onRequestHead(connection: HttpConnection, request: HttpRequest) {
        if (headerLog.isDebugEnabled) {
            val idRequestDirection: String = getId(connection) + requestDirection
            headerLog.debug(idRequestDirection + RequestLine(request))
            val it = request.headerIterator()
            while (it.hasNext()) {
                headerLog.debug(idRequestDirection + it.next())
            }
        }
    }

    override fun onResponseHead(connection: HttpConnection, response: HttpResponse) {
        if (headerLog.isDebugEnabled) {
            val idResponseDirection: String = getId(connection) + responseDirection
            headerLog.debug(idResponseDirection + StatusLine(response))
            for (header in response.headerIterator()) {
                headerLog.debug(idResponseDirection + header)
            }
        }
    }

    override fun onExchangeComplete(connection: HttpConnection, keepAlive: Boolean) {
        if (connLog.isDebugEnabled) {
            if (keepAlive) {
                connLog.debug(getId(connection) + " Connection is kept alive")
            } else {
                connLog.debug(getId(connection) + " Connection is not kept alive")
            }
        }
    }

}

private class LogAppendable(private val log: Logger, private val prefix: String) : Appendable {

    private val buffer: StringBuilder = StringBuilder()

    override fun append(text: CharSequence): Appendable {
        return append(text, 0, text.length)
    }

    override fun append(text: CharSequence, start: Int, end: Int): Appendable {
        for (i in start until end) {
            append(text[i])
        }
        return this
    }

    override fun append(ch: Char): Appendable {
        if (ch == '\n') {
            log.debug("$prefix $buffer")
            buffer.setLength(0)
        } else if (ch != '\r') {
            buffer.append(ch)
        }
        return this
    }

    fun flush() {
        if (buffer.isNotEmpty()) {
            log.debug("$prefix $buffer")
            buffer.setLength(0)
        }
    }

}

internal class LoggingH2StreamListener : H2StreamListener {

    private val framePrinter = FramePrinter()
    private val headerLog = LoggerFactory.getLogger("com.ok2c.muffin.http.headers")
    private val frameLog = LoggerFactory.getLogger("com.ok2c.muffin.http2.frame")
    private val framePayloadLog = LoggerFactory.getLogger("com.ok2c.muffin.http2.frame.payload")
    private val flowCtrlLog = LoggerFactory.getLogger("com.ok2c.muffin.http2.flow")

    companion object {
        val INSTANCE = LoggingH2StreamListener()
    }

    private fun logFrameInfo(prefix: String, frame: RawFrame) {
        val logAppendable = LogAppendable(frameLog, prefix)
        framePrinter.printFrameInfo(frame, logAppendable)
        logAppendable.flush()
    }

    private fun logFramePayload(prefix: String, frame: RawFrame) {
        val logAppendable = LogAppendable(framePayloadLog, prefix)
        framePrinter.printPayload(frame, logAppendable)
        logAppendable.flush()
    }

    private fun logFlowControl(prefix: String, streamId: Int, delta: Int, actualSize: Int) {
        val buffer = StringBuilder()
        buffer.append(prefix).append(" stream ").append(streamId).append(" flow control ")
            .append(delta).append(" -> ")
            .append(actualSize)
        flowCtrlLog.debug(buffer.toString())
    }

    override fun onHeaderInput(connection: HttpConnection, streamId: Int, headers: List<Header?>) {
        if (headerLog.isDebugEnabled) {
            val prefix: String = getId(connection) + " << "
            for (header in headers) {
                headerLog.debug(prefix + header)
            }
        }
    }

    override fun onHeaderOutput(connection: HttpConnection, streamId: Int, headers: List<Header?>) {
        if (headerLog.isDebugEnabled) {
            val prefix: String = getId(connection) + " >> "
            for (header in headers) {
                headerLog.debug(prefix + header)
            }
        }
    }

    override fun onFrameInput(connection: HttpConnection, streamId: Int, frame: RawFrame) {
        if (frameLog.isDebugEnabled) {
            logFrameInfo(getId(connection) + " <<", frame)
        }
        if (framePayloadLog.isDebugEnabled) {
            logFramePayload(getId(connection) + " <<", frame)
        }
    }

    override fun onFrameOutput(connection: HttpConnection, streamId: Int, frame: RawFrame) {
        if (frameLog.isDebugEnabled) {
            logFrameInfo(getId(connection) + " >>", frame)
        }
        if (framePayloadLog.isDebugEnabled) {
            logFramePayload(getId(connection) + " >>", frame)
        }
    }

    override fun onInputFlowControl(connection: HttpConnection, streamId: Int, delta: Int, actualSize: Int) {
        if (flowCtrlLog.isDebugEnabled) {
            logFlowControl(getId(connection) + "  in", streamId, delta, actualSize)
        }
    }

    override fun onOutputFlowControl(connection: HttpConnection, streamId: Int, delta: Int, actualSize: Int) {
        if (flowCtrlLog.isDebugEnabled) {
            logFlowControl(getId(connection) + " out", streamId, delta, actualSize)
        }
    }

}

private class Wire(private val log: Logger, private val id: String) {

    private fun wire(header: String, b: ByteArray, pos: Int, off: Int) {
        val buffer = StringBuilder()
        for (i in 0 until off) {
            val ch = b[pos + i].toInt()
            if (ch == Chars.CR) {
                buffer.append("[\\r]")
            } else if (ch == Chars.LF) {
                buffer.append("[\\n]\"")
                buffer.insert(0, "\"")
                buffer.insert(0, header)
                log.debug("$id $buffer")
                buffer.setLength(0)
            } else if (ch < Chars.SP || ch >= Chars.DEL) {
                buffer.append("[0x")
                buffer.append(Integer.toHexString(ch))
                buffer.append("]")
            } else {
                buffer.append(ch.toChar())
            }
        }
        if (buffer.isNotEmpty()) {
            buffer.append('\"')
            buffer.insert(0, '\"')
            buffer.insert(0, header)
            log.debug("$id $buffer")
        }
    }

    fun isEnabled(): Boolean {
        return log.isDebugEnabled
    }

    @JvmOverloads
    fun output(b: ByteArray, pos: Int = 0, off: Int = b.size) {
        wire(">> ", b, pos, off)
    }

    @JvmOverloads
    fun input(b: ByteArray, pos: Int = 0, off: Int = b.size) {
        wire("<< ", b, pos, off)
    }

    fun output(b: Int) {
        output(byteArrayOf(b.toByte()))
    }

    fun input(b: Int) {
        input(byteArrayOf(b.toByte()))
    }

    fun output(b: ByteBuffer) {
        if (b.hasArray()) {
            output(b.array(), b.arrayOffset() + b.position(), b.remaining())
        } else {
            val tmp = ByteArray(b.remaining())
            b[tmp]
            output(tmp)
        }
    }

    fun input(b: ByteBuffer) {
        if (b.hasArray()) {
            input(b.array(), b.arrayOffset() + b.position(), b.remaining())
        } else {
            val tmp = ByteArray(b.remaining())
            b[tmp]
            input(tmp)
        }
    }

}

private fun formatOps(ops: Int): String {
    val buffer: StringBuilder = StringBuilder(6)
    buffer.append('[')
    if ((ops and SelectionKey.OP_READ) > 0) {
        buffer.append('r')
    }
    if ((ops and SelectionKey.OP_WRITE) > 0) {
        buffer.append('w')
    }
    if ((ops and SelectionKey.OP_ACCEPT) > 0) {
        buffer.append('a')
    }
    if ((ops and SelectionKey.OP_CONNECT) > 0) {
        buffer.append('c')
    }
    buffer.append(']')
    return buffer.toString()
}

internal class LoggingIOSession(private val session: IOSession, private val log: Logger, wireLog: Logger) : IOSession {

    private val wireLog: Wire =
        Wire(wireLog, session.id)

    override fun getId(): String {
        return session.id
    }

    override fun getLock(): Lock {
        return session.lock
    }

    override fun enqueue(command: Command, priority: Command.Priority) {
        session.enqueue(command, priority)
        if (log.isDebugEnabled) {
            log.debug("$session Enqueued ${command.javaClass.simpleName} with priority $priority")
        }
    }

    override fun hasCommands(): Boolean {
        return session.hasCommands()
    }

    override fun poll(): Command? {
        return session.poll()
    }

    override fun channel(): ByteChannel {
        return session.channel()
    }

    override fun getLocalAddress(): SocketAddress {
        return session.localAddress
    }

    override fun getRemoteAddress(): SocketAddress {
        return session.remoteAddress
    }

    override fun getEventMask(): Int {
        return session.eventMask
    }

    override fun setEventMask(ops: Int) {
        session.eventMask = ops
        if (log.isDebugEnabled) {
            log.debug("$session Event mask set ${formatOps(ops)}")
        }
    }

    override fun setEvent(op: Int) {
        session.setEvent(op)
        if (log.isDebugEnabled) {
            log.debug("$session Event set ${formatOps(op)}")
        }
    }

    override fun clearEvent(op: Int) {
        session.clearEvent(op)
        if (log.isDebugEnabled) {
            log.debug("$session Event cleared ${formatOps(op)}")
        }
    }

    override fun close() {
        if (log.isDebugEnabled) {
            log.debug("$session Close")
        }
        session.close()
    }

    override fun getStatus(): IOSession.Status {
        return session.status
    }

    override fun isOpen(): Boolean {
        return session.isOpen
    }

    override fun close(closeMode: CloseMode) {
        if (log.isDebugEnabled) {
            log.debug("$session Shutdown $closeMode")
        }
        session.close(closeMode)
    }

    override fun getSocketTimeout(): Timeout {
        return session.socketTimeout
    }

    override fun setSocketTimeout(timeout: Timeout) {
        if (log.isDebugEnabled) {
            log.debug("$session Set timeout $timeout")
        }
        session.socketTimeout = timeout
    }

    override fun read(dst: ByteBuffer): Int {
        val bytesRead: Int = session.read(dst)
        if (log.isDebugEnabled) {
            log.debug("$session $bytesRead bytes read")
        }
        if (bytesRead > 0 && wireLog.isEnabled()) {
            val b: ByteBuffer = dst.duplicate()
            val p: Int = b.position()
            b.limit(p)
            b.position(p - bytesRead)
            wireLog.input(b)
        }
        return bytesRead
    }

    override fun write(src: ByteBuffer): Int {
        val byteWritten: Int = session.write(src)
        if (log.isDebugEnabled) {
            log.debug("$session $byteWritten bytes written")
        }
        if (byteWritten > 0 && wireLog.isEnabled()) {
            val b: ByteBuffer = src.duplicate()
            val p: Int = b.position()
            b.limit(p)
            b.position(p - byteWritten)
            wireLog.output(b)
        }
        return byteWritten
    }

    override fun updateReadTime() {
        session.updateReadTime()
    }

    override fun updateWriteTime() {
        session.updateWriteTime()
    }

    override fun getLastReadTime(): Long {
        return session.lastReadTime
    }

    override fun getLastWriteTime(): Long {
        return session.lastWriteTime
    }

    override fun getLastEventTime(): Long {
        return session.lastEventTime
    }

    override fun getHandler(): IOEventHandler {
        return session.handler
    }

    override fun upgrade(handler: IOEventHandler?) {
        if (log.isDebugEnabled) {
            log.debug("$session Protocol upgrade: ${handler?.javaClass}")
        }
        session.upgrade(handler)
    }

    override fun toString(): String {
        return session.toString()
    }

}

internal class LoggingIOSessionDecorator : Decorator<IOSession> {

    private val wireLog = LoggerFactory.getLogger("com.ok2c.muffin.http.wire")

    companion object {
        val INSTANCE = LoggingIOSessionDecorator()
    }

    override fun decorate(ioSession: IOSession): IOSession {
        val sessionLog = LoggerFactory.getLogger(ioSession.javaClass)
        return LoggingIOSession(ioSession, sessionLog, wireLog)
    }

}

internal class LoggingIOSessionListener private constructor() : IOSessionListener {

    companion object {
        val INSTANCE = LoggingIOSessionListener()
    }

    private val connLog = LoggerFactory.getLogger("com.ok2c.muffin.http.connection")

    override fun connected(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session connected")
        }
    }

    override fun startTls(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session TLS started")
        }
    }

    override fun inputReady(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session input ready")
        }
    }

    override fun outputReady(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session output ready")
        }
    }

    override fun timeout(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session timeout")
        }
    }

    override fun exception(session: IOSession, ex: Exception) {
        if (ex is ConnectionClosedException) {
            return
        }
        connLog.error(session.toString() + " " + ex.message, ex)
    }

    override fun disconnected(session: IOSession) {
        if (connLog.isDebugEnabled) {
            connLog.debug("$session disconnected")
        }
    }

}

internal class LoggingExceptionCallback : Callback<Exception> {

    companion object {
        val INSTANCE = LoggingExceptionCallback()
    }

    private val log = LoggerFactory.getLogger("com.ok2c.muffin.reactor")

    override fun execute(ex: Exception) {
        log.error(ex.message, ex)
    }

}

internal class LoggingConnPoolListener : ConnPoolListener<HttpHost?> {

    companion object {
        val INSTANCE = LoggingConnPoolListener()
    }

    private val connLog = LoggerFactory.getLogger("com.ok2c.muffin.http.connection")

    override fun onLease(route: HttpHost?, connPoolStats: ConnPoolStats<HttpHost?>) {
        if (connLog.isDebugEnabled) {
            val totals = connPoolStats.totalStats
            connLog.debug(
                "Leased ${route}: total kept alive: ${totals.available}; " +
                        "total allocated: ${totals.leased + totals.available} of ${totals.max}"
            )
        }
    }

    override fun onRelease(route: HttpHost?, connPoolStats: ConnPoolStats<HttpHost?>) {
        if (connLog.isDebugEnabled) {
            val totals = connPoolStats.totalStats
            connLog.debug(
                "Released ${route}: total kept alive: ${totals.available}; " +
                        "total allocated: ${totals.leased + totals.available} of ${totals.max}"
            )
        }
    }

}

