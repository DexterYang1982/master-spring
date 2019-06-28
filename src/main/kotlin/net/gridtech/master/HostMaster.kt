package net.gridtech.master

import net.gridtech.core.Bootstrap
import net.gridtech.core.exchange.ExchangeParcel
import net.gridtech.core.util.parse
import net.gridtech.core.util.stringfy
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.reflect.KFunction

class HostMaster(bootstrap: Bootstrap) : TextWebSocketHandler() {
    private val childrenSet = CopyOnWriteArraySet<WebSocketSession>()

    init {


    }

    override fun afterConnectionEstablished(session: WebSocketSession) {
        childrenSet.add(session)
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        try {
            val exchangeParcel: ExchangeParcel = parse(message.payload)
        } catch (e: Throwable) {
            e.printStackTrace()
        }
        super.handleTextMessage(session, message)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        childrenSet.remove(session)
    }

    fun send(session: WebSocketSession, function: KFunction<*>, content: Any, serviceName: String? = null) {
        if (session.isOpen) {
            try {
                session.sendMessage(
                        TextMessage(
                                stringfy(
                                        ExchangeParcel(
                                                command = function.name,
                                                content = stringfy(content),
                                                serviceName = serviceName
                                        ))))
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
    }
}