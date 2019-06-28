package net.gridtech.master

import net.gridtech.core.Bootstrap
import net.gridtech.core.data.*
import net.gridtech.core.exchange.DataShell
import net.gridtech.core.exchange.ExchangeParcel
import net.gridtech.core.exchange.IMaster
import net.gridtech.core.exchange.ISlave
import net.gridtech.core.util.parse
import net.gridtech.core.util.stringfy
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.util.concurrent.CopyOnWriteArraySet
import kotlin.reflect.KFunction
import kotlin.reflect.full.declaredMemberFunctions

class HostMaster(private val bootstrap: Bootstrap) : TextWebSocketHandler() {
    private val commandMap: Map<String, KFunction<*>> = IMaster::class.declaredMemberFunctions.associateBy { it.name }
    private val childrenSet = CopyOnWriteArraySet<WebSocketSession>()

    companion object {
        private const val CHILD_SCOPE = "scope"
        private const val CHILD_NODE_ID = "nodeId"
        private const val CHILD_PEER = "peer"
        fun childNodeId(session: WebSocketSession): String = session.attributes[CHILD_NODE_ID] as String
        fun childPeer(session: WebSocketSession): String = session.attributes[CHILD_PEER] as String
        fun childScope(session: WebSocketSession): ChildScope = session.attributes[CHILD_SCOPE] as ChildScope
    }

    init {

    }

    override fun afterConnectionEstablished(session: WebSocketSession) {
        childrenSet.add(session)
        createChildNodeScope(session)
        send(session, ISlave::beginToSync, "")
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        try {
            val exchangeParcel: ExchangeParcel = parse(message.payload)
            commandMap[exchangeParcel.command]?.call(handler, session, exchangeParcel.content, exchangeParcel.serviceName)
        } catch (e: Throwable) {
            e.printStackTrace()
        }
        super.handleTextMessage(session, message)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        childrenSet.remove(session)
    }

    fun createChildNodeScope(session: WebSocketSession) {
        val nodeScope = bootstrap.nodeService.getNodeScope(childNodeId(session))
        val nodeIdScope = nodeScope.map { it.id }
        val nodeClassIdScope = nodeScope.map { it.nodeClassId }.toSet()
        val fieldIdScope = nodeClassIdScope.flatMap { nodeClassId ->
            bootstrap.nodeClassService.getById(nodeClassId)?.let { nodeClass ->
                bootstrap.fieldService.getByNodeClass(nodeClass)
            } ?: emptyList()
        }.map { it.id }
        session.attributes[CHILD_SCOPE] = ChildScope(
                nodeScope = nodeIdScope.toMutableSet(),
                nodeClassScope = nodeClassIdScope.toMutableSet(),
                sync = mapOf(
                        NodeClassService::class.simpleName!! to nodeClassIdScope.toMutableList(),
                        FieldService::class.simpleName!! to fieldIdScope.toMutableList(),
                        NodeService::class.simpleName!! to nodeIdScope.toMutableList())
        )
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

    private val handler = object : IMaster<WebSocketSession> {
        override fun dataShellFromSlave(session: WebSocketSession, content: String, serviceName: String?) {
            val dataShell: DataShell = parse(content)
            val scope = childScope(session)
            val inScope = scope.sync[serviceName]!!.remove(dataShell.id)
            if (inScope) {
                IBaseService.get(serviceName!!).getById(dataShell.id)?.apply {
                    if (updateTime > dataShell.updateTime) {
                        send(session, ISlave::dataUpdate, this, serviceName)
                    }
                }
            } else {
                send(session, ISlave::dataDelete, dataShell.id, serviceName)
            }
        }

        override fun serviceSyncFinishedFromSlave(session: WebSocketSession, content: String, serviceName: String?) {
            val syncList = childScope(session).sync[serviceName]
            syncList?.forEach {
                IBaseService.get(serviceName!!).getById(it)?.apply {
                    send(session, ISlave::dataUpdate, this, serviceName)
                }
            }
            syncList?.clear()
            send(session, ISlave::serviceSyncFinishedFromMaster, "", serviceName)
        }

        override fun fieldValueUpdate(session: WebSocketSession, content: String, serviceName: String?) {
            val fieldValue: FieldValueStub = parse(content)
            bootstrap.fieldValueService.save(fieldValue, childPeer(session))
        }
    }
}