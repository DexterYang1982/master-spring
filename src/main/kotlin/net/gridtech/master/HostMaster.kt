package net.gridtech.master

import io.reactivex.schedulers.Schedulers.io
import net.gridtech.core.Bootstrap
import net.gridtech.core.data.*
import net.gridtech.core.exchange.DataShell
import net.gridtech.core.exchange.ExchangeParcel
import net.gridtech.core.exchange.IMaster
import net.gridtech.core.exchange.ISlave
import net.gridtech.core.util.compose
import net.gridtech.core.util.dataChangedPublisher
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
        dataChangedPublisher
                .subscribeOn(io())
                .subscribe { message ->
                    childrenSet.filter { session ->
                        message.peer != childPeer(session)
                    }.forEach { session ->
                        val childScope = childScope(session)
                        if (message.type == ChangedType.DELETE) {
                            if (childScope.scope[message.serviceName]?.remove(message.dataId) == true &&
                                    childScope.sync[message.serviceName]?.remove(message.dataId) == false) {
                                send(session, ISlave::dataDelete, message.dataId, message.serviceName)
                            }
                        } else {
                            if (childScope.scope[message.serviceName]?.contains(message.dataId) == true &&
                                    childScope.sync[message.serviceName]?.contains(message.dataId) == false) {

                                IBaseService.service(message.serviceName).getById(message.dataId)?.apply {
                                    send(session, ISlave::dataUpdate, this, message.serviceName)
                                }
                            } else if (message.serviceName == NodeService::class.simpleName) {


                            }
                        }
                    }
                }
    }

    override fun afterConnectionEstablished(session: WebSocketSession) {
        println("child node ${childNodeId(session)} peer ${childPeer(session)} connected")
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
        System.err.println("child node ${childNodeId(session)} peer ${childPeer(session)} disconnected ${status.code}")
        childrenSet.remove(session)
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        System.err.println("child node ${childNodeId(session)} peer ${childPeer(session)} error ${exception.message}")
        childrenSet.remove(session)
    }

    private fun createChildNodeScope(session: WebSocketSession) {
        val childNodeId = childNodeId(session)
        val nodeScope = bootstrap.nodeService.getNodeScope(childNodeId)
        val nodeClassIdScope = nodeScope.map { it.nodeClassId }.toSet()
        val fieldScope = nodeClassIdScope.flatMap { nodeClassId ->
            bootstrap.nodeClassService.getById(nodeClassId)?.let { nodeClass ->
                bootstrap.fieldService.getByNodeClass(nodeClass)
            } ?: emptyList()
        }
        val fieldValueIdScope = nodeScope.flatMap { node ->
            fieldScope
                    .filter { it.nodeClassId == node.nodeClassId }
                    .filter { bootstrap.fieldValueService.valueSyncToChild(childNodeId, node, it) }
                    .map { field ->
                        compose(node.id, field.id)
                    }
        }
        session.attributes[CHILD_SCOPE] = ChildScope(
                scope = mapOf(
                        NodeClassService::class.simpleName!! to nodeClassIdScope.toMutableSet(),
                        FieldService::class.simpleName!! to fieldScope.map { it.id }.toMutableSet(),
                        NodeService::class.simpleName!! to nodeScope.map { it.id }.toMutableSet(),
                        FieldValueService::class.simpleName!! to fieldValueIdScope.toMutableSet()
                ),
                sync = mapOf(
                        NodeClassService::class.simpleName!! to nodeClassIdScope.toMutableList(),
                        FieldService::class.simpleName!! to fieldScope.map { it.id }.toMutableList(),
                        NodeService::class.simpleName!! to nodeScope.map { it.id }.toMutableList(),
                        FieldValueService::class.simpleName!! to fieldValueIdScope.toMutableList()
                )
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
                IBaseService.service(serviceName!!).getById(dataShell.id)?.apply {
                    if (updateTime > dataShell.updateTime) {
                        send(session, ISlave::dataUpdate, this, serviceName)
                    }
                }
            } else {
                send(session, ISlave::dataDelete, dataShell.id, serviceName)
            }
        }

        override fun serviceSyncFinishedFromSlave(session: WebSocketSession, content: String, serviceName: String?) {
            childScope(session).sync[serviceName]?.apply {
                forEach {
                    IBaseService.service(serviceName!!).getById(it)?.apply {
                        send(session, ISlave::dataUpdate, this, serviceName)
                    }
                }
                clear()
            }
            send(session, ISlave::serviceSyncFinishedFromMaster, "", serviceName)
        }

        override fun fieldValueUpdate(session: WebSocketSession, content: String, serviceName: String?) {
            val fieldValue: FieldValueStub = parse(content)
            bootstrap.fieldValueService.save(fieldValue, childPeer(session))
        }
    }
}