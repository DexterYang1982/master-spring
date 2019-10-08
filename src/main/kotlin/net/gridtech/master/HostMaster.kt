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
        private const val CHILD_INSTANCE = "instance"
        fun childNodeId(session: WebSocketSession): String = session.attributes[CHILD_NODE_ID] as String
        fun childInstance(session: WebSocketSession): String = session.attributes[CHILD_INSTANCE] as String
        fun childScope(session: WebSocketSession): ChildScope = session.attributes[CHILD_SCOPE] as ChildScope
    }

    init {
        dataChangedPublisher
                .subscribeOn(io())
                .subscribe { message ->
                    childrenSet.filter { session ->
                        message.instance != childInstance(session)
                    }.forEach { session ->
                        val childScope = childScope(session)
                        if (message.type == ChangedType.DELETE) {
                            if (childScope.scope[message.serviceName]?.remove(message.dataId) == true
                                    && childScope.sync[message.serviceName]?.remove(message.dataId) == false
                                    && message.serviceName != bootstrap.fieldValueService.serviceName) {
                                send(session, ISlave::dataDelete, message.dataId, message.serviceName)
                            }
                        } else {
                            if (childScope.scope[message.serviceName]?.contains(message.dataId) == true &&
                                    childScope.sync[message.serviceName]?.contains(message.dataId) == false) {
                                bootstrap.service(message.serviceName).getById(message.dataId)?.apply {
                                    send(session, ISlave::dataUpdate, this, message.serviceName)
                                }
                            } else if (message.serviceName == bootstrap.nodeClassService.serviceName) {
                                val childNode = bootstrap.nodeService.getById(childNodeId(session))!!
                                val nodeClassUpdated = bootstrap.nodeClassService.getById(message.dataId)!!
                                if (nodeClassUpdated.tags.containsAll(childNode.externalNodeClassTagScope)) {
                                    childScope.scope[bootstrap.nodeClassService.serviceName]?.add(nodeClassUpdated.id)
                                    send(session, ISlave::dataUpdate, nodeClassUpdated, message.serviceName)
                                }
                            } else if (message.serviceName == bootstrap.nodeService.serviceName ||
                                    message.serviceName == bootstrap.fieldService.serviceName) {
                                val childNode = bootstrap.nodeService.getById(childNodeId(session))!!
                                var nodeClassToScope: INodeClass? = null
                                var fieldsToScope: List<IField>? = null
                                var nodeToScope: INode? = null
                                var fieldValueToScope: List<String>? = null
                                if (message.serviceName == bootstrap.nodeService.serviceName) {
                                    var inFullScope = false
                                    val nodeUpdated = bootstrap.nodeService.getById(message.dataId)!!
                                    if (nodeUpdated.path.contains(childNode.id)) {
                                        nodeToScope = nodeUpdated
                                        inFullScope = true
                                    } else {
                                        var external = false
                                        childNode.externalNodeIdScope.forEach {
                                            external = external || nodeUpdated.path.contains(it)
                                        }
                                        if (external) {
                                            nodeToScope = nodeUpdated
                                            inFullScope = false
                                        }
                                    }
                                    if (nodeToScope != null && !childScope.scope.getValue(bootstrap.nodeClassService.serviceName).contains(nodeToScope.nodeClassId)) {
                                        nodeClassToScope = bootstrap.nodeClassService.getById(nodeUpdated.nodeClassId)!!
                                    }
                                    if (nodeClassToScope != null) {
                                        fieldsToScope = bootstrap.fieldService.getByNodeClass(nodeClassToScope)
                                    }

                                    fieldValueToScope = (fieldsToScope?:bootstrap.nodeClassService.getById(nodeUpdated.nodeClassId)?.let { bootstrap.fieldService.getByNodeClass(it) })
                                            ?.filter { field ->
                                                inFullScope || field.through
                                            }
                                            ?.map { field ->
                                                compose(nodeUpdated.id, field.id)
                                            }
                                    System.err.println(fieldValueToScope)

                                } else if (message.serviceName == bootstrap.fieldService.serviceName) {
                                    val fieldUpdated = bootstrap.fieldService.getById(message.dataId)!!
                                    if (childScope.scope.getValue(bootstrap.nodeClassService.serviceName).contains(fieldUpdated.nodeClassId)) {
                                        fieldsToScope = listOf(fieldUpdated)
                                        fieldValueToScope = childScope.scope.getValue(bootstrap.nodeService.serviceName).mapNotNull { nodeId ->
                                            bootstrap.nodeService.getById(nodeId)
                                        }.filter { node ->
                                            node.nodeClassId == fieldUpdated.nodeClassId
                                        }.filter { node ->
                                            fieldUpdated.through || node.id == childNode.id || node.path.contains(childNode.id)
                                        }.map { node ->
                                            compose(node.id, fieldUpdated.id)
                                        }
                                    }
                                }
                                nodeClassToScope?.apply {
                                    childScope.scope.getValue(bootstrap.nodeClassService.serviceName).add(id)
                                    send(session, ISlave::dataUpdate, this, bootstrap.nodeClassService.serviceName)
                                }
                                fieldsToScope?.forEach { field ->
                                    childScope.scope.getValue(bootstrap.fieldService.serviceName).add(field.id)
                                    send(session, ISlave::dataUpdate, field, bootstrap.fieldService.serviceName)
                                }
                                nodeToScope?.apply {
                                    childScope.scope.getValue(bootstrap.nodeService.serviceName).add(id)
                                    send(session, ISlave::dataUpdate, this, bootstrap.nodeService.serviceName)
                                }
                                fieldValueToScope?.forEach { fieldValueId ->
                                    childScope.scope.getValue(bootstrap.fieldValueService.serviceName).add(fieldValueId)
                                    bootstrap.fieldValueService.getById(fieldValueId)?.apply {
                                        send(session, ISlave::dataUpdate, this, bootstrap.fieldValueService.serviceName)
                                    }
                                }
                            }
                        }
                    }
                }
    }

    override fun afterConnectionEstablished(session: WebSocketSession) {
        println("child node ${childNodeId(session)} instance ${childInstance(session)} connected")
        childrenSet.add(session)
        createChildNodeScope(session)
        send(session, ISlave::beginToSync, "")
    }

    override fun handleTextMessage(session: WebSocketSession, message: TextMessage) {
        try {
            val exchangeParcel: ExchangeParcel = parse(message.payload)
            println("Master->$exchangeParcel")
            commandMap[exchangeParcel.command]?.call(handler, session, exchangeParcel.content, exchangeParcel.serviceName)
        } catch (e: Throwable) {
            e.printStackTrace()
        }
        super.handleTextMessage(session, message)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        System.err.println("child node ${childNodeId(session)} instance ${childInstance(session)} disconnected ${status.code}")
        childrenSet.remove(session)
    }

    override fun handleTransportError(session: WebSocketSession, exception: Throwable) {
        System.err.println("child node ${childNodeId(session)} instance ${childInstance(session)} error ${exception.message}")
        childrenSet.remove(session)
    }

    private fun createChildNodeScope(session: WebSocketSession) {
        val childNodeId = childNodeId(session)
        val childNode = bootstrap.nodeService.getById(childNodeId)!!
        val nodeScope = bootstrap.nodeService.getNodeScope(childNode)
        val nodeClassIdScope = nodeScope.map { it.nodeClassId }.toMutableSet().apply {
            addAll(bootstrap.nodeClassService.getByTags(childNode.externalNodeClassTagScope).map { it.id })
        }
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
                        bootstrap.nodeClassService.serviceName to nodeClassIdScope.toMutableSet(),
                        bootstrap.fieldService.serviceName to fieldScope.map { it.id }.toMutableSet(),
                        bootstrap.nodeService.serviceName to nodeScope.map { it.id }.toMutableSet(),
                        bootstrap.fieldValueService.serviceName to fieldValueIdScope.toMutableSet()
                ),
                sync = mapOf(
                        bootstrap.nodeClassService.serviceName to nodeClassIdScope.toMutableList(),
                        bootstrap.fieldService.serviceName to fieldScope.map { it.id }.toMutableList(),
                        bootstrap.nodeService.serviceName to nodeScope.map { it.id }.toMutableList(),
                        bootstrap.fieldValueService.serviceName to fieldValueIdScope.toMutableList()
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
            if (serviceName == bootstrap.fieldValueService.serviceName && inScope) {
                val localFieldValue = bootstrap.fieldValueService.getById(dataShell.id)
                val localUpdateTime = localFieldValue?.updateTime ?: -1
                if (localUpdateTime > dataShell.updateTime) {
                    send(session, ISlave::dataUpdate, localFieldValue!!, serviceName)
                }
                if (localUpdateTime < dataShell.updateTime) {
                    send(session, ISlave::fieldValueAskFor, DataShell(dataShell.id, localUpdateTime), serviceName)
                }

            } else {
                if (inScope) {
                    bootstrap.service(serviceName!!).getById(dataShell.id)?.apply {
                        if (updateTime > dataShell.updateTime) {
                            send(session, ISlave::dataUpdate, this, serviceName)
                        }
                    }
                } else {
                    send(session, ISlave::dataDelete, dataShell.id, serviceName)
                }
            }
        }


        override fun serviceSyncFinishedFromSlave(session: WebSocketSession, content: String, serviceName: String?) {
            childScope(session).sync[serviceName]?.apply {
                forEach {
                    bootstrap.service(serviceName!!).getById(it)?.apply {
                        send(session, ISlave::dataUpdate, this, serviceName)
                    }
                }
                clear()
            }
            send(session, ISlave::serviceSyncFinishedFromMaster, "", serviceName)
        }

        override fun fieldValueUpdate(session: WebSocketSession, content: String, serviceName: String?) {
            val fieldValue: FieldValueStub = parse(content)
            bootstrap.fieldValueService.save(fieldValue, childInstance(session))
        }
    }
}