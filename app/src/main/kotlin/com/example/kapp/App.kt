package com.example.kapp

import java.net.ServerSocket
import kotlinx.coroutines.*
import sun.rmi.server.Dispatcher
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket
import java.net.SocketException
import kotlin.uuid.Uuid


@Volatile
var clients = listOf<Client>()

data class Client(val connection: Socket, val groupID: String)

@Volatile
var topic = mapOf<String, Message>()

enum class Intent(val intentStrign: String){
    POLL("POLL"),
    WRITE("WRITE")
}

data class Message(val intent: Intent, val topic: String, val message: String)

val server: ServerSocket = ServerSocket(9090)

val messages = listOf<Message>()

@kotlin.uuid.ExperimentalUuidApi
suspend fun main() {
    startServer(server)
}

@kotlin.uuid.ExperimentalUuidApi
suspend fun startServer(server: ServerSocket) = coroutineScope {
    println("Server started at socket 9090")
    while(!server.isClosed) {
        try {
            val socketConnection = server.accept()
            clients = clients + Client(socketConnection, Uuid.random().toString())
            println("Accepted client: $socketConnection")
            println("Current clients: $clients")
            launch(Dispatchers.IO) {
                handleConnection(socketConnection)
            }
        } catch (e: SocketException) {
            println("Server is suddenly closed")
        }
    }
}

fun stopServer(server: ServerSocket) {
    println("Stopping server...")
    server.close()
}

// FOR NOW message will be delivered in parts split by "|" sign 
// | Intent (READ/WRITE) | TOPIC | Partition (FUTURE) | Message
suspend fun handleConnection(connection: Socket) {
    connection.use { socket -> 
        val input = BufferedReader(InputStreamReader(socket.getInputStream()))
        val output = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))

        // we need to simultanously listen to sockets and write to them - without blocking eachoter 
        // Is it time for channels?
        while(true) {
            println("Server waiting for messages")
            val inputMessage = input.readLine() ?: break
            val message = parseMessage(inputMessage)

            if(message.intent == Intent.POLL) {
                // client is polling a message from topic
                handlePoll()
            } else { 
                // client wants to write to the topic
                handleWrite()
            }


            // so we need to implement long polling - 
            with(output) {
                write("Hello")
                newLine()
                flush()
            }
        }
        // while loop to read all messages until dissconnect / disconnection message
    }
}

fun handlePoll() {

}

fun handleWrite() {

}

fun parseMessage(msg: String) : Message {
    println("Parsing user message: $msg")
    val messageContents = msg.split('|')
    println(messageContents)
    // the problem is there that such message should not cause an exception on the server but just return an error to the 
    // user - bad check
    check ( messageContents.size == 3 ) {"Message has to have 3 elements"}

    return Message(Intent.valueOf(messageContents[0]), messageContents[1], messageContents[2])
}
