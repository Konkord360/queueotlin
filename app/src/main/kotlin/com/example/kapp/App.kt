package com.example.kapp

import java.net.ServerSocket
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
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

// What about schema definition?
// If schema is defined structually - JSON or AVRO - I dont need to treat it as an ojbect
// Just validate that all fields defined as obligatory in schema are present in the message
// Simple schema definition - Kafka doesn't do schema definition - this is additional feature
// - that speaks more in favor of just a map of topics
data class Topic(val name: String, val messages: List<Message>) //, val schema: String) // ??

var topicsToChannels = mapOf<String, Channel<Message>>()

@Volatile
var topics = mapOf<String, List<String>>()

enum class Intent(val intentStrign: String){
    POLL("POLL"),
    WRITE("WRITE")
}

//Message shouldn't be send to the producer - there should be a way of knowing who sent the message
// (groupId?) 
data class Message(val intent: Intent, val topic: String, val message: String)

val server: ServerSocket = ServerSocket(9090)

var messages = listOf<Message>()

// does it even make sens to use one? potentially Causing a bottle neck at the point of sending messages
// Multiple coroutines write to a channel and multiple receive from it - faster then checking wether list is appended
// Still need an offset for a consumer for reconeccts 
val messagesChannel = Channel<Message>(Channel.BUFFERED)

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
    clients = listOf()
    messages = listOf()
    topicsToChannels = mapOf()
}

// TODO lets do the writing to topics and pollings topics first
// FOR NOW message will be delivered in parts split by "|" sign 
// | Intent (READ/WRITE) | TOPIC | Partition (FUTURE) | Message
suspend fun handleConnection(connection: Socket) = coroutineScope{
    connection.use { socket -> 
        val input = BufferedReader(InputStreamReader(socket.getInputStream()))

        // we need to simultanously listen to sockets and write to them - without blocking eachoter 
        // Is it time for channels?
        while(true) {
            println("Server waiting for messages")
            val inputMessage = input.readLine() ?: break
            val message = parseMessage(inputMessage)
            
            // Additional message for topic creation / deletion?
            if(message.intent == Intent.POLL) {
                // client is polling a message from topic
                launch(Dispatchers.IO) {
                    handlePoll(connection, message.topic)
                }
            } else { 
                // client wants to write to the topic
                launch(Dispatchers.IO) {
                    handleWrite(message)
                    println("Finished hanling of user message")
                }
            }


            // so we need to implement long polling - 
            //with(output) {
                //    write("Hello")
                //    newLine()
                //    flush()
                //}
            }
        // while loop to read all messages until dissconnect / disconnection message
    }
}

// Channel for each topic?
suspend fun handlePoll(connection: Socket, topic: String) = coroutineScope {
    println("Handling long poll from $connection on $topic")
    with(BufferedWriter(OutputStreamWriter(connection.getOutputStream()))) {
        try {
            withTimeout(2000) { 
                println("waiting for message on a channel")
                // xdd topic validation
                // Should be list of topics with messages that gets persisted periodically
                // along with the hashMap of topics and channels
                // Each topic should have its own channel(Maybe bad usage of the channels - will see)
                if (topic in topicsToChannels.keys) {
                    val message = topicsToChannels[topic]!!.receive()
                    println("Message on channerl received $message")
                    write("OK|${message.message}")
                    newLine()
                    flush()
                } else {
                    println("Topic does not exist")
                    write("OK|TOPIC_DOES_NOT_EXIST")
                    newLine()
                    flush()

                }

               // val message = messagesChannel.receive()
               // if (message.topic == topic){
               //     println("Message on channerl received $message")
               //     write("OK|${message.message}")
               //     newLine()
               //     flush()
               // }
            }
        } catch(e: TimeoutCancellationException){
            println("No messages during the long poll")
            write("OK|NO_MESSAGES")
            newLine()
            flush()
        }
    }
}

suspend fun handleWrite(message: Message) {
    println("Sending write to a channel")
    if (!(message.topic in topics.keys)) {
        println("Message sent to not existing topic")
        println("Creating new topic: ${message.topic}")
        // probably can be just one data structure
        topics = topics + (message.topic to listOf(message.message))
        val topicChannel = Channel<Message>(Channel.BUFFERED)
        topicsToChannels = topicsToChannels + (message.topic to topicChannel)
        topicChannel.send(message)
        println("Current list of topics: $topics")
    }
    //messagesChannel.send(message)
    messages = messages + message
    println("Sent user message to a channel")
    //messagesChannel.close()
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
