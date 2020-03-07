from time import sleep
from ftqueue import FTQueue 
from threading import Thread

import time
import json
import requests 
import queue
import socket
import sys
import threading

globalCounter = 0
coordinator = 1
peerList = ["128.110.153.97","128.110.153.81","128.110.153.85","128.110.153.89","128.110.153.104"]
listeningPort = 10001
serverIp = "128.110.153.85"
coordinatorIp = "128.110.153.97"
globalSequenceQueryPort = 10002
clientPort = 10003
bufferSize = 1024
global_sequence_number = 1

messageBuffer = list()
logBuffer = list()
clientQueue = queue.Queue()

def fetchCoordinatorIP():
    return peerList[globalCounter % len(peerList)]

def place_message_in_buffer(id):
    global messageBuffer
    for i in range(len(messageBuffer) - 1):
        if id > messageBuffer[i] and id < messageBuffer[i+1]:
            messageBuffer = messageBuffer[:i+1] + [id] + messageBuffer[i+1:]
            return

    messageBuffer.append(id)

def add_to_buffer(message_id):
    place_message_in_buffer(message_id)

def purge_item_buffer():
    global messageBuffer
    messageBuffer.pop(0)

def event_listener():
    global globalCounter
    
    # Define Queue
    queue = FTQueue()
    queue_id = queue.qCreate('message', 100)

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind the socket to the port
    server_address = (serverIp, listeningPort)
    # Listen for incoming connections
    print('starting event listener up on', server_address)
    UDPServerSocket.bind(server_address)
    while True:
        # Wait for a connection
        receivedData = UDPServerSocket.recvfrom(bufferSize)
        message = receivedData[0].decode('utf-8')
        destIp = receivedData[1][0]
        print('received', message)
        if (message.split('-')[0] == 'message'):
            if (int(message.split('-')[1]) <= globalCounter+1):
                print('directly Queued')
                # Log Messages in the Buffer
                logBuffer.append(int(message.split('-')[1]))
                incrementGlobalCounter()
                # check message buffer
                while (len(messageBuffer)>0):
                  if (messageBuffer[0] == globalCounter+1):
                    logBuffer.append(int(messageBuffer[0]))
                    purge_item_buffer()
                  else:
                    coordinatorIp = fetchCoordinatorIP()
                    send_message("recovery-"+str(globalCounter+1), coordinatorIp)
                    break
                # Push Message to fault tolerant Queue
                queue.qPush(queue_id, message)
            else:
                print('need buffer and recovery')
                id = message.split('-')[1]
                add_to_buffer(int(message.split('-')[1]))
                coordinatorIp = fetchCoordinatorIP()
                send_message("recovery-"+str(globalCounter+1), coordinatorIp)
        elif (message.split('-')[0]=='globalSequence'):
            send_globalSequence(destIp)
        elif (message.split('-')[0]=='recovery'):
            # find message and send it
            id = message.split('-')[1]
            send_message("message-"+id+"-"+serverIp, destIp)
            # purge_item_buffer()
        print("logBuffer", logBuffer)
        print("messageBuffer", messageBuffer)

def globalSequence_listener():
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind the socket to the port
    server_address = (serverIp, globalSequenceQueryPort)
    # Listen for incoming connections
    print('starting coordinator listener up on', server_address)
    UDPServerSocket.bind(server_address)
    while True:
        # Wait for a connection
        receivedData = UDPServerSocket.recvfrom(bufferSize)
        message = receivedData[0].decode('utf-8')
        destIp = receivedData[1][0]
        print('gS listener msg received', message, destIp)
        if (message.split('-')[0]=='globalSequence'):
            send_globalSequence(destIp)
        elif (message.split('-')[0]=='returnGlobalSequence'):
            if clientQueue.empty():
                send_testMessage(message.split('-')[1], "hello")
            else:
                clientMessage = clientQueue.get()
                send_testMessage(message.split('-')[1], clientMessage)

def send_globalSequence(destIp):
    try:
        incrementGlobalCounter()
        UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        UDPClientSocket.settimeout(10)
        server_address = (destIp, globalSequenceQueryPort)
        UDPClientSocket.sendto(str.encode("returnGlobalSequence-"+str(globalCounter+1), "utf-8"), server_address)
    except socket.timeout as err:
        serverTime = 'sendLoss'

def send_message(message, destIp):
    try:
        UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        UDPClientSocket.settimeout(10)
        server_address = (destIp, listeningPort)
        UDPClientSocket.sendto(str.encode(message, "utf-8"), server_address)
    except socket.timeout as err:
        serverTime = 'sendLoss'

def send_broadcast(message):
    for i in peerList:
        try:
            UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
            UDPClientSocket.settimeout(10)
            server_address = (i, listeningPort)
            UDPClientSocket.sendto(str.encode(message,'utf-8'), server_address)
        except socket.timeout as err:
            serverTime = 'sendLoss'

def incrementGlobalCounter():
    global globalCounter
    globalCounter = globalCounter + 1

def getGlobalNumber():
    try:
        UDPSendSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        UDPSendSocket.settimeout(10)
        coordinatorIp = fetchCoordinatorIP()
        server_address = (coordinatorIp, globalSequenceQueryPort)
        UDPSendSocket.sendto(str.encode("globalSequence-0", "utf-8"), server_address)
        print('gS msg sent', server_address)
        ##
    except socket.timeout as err:
        serverTime = 'sendLoss'

def send_testMessage(counterValue, message):
    ## message syntax
    ## tag-globalSequence-originatorIp-content
    message = "message-"+str(counterValue)+"-"+serverIp+"-"+message
    #print(message)
    send_broadcast(message)

def client_listener():
    global clientQueue
    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind the socket to the port
    server_address = (serverIp, clientPort)
    # Listen for incoming connections
    print('starting event listener up on', server_address)
    UDPServerSocket.bind(server_address)
    while True:
        # Wait for a connection
        receivedData = UDPServerSocket.recvfrom(bufferSize)
        message = receivedData[0].decode('utf-8')
        destIp = receivedData[1][0]
        clientQueue.put(message)
        getGlobalNumber()

if __name__ == '__main__':
    print("Starting Logging Server")
    threading.Thread(target=event_listener).start()
    threading.Thread(target=globalSequence_listener).start()
    threading.Thread(target=client_listener).start()
    print('Running Consumer..')
    time.sleep(3)
    #getGlobalNumber()
    time.sleep(3)
    #getGlobalNumber()
    time.sleep(3)
    #getGlobalNumber()
