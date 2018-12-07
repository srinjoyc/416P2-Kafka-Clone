# 416P2-Kafka-Clone
A simplified version of the kafka.

## Topology
![img](https://github.com/srinjoyc/416P2-Kafka-Clone/topology.png)

## Instruction to use producer to send message to kafka routine:
run producer:
```shell
go run provider.go p1.json 
```
run kafka:
```shell
go run kafka.go k1.json 
```
There are 2 ways to generate message and send to kafka main routine:

#### input a message as you want

producer:
```
*******************
* Instuction: there are several kinds of operations
* msg: upload the message into connected kafka nodes
* file: upload the file to connected nodes
*******************
cmd: msg
Input topic: demo
Input partition: 1
Input message: hello world
Response : {kID:k1, status:succeed}
cmd: 

```

kafka:
```
Listening provider at : 127.0.0.1:10001
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:hello world}
```

####  input a message file name
producer:
```
*******************
* Instuction: there are several kinds of operations
* msg: upload the message into connected kafka nodes
* file: upload the file to connected nodes
*******************
cmd: file
Input topic: 1
Input partition list such as 1,2,3: 1
Input file name:msg.txt
Response : {kID:k1, status:succeed}
Response : {kID:k1, status:succeed}
Response : {kID:k1, status:succeed}
Response : {kID:k1, status:succeed}
Response : {kID:k1, status:succeed}
```

kafka:
```
Listening provider at : 127.0.0.1:10001
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:1}
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:2}
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:3}
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:4}
Receive Provider Msg: {pID:P1, type:Text, partition:1, text:5}
```
