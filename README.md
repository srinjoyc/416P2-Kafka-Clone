# 416P2-Kafka-Clone
A simplified version of the kafka protocol.

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
Input a message:hello world
Response : &{ID:k1 Type:text Text:succeed}
cmd: 

```

kafka:
```
Listening provider at : 127.0.0.1:10001
Provider Msg: &{ID:P1 Type:text Text:hello world}
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
Input file name:msg.txt
Response : &{ID:k1 Type:text Text:succeed}
Response : &{ID:k1 Type:text Text:succeed}
Response : &{ID:k1 Type:text Text:succeed}
Response : &{ID:k1 Type:text Text:succeed}
Response : &{ID:k1 Type:text Text:succeed}
cmd: 

```

kafka:
```
Listening provider at : 127.0.0.1:10001
Provider Msg: &{ID:P1 Type:text Text:1}
Provider Msg: &{ID:P1 Type:text Text:2}
Provider Msg: &{ID:P1 Type:text Text:3}
Provider Msg: &{ID:P1 Type:text Text:4}
Provider Msg: &{ID:P1 Type:text Text:5}
```
