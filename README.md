# 416P2-Kafka-Clone
A simplified version of the kafka.

## Topology
![img](https://github.com/srinjoyc/416P2-Kafka-Clone/blob/master/topology.png?raw=true)


### Step 1: Run cluster of Manager:
```
cd manager
bash m1.sh
# bash m2.sh # run in a different terminal
# bash m3.sh 
cd ..
```

### Step 2: Run cluster of Borker:
```
cd Broker
bash b1.sh 
# bash b2.sh # run in a different terminal
# bash b3.sh 
# bash b4.sh 
# bash 53.sh 
cd ..
```

### Step 3: Run provider by script:
```
cd provider
bash topic.sh
bash append.sh
```
