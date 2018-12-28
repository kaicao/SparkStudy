### Spark streaming documentation  
https://spark.apache.org/docs/latest/streaming-programming-guide.html

### Start TCP server using Netcat
Example for start Netcat TCP server on localhost port
9999
```
nc -lt 9999
```

### Run application
Go to <b>TCPSocketStreamMain</b> and run the main using IntelliJ. 

This will start the TCP reader on localhost port 9999.

<b>TCPSocketStreamMain</b> will summarize words counts it received from the tcp channel every 10s.