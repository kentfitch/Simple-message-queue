package projectComputing.MessageQueue ; 

// java -classpath . projectComputing.MessageQueue.DemoSink 127.0.0.1 6212 sinkFile

public class DemoSink {

	// Demo sink showing simple pattern using the MessageQueueReader and ReceivedMessageQueueMessage classes
	// to receive messages from a message queue.
	
	public static void main(String args[]) throws Exception {

		if (args.length != 2) Usage() ;

		// use the messageQueue host and sink port to create a MessageQueueReader
		MessageQueueReader mqReader = new MessageQueueReader(args[0], Integer.parseInt(args[1])) ;
		System.out.println("Connected to " + mqReader.toString()) ;

		// receive and display messages...

		while (true) {
			ReceivedMessageQueueMessage message = mqReader.read() ;
			System.out.println("Received message sequence " + mqReader.count + ", len " + message.contents.length + 
				", id " +  message.idAsString() + 
				((message.possiblyReplayed) ? ", possiblyReplayed" : "")) ;
			System.out.println("contents: " + message.contentsAsString()) ;
		}
	}

	static void Usage() {

		System.out.println("DemoSink command line parms are:\n" +
			"Message queue server ip address\n" +
			"Message queue server sink port\n" +
			"eg java projectcomputing.MessageQueue.DemoSink 127.0.0.1 6212") ;
			System.exit(-1) ;
	}
}
