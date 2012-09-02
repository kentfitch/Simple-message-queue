package projectComputing.MessageQueue ; 

import java.io.*;
import java.nio.* ;

// java -classpath . projectComputing.MessageQueue.DemoSource 127.0.0.1 6211 

public class DemoSource {

	// Demo source showing usage of the MessageQueueWriter class to put messages on a message queue.
	
	public static void main(String args[]) throws Exception {

		if (args.length != 2) Usage() ;

		// use the messageQueue host and source port to create a MessageQueueReader
		MessageQueueWriter mqWriter = new MessageQueueWriter(args[0], Integer.parseInt(args[1])) ;
		System.out.println(mqWriter.toString()) ;

		// send 2 messages with an id
		
		ByteBuffer id = ByteBuffer.allocate(16) ;		// for our id
		byte[] idBytes = id.array() ;

		id.putLong(0, 0L) ;		id.putLong(8, 1L) ;		// id 00...00 01
		mqWriter.write(idBytes, "Hello world!".getBytes()) ;

		id.putLong(8, 2L) ;								// id 00...00 02
		mqWriter.write(idBytes, "Hello world, again..".getBytes()) ;

		// now send 2 messages without an id (the messageQueue will create ids for these)

		mqWriter.write("Hello world for the 3rd time".getBytes()) ;
		mqWriter.write("Goodbye world!".getBytes()) ;

		mqWriter.close() ;
		System.out.println(mqWriter.toString()) ;
	}

	static void Usage() {

		System.out.println("DemoSource command line parms are:\n" +
			"Message queue server ip address\n" +
			"Message queue server source port\n" +
			"eg java projectcomputing.MessageQueue.DemoSource 127.0.0.1 6211") ;
			System.exit(-1) ;
	}
}		
