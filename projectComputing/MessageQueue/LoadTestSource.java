package projectComputing.MessageQueue ; 

import java.io.*;
import java.net.*;
import java.nio.* ;

// java -classpath . projectComputing.MessageQueue.LoadTestSource 127.0.0.1 6211 0 y 4000 1000000

public class LoadTestSource {

	// load-test source that generates messages of a frequency and type determined by command line parms.  
	// ctrl-c to quit

	
	public static void main(String args[]) throws Exception {

		if (args.length != 6) Usage() ;
		
		String serverAddr = args[0] ;
		int serverPort = Integer.parseInt(args[1]) ;
		int sleep = Integer.parseInt(args[2]) ;
		boolean generateId = "y".equals(args[3]) ;
		int messageSize = Integer.parseInt(args[4]) ;
		int limit = Integer.parseInt(args[5]) ;

		MessageQueueWriter mqWriter = new MessageQueueWriter(serverAddr, serverPort) ;

		System.out.println(mqWriter.toString()) ;
		System.out.println("Each message sent is a dot, 100 per line. Ctrl-c to quit") ;
		
		long c = 0 ;
		byte contents[] = new byte[messageSize] ;
		for (int i=0;i<messageSize;i++) contents[i] = 'Z' ;
		ByteBuffer id = ByteBuffer.allocate(16) ;
		byte[] idBytes = id.array() ;

		while (true) {
			c++ ;
			id.putLong(0, c) ;
			id.putLong(8, c) ;

			if (generateId) {
				id.putLong(0, c) ;
				id.putLong(8, c) ;
				mqWriter.write(idBytes, contents) ;
			}
			else mqWriter.write(contents) ;


			if ((c % 1000) == 0) System.out.println(". " + c) ;
			//else System.out.print(".") ;

			if (sleep > 0) {
				try {
					Thread.currentThread().sleep(sleep) ;
				}
				catch (Exception e) { }
			}
			if (c == limit) break ;
		}
		System.out.println("Sent " + c + " messages") ;
		mqWriter.close() ;
		System.out.println(mqWriter.toString()) ;
	}



	static void Usage() {

		System.out.println("LoadTestSource command line parms are:\n" +
			"Message queue server ip address\n" +
			"Message queue server source port\n" +
			"Interval between message generation in millsec (0 means no interval)\n" +
			"Message id generated flag: y means we generate an id\n" +
			"Message contents size in bytes\n" +
		    "Message count (to send)\n" +
			"eg java projectcomputing.MessageQueue.LoadTestSource 127.0.0.1 6211 0 y 4000 1000000") ;
			System.exit(-1) ;
	}
}		
