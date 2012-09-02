package projectComputing.MessageQueue ; 

import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.* ;

// java -classpath . projectComputing.MessageQueue.LoadTestSink 127.0.0.1 6212 sinkFile

public class LoadTestSink {

	// load-test sink that receives messages, dumps them to a file.  ctrl-c to quit
	
	public static void main(String args[]) throws Exception {

		if (args.length != 3) Usage() ;
		
		String serverAddr = args[0] ;
		int sinkPort = Integer.parseInt(args[1]) ;
		String outputFile = args[2] ; 

		MessageQueueReader mqReader = new MessageQueueReader(serverAddr, sinkPort) ;

		System.out.println("connected to " + mqReader.toString()) ;

		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile)) ;
		System.out.println("Writing to file " + outputFile) ;

		while (true) {

			ReceivedMessageQueueMessage mqm = mqReader.read() ;

			fos.write(new String("Message " + mqReader.count + ", len " + mqm.contents.length + 
				", id " +  mqm.idAsString() + 
				((mqm.possiblyReplayed) ? ", possiblyReplayed" : "") + "\n").getBytes()) ;
			fos.write(mqm.contents) ;
			fos.write("\n".getBytes()) ;
			fos.flush() ;
		
			if ((mqReader.count % 1000) == 0) System.out.println(". " + (mqReader.count)) ;
			//else System.out.print(".") ;
		}
	}

	static void Usage() {

		System.out.println("LoadTestSink command line parms are:\n" +
			"Message queue server ip address\n" +
			"Message queue server sink port\n" +
			"File to receive messages\n" +
			"eg java projectcomputing.MessageQueue.LoadTestSink 127.0.0.1 6212 sinkFile") ;
			System.exit(-1) ;
	}
}

