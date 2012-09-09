package projectComputing.MessageQueue ; 

/** 
  MessageQueue - very simple disk-backed message queue with multiple sources but one sink.
  2012 John Evershed, Kent Fitch, www.projectcomputing.com

 javac projectComputing/MessageQueue/MessageQueue.java
 java -classpath . projectComputing.MessageQueue.MessageQueue
**/

import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.* ;
import java.util.*;

public class MessageQueue {

	static final int DEFAULT_PORT_USED_BY_SOURCES = 6211 ;
	static final int DEFAULT_PORT_USED_BY_SINK = 6212 ;
	static final int DEFAULT_MAX_MEMORY_QUEUE_SIZE = 64 * 1000000 ;	// 64MB
	static final int DEFAULT_DISK_FILE_SIZE_DIVISOR = 4 ;

	static final String DEFAULT_DIRECTORY_NAME = "messageStore" ;

	static final int APPROX_PER_MESSAGE_MEMORY_OVERHEAD = 200 ; 	// approx java object overhead..

	static final int MINIMUM_RECORDS_PER_FILE = 100 ;	// dont close/delete current file when last message is taken unless we've written this number of records to the file.  A small number reduces replay-on-startup, but increases overheads.

	static public boolean DEBUG = true ; 							// extra system.err logging
	/** Message sources connect to this port.  Overrideable by system property -DmessageQueueSourcePort **/
	public int portUsedBySources ;

	/** Message sink connects to this port.  Overrideable by system property -DmessageQueueSinkPort **/
	public int portUsedBySink ;

	/** Very approximate max size in bytes of in memory message queue.  Overrideable by system property -DmaxMemoryQueueSize **/
	public int maxMemoryQueueSize ;

	/** Number to divide into maxMemoryQueueSize to give approx max disk file size; eg, 4 makes disk file 1 quarter the size. 
	    Overrideable by system property -DdiskFileSizeDivisor.
		Note:- bad things will happen if the (in-memory) message queue size is reduced below the file message size between
		runs if there are files persisted on disk - they wont fit in the allocated memory, so the allocated memory
		limit will be ignored.
    **/
	public int diskFileSizeDivisor ;

	/** Directory used for persisting messages.  Overrideable by system property -DmessageQueueDirectory **/
	public String messageQueueDirectoryName ;

	int messageQueueSize = 0 ; 		// approx size of currently in-memory queued messages
	final LinkedList<Message> messageQueue = new LinkedList<Message>() ;

	File messageQueueDirectory ;
	MessageQueueFile currentMessageQueueFile ;
	int maxDiskFileSize ; 			// derived from maxMemoryQueueSize and diskFileSizeDivisor
	
	boolean writingNewMessagesToMemory = true ;	// when false, we're not adding new messages to memory
	
	int in = 0 ;
	int out = 0 ;
	int acked = 0 ;

	public static void main(String args[]) throws Exception {

		new MessageQueue().begin()  ;
	}

	public MessageQueue() throws Exception {

		portUsedBySources = SetFromSystemProperty("messageQueueSourcePort", DEFAULT_PORT_USED_BY_SOURCES) ;
		portUsedBySink = SetFromSystemProperty("messageQueueSinkPort", DEFAULT_PORT_USED_BY_SINK) ;
		maxMemoryQueueSize = SetFromSystemProperty("maxMemoryQueueSize", DEFAULT_MAX_MEMORY_QUEUE_SIZE) ;
		diskFileSizeDivisor = SetFromSystemProperty("diskFileSizeDivisor", DEFAULT_DISK_FILE_SIZE_DIVISOR) ;
		messageQueueDirectoryName = SetFromSystemProperty("messageQueueDirectoryName", DEFAULT_DIRECTORY_NAME) ;
	}

	public void begin() throws Exception {

		maxDiskFileSize = maxMemoryQueueSize / diskFileSizeDivisor ;

		messageQueueDirectory = new File(messageQueueDirectoryName) ;
		if (messageQueueDirectory.exists()) {
			if (!messageQueueDirectory.isDirectory()) throw new Exception("messageQueueDirectory " + messageQueueDirectory + " is not a directory") ;
			writingNewMessagesToMemory = !readMessagesFromFiles(true) ;		// if there are any files here, we process them first
		}
		else if (!messageQueueDirectory.mkdirs()) throw new Exception("messageQueueDirectory " + messageQueueDirectory + " did not exists and could not be created") ;

		new Thread(new MessageQueueSourceListener(this)).start() ;		// start listening for the source(s)
		
		new Thread(new MessageQueueSinkListener(this)).start() ;		// start listening for the sink

		System.err.println("MessageQueue initiated, portUsedBySources: " + portUsedBySources + 
			", portUsedBySink: " + portUsedBySink + ", maxMemoryQueueSize: " + maxMemoryQueueSize +
			", diskFileSizeDivisor: " + diskFileSizeDivisor + ", maxDiskFileSize: " + maxDiskFileSize + 
			", messageQueueDirectoryName: " + messageQueueDirectoryName) ;
	}

	
	/** Try to read messages from disk into memory.  Returns true if there are MORE messages remaining to read on disk when we return. 

		When we start and there are disk file(s) containing messages, the sink may have seen some or all of them already.  So, some of the
		contents of the first file on disk MAY already have been seen by the sink.  atStartup is true called immediately at startup.

	**/

	private synchronized boolean readMessagesFromFiles(boolean atStartup) throws Exception {

		String contents[] = messageQueueDirectory.list() ;
		if (contents.length == 0) return false ;

		boolean anyFilesLeftUnprocessed = false ;

		Arrays.sort(contents) ; 		// sorting by name works.. name is monotonically increasing..
		for (String fn: contents) { 	// we're trying to find the first file containing something..
			File f = new File(messageQueueDirectory, fn) ;
			int flen = (int) f.length()  ;
			if (flen < 16L) {			// must be useless/corrupt..
				System.err.println("Unexpected short messageQueue file deleted: " + f) ;
				f.delete() ;
				continue ;
			}

			// got a file with contents..  If theres nothing in memory, or the WHOLE FILE will fit into the message queue, add it..
			if ((messageQueueSize == 0) 	// always read file if no messages in memory, regardless of file size, to allow sane restart of older, possibly "too big" files...
				|| (((flen * 2) + messageQueueSize) < maxMemoryQueueSize)) {
				MessageQueueFile.CopyFileToMemoryQueue(f, this, atStartup) ;	//  *2 for for object overhead...
				atStartup = false ;
			}
			else anyFilesLeftUnprocessed = true ; 	// dont write any new messages from sources to memory, as memory is full
		}
		return anyFilesLeftUnprocessed ;		
	}
			

	public synchronized void add(Message message) throws Exception {

		in++ ;
		writeToDisk(message) ;					// write every message to disk

		if (writingNewMessagesToMemory) {
			writeToMemory(message) ;
			if (messageQueueSize >= maxMemoryQueueSize) {	
				if (MessageQueue.DEBUG) System.err.println("Memory queue full, currentMessageQueueFile:" + currentMessageQueueFile + ", in:" + in + ", out:" + out + ", acked:" + acked) ;
				writingNewMessagesToMemory = false ;	// suspend writing to memory for now - wont resume until everything on disk has been processed
				if (currentMessageQueueFile != null) {			// close file so entire file is in memory
					currentMessageQueueFile.close(message) ;	// file will be deleted when last message is taken by sink from memory
					currentMessageQueueFile = null ;
				}
			}
		}
		notifyAll() ;							// maybe a sink is waiting
	}

	// caller is responsible for checking message will fit (or maybe caller doesnt care)
	// dont call this unless you know what you are doing - normal procedure is to invoke add()
	synchronized void writeToMemory(Message message) {	

		messageQueueSize += message.contents.length + APPROX_PER_MESSAGE_MEMORY_OVERHEAD ; 
		messageQueue.add(message) ;
	}

	private void writeToDisk(Message message) throws Exception {

		if (currentMessageQueueFile == null) currentMessageQueueFile = new MessageQueueFile(messageQueueDirectory) ;
		if (!currentMessageQueueFile.add(message, maxDiskFileSize))
			currentMessageQueueFile = null ; // we filled the file...					
	}

	public Message take() throws Exception {

		while (true) {
			synchronized(this) {
				Message m =  messageQueue.peek() ;
				if (m != null) { out++; return m ; }
				wait() ;
			}
		}
	}

	/* because there is only 1 sink, it is ALWAYS the head of the messageQueue that was sent */

	public void lastMessageSentSuccessfully() throws Exception {

		synchronized(this) {
			acked++ ;
			Message m = messageQueue.remove() ;
			messageQueueSize = messageQueueSize - m.contents.length - APPROX_PER_MESSAGE_MEMORY_OVERHEAD ;
			if (m.messageIsLastInThisFile != null) MessageQueueFile.Delete(m.messageIsLastInThisFile) ;
			if (messageQueue.isEmpty()) {	// we have exhausted the memory queue
				if (MessageQueue.DEBUG) System.err.println("Last message taken, queue empty, writingNewMessagesToMemory:"+writingNewMessagesToMemory+", messageQueueSize:"+messageQueueSize + ", in:" + in + ", out:" + out + ", acked:" + acked) ;
				if (writingNewMessagesToMemory)	{ // we were writing to disk and to memory

					// So there's nothing on disk to send, but everything in the current file HAS been sent, so
					// the current file can be deleted.  But it is likely that this last message was the
					// only record in the file, and we don't really want to keep opening and closing/deleting files
					// just because the sink is "keeping up".  So, only close and delete if we've written at least
					// MINIMUM_RECORDS_PER_FILE

					if ((currentMessageQueueFile != null) && (currentMessageQueueFile.in >=  MINIMUM_RECORDS_PER_FILE)) {
						currentMessageQueueFile.closeAndDelete() ;
						currentMessageQueueFile = null ;
					}
				}
				else { 		// we're not writing to memory - we must have been previously full
 	
					// We've just exhausted memory queue and we were in "processing disk file" mode.
					// Either there's more on disk to process, or we can start receiving from source into memory

					if (currentMessageQueueFile != null) {		// close currently open file - dont want to be reading and writing it!
						currentMessageQueueFile.close(null) ;
						currentMessageQueueFile = null ;
					}

					writingNewMessagesToMemory = !readMessagesFromFiles(false) ;
				}
			}
		}
	}

	static int SetFromSystemProperty(String propName, int defaultVal) {

		String s = System.getProperty(propName) ;
		if (s != null) 
			try {
				return Integer.parseInt(s) ;
			}
			catch (Exception e) {
				System.err.println("Invalid value for system property " + propName + " - defaulting to " + defaultVal) ;
			} ;
		return defaultVal ;
	}

	static String SetFromSystemProperty(String propName, String  defaultVal) {

		String s = System.getProperty(propName) ;
		return (s == null) ? defaultVal : s ;
	}
}

class MessageQueueFile {

	static final byte[] HEADER       = "HEADER  ".getBytes() ;
	static final byte[] VERSION      = "00000001".getBytes() ;
	static final byte[] MESSAGESTART = "MESSAGE ".getBytes() ;
	static final byte[] EOF          = "EOF     ".getBytes() ;

	static final int APPROX_PER_MESSAGE_FILE_OVERHEAD = 50 ; 		// approx file format overhead..

	static int seq ;
	static long lastTime ;

	File file ;
	int len = 0 ;
	BufferedOutputStream bos ; 
	int in = 0 ;

	MessageQueueFile(File messageQueueDirectory) throws Exception {		// create a new file

		long now = System.currentTimeMillis() ;
		if (now == lastTime) seq++ ;
		else {
			lastTime = now ;
			seq = 0 ;
		}
		String fileName = new Formatter().format("MQ-%016x-%04x", now, seq).toString() ;	// MQ-time-seq (seq is just in case computer is very fast and queue is very busy!
		file = new File(messageQueueDirectory, fileName) ;
		bos = new BufferedOutputStream(new FileOutputStream(file)) ;
		bos.write(HEADER) ;
		bos.write(VERSION) ;
		if (MessageQueue.DEBUG) System.err.println("Created MessageQueueFile " + file) ;
	}
	

	boolean add(Message message, int maxDiskFileSize) throws Exception {			// return true if more messages can be written to this file, else false

		bos.write(MESSAGESTART) ;

		// write message id (16 bytes) encoded as hex and the message contents length (an int) encoded as hex

	    String t = new BigInteger(1, message.id).toString(16) ;
		if (t.length() < 32) 
			bos.write("00000000000000000000000000000000".substring(0, 32 - t.length()).getBytes()) ;	// zero pad

		bos.write(t.getBytes()) ;		// id
		bos.write(new Formatter().format("%08x", message.contents.length).toString().getBytes()) ;		// contents length
		bos.write(message.contents) ;
		bos.flush() ;					// ensure it is persisted...
		in++ ;
		len += message.contents.length + APPROX_PER_MESSAGE_FILE_OVERHEAD ;
		if ((len + message.contents.length + APPROX_PER_MESSAGE_FILE_OVERHEAD) >= maxDiskFileSize) {	// checking to see if another message of the size we just got would fit
			close(message) ;			// close, marking the last message as the last in the file, which when taken, triggers deletion of the file
			return false ;
		}
		return true ;
	}

	void close(Message message) throws Exception {

		if (MessageQueue.DEBUG) System.err.println("close " + file + " total records in: " + in) ;
		if (message != null) message.setAsLastInFile(file) ;		// when this message is taken by sink, file can be deleted
		bos.write(EOF) ;
		bos.close() ;
		bos = null ;
		file = null ;
	}

	void closeAndDelete() throws Exception {

		if (MessageQueue.DEBUG) System.err.println("close and delete " + file + " total records in: " + in) ;
		bos.close() ;
		bos = null ;
		Delete(file) ;
		file = null ;
	}

	// all messages in the file are guaranteed to fit into the messageQueue

	static void CopyFileToMemoryQueue(File f, MessageQueue messageQueue, boolean possibleReplay) throws Exception {

		if (MessageQueue.DEBUG) System.err.println("Adding file to memory: " + f + ",possibleReplay="+possibleReplay) ;
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f)) ;

		byte header[] = Read(f, bis, 8) ;
		for (int i=0;i<8;i++) if (header[i] != HEADER[i]) throw new Exception("corrupt message queue file " + f + " detected in header " + new String(header)) ;
		byte version[] = Read(f, bis, 8) ;
		for (int i=0;i<8;i++) if (version[i] != VERSION[i])	throw new Exception("corrupt message queue file " + f + " detected in version " + new String(version)) ;

		int readCount = 0 ;
		Message lastMessage = null ;
		while (true) {
			byte messageStart[] = null ;
			try {
				messageStart = Read(f, bis, 8) ;
			}
			catch (Exception e) {	// file was just suddenly closed...  sort of normal.
				System.err.println("Unexpected EOF on file " + f + " - processing what we read..") ;
				break ;
			}
			
			boolean eof = true ;
			for (int i=0;i<8;i++) if (messageStart[i] != EOF[i]) { eof = false ; break ; }
			if (eof) break ;
			for (int i=0;i<8;i++) if (messageStart[i] != MESSAGESTART[i])
				throw new Exception("corrupt message queue file " + f + " detected in message start " + new String(messageStart)) ;

			// next 32 bytes are a message id encoded as hex

			byte[] messageId = new byte[16] ;
			for (int i=0;i<16;i++) {
				byte b1 = (byte) bis.read() ;	//0, 1, .. f
				byte b2 = (byte) bis.read() ;
				if ((b1 < '0') || (b1 > 'f') || (b2 < '0') || (b2 > 'f')) 
					throw new Exception("corrupt message queue file " + f + " detected in message hdr") ;
				messageId[i] = (byte) (((int) (b1 - '0')) * 16 + (int) (b2 - '0')) ;
			}

			// next 8 bytes are message len encoded as hex

			String len = new String(Read(f, bis, 8)) ;
			int messageLen = 0 ;
			try {
				messageLen = Integer.parseInt(len, 16) ;
			}
			catch (Exception e) {
				throw new Exception("corrupt message queue file " + f + " detected in message length " + len + ", e="+e) ;
			}

			byte messageContents[] = Read(f, bis, messageLen) ;
			if (messageContents  == null) {
				System.err.println("Truncated message in file " + f + " - contents truncated") ;
				break ;
			}			
			lastMessage = new Message(messageId, messageContents, possibleReplay) ;
			messageQueue.writeToMemory(lastMessage) ;				// guaranteed to fit
			readCount++ ;
		}
		if (lastMessage != null) lastMessage.setAsLastInFile(f) ;	// when this message is consumed, file can be deleted
		if (MessageQueue.DEBUG) System.err.println("file added, mq mem size:"+messageQueue.messageQueueSize + ", records read:"+readCount) ;
	}

	static byte[] Read(File f, BufferedInputStream bis, int sz) throws Exception {

		final byte buf[] = new byte[sz] ;
		int start = 0 ;
		int len = sz ;
		while (len > 0) {
			int i = bis.read(buf, start, len) ;
			if (i < 0) throw new Exception("truncated message file " + f) ;
			start += i ;
			len -= i ;
		}
		return buf ;
	}

	static void Delete(File messageQueueFile) {

			if (MessageQueue.DEBUG) System.err.println("Deleting message queue file " + messageQueueFile) ;
			if (!messageQueueFile.delete()) System.err.println("Failed to delete message queue file " + messageQueueFile) ;
	}
}


class MessageQueueSourceListener implements Runnable {

	private final MessageQueue messageQueue ;

	MessageQueueSourceListener(MessageQueue messageQueue) throws Exception {

		this.messageQueue = messageQueue ;
	}

	public void run() {		// our thread..

		try {
			Thread.currentThread().setName("MessageQueueSourceListener") ;
			ServerSocket serverSocket = new ServerSocket(messageQueue.portUsedBySources) ;
			while (true) {
				Socket socket = serverSocket.accept() ;
				new MessageQueueSource(messageQueue, socket) ;
			}
		}
		catch (Exception e) {
			System.err.println("Error in MessageQueueSourceListener:" + e) ;
			e.printStackTrace() ;
		}
	}
}

class MessageQueueSource implements Runnable {

	private final MessageQueue messageQueue ;
	private final Socket socket ;

	MessageQueueSource(MessageQueue messageQueue, Socket socket) {

		this.messageQueue = messageQueue ;
		this.socket = socket ;
		new Thread(this).start() ;
	}

	public void run() {		// per source thread..

		String remote = socket.getRemoteSocketAddress().toString() ;
		System.out.println("Source session started: " + remote) ;
		try {
			Thread.currentThread().setName("MessageQueueSource-" + remote) ;
			BufferedInputStream bis = new BufferedInputStream(socket.getInputStream()) ;
			BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream()) ;
			readFromSource(bis, bos) ;
			socket.close() ;
		}
		catch (Exception e) {
			System.err.println("Error in MessageQueueSource-" + remote + ":" + e) ;
			e.printStackTrace() ;
		}
		finally {
			try {
				socket.close() ;
			}
			catch (Exception e) { } 
		}
		System.out.println("Source session ended: " + remote) ;
	}

	void readFromSource(BufferedInputStream bis, BufferedOutputStream bos) throws Exception {

		/* simple protocol:
			message without an id: 'M', len (4 bytes), contents[len]
			message with    an id: 'I', id (16 bytes), len (4 bytes), contents[len]
			orderly eof          : 'E'
		
			The id is a java int serialised as bytes
			The id is just 16 bytes.
		*/

		while (true) {
			byte b = (byte) bis.read() ;
			if (b < 0) { 
				System.out.println("Unexpected EOF from source") ;
				break  ;
			}
			if (b == 'M') {
				int len = readLen(bis) ;
				byte[] contents = readBytes(bis, len) ;
				messageQueue.add(new Message(contents)) ; 	// construct a message with our id
				ack(bos) ;
			}
			else if (b == 'I') {
				byte[] id = readBytes(bis, 16) ;
				int len = readLen(bis) ;
				byte[] contents = readBytes(bis, len) ;
				messageQueue.add(new Message(id, contents, false)) ; 	// construct a message with their id
				ack(bos) ;
			}
			else if (b == 'E') {
				System.out.println("EOF received from source") ;
				break  ;
			}
			else throw new Exception("Unexpected message start: " + b) ;
		}
	}

	int readLen(BufferedInputStream bis) throws Exception {

		ByteBuffer bb = ByteBuffer.allocate(4) ;
		for (int i=0;i<4;i++) {
			int j = bis.read() ;
			if (j < 0) throw new Exception("eof reading length") ;
			bb.put((byte) j) ;
		}
		return bb.getInt(0) ;
	}

	byte[] readBytes(BufferedInputStream bis, int sz) throws Exception {

		final byte buf[] = new byte[sz] ;
		int start = 0 ;
		int len = sz ;
		while (len > 0) {
			int i = bis.read(buf, start, len) ;
			if (i < 0) throw new Exception("truncated contents") ;
			start += i ;
			len -= i ;
		}
		return buf ;
	}

	void ack(BufferedOutputStream bos) throws Exception {
		
		bos.write('Y') ;
		bos.flush() ;
	}
}


class MessageQueueSinkListener implements Runnable {

	private final MessageQueue messageQueue ;

	ServerSocket serverSocket ;
	String remote ;
	BufferedInputStream bis ;
	BufferedOutputStream bos ;


	MessageQueueSinkListener(MessageQueue messageQueue) throws Exception {

		this.messageQueue = messageQueue ;
	}

	public void run() {		// our thread..

		try {
			Thread.currentThread().setName("MessageQueueSinkListener") ;
			serverSocket = new ServerSocket(messageQueue.portUsedBySink) ;
			while (true) {
				
				try {
					processSink(serverSocket.accept()) ;
				}
				catch (Exception e) {
					System.err.println("Error in MessageQueueSinkListener processSink:" + e) ;
					e.printStackTrace() ;
					// reloop and try again
				}								
			}
		}
		catch (Exception e) {
			System.err.println("Error in MessageQueueSinkListener:" + e) ;
			e.printStackTrace() ;
		}
	}

	void processSink(Socket socket) throws Exception {

		remote = socket.getRemoteSocketAddress().toString() ;
		System.out.println("Sink session started: " + remote) ;
		
		// VERY simple protocol - we send them a message as soon as there is one and they ack its receipt (or drop the socket)
		// They may have seen the message before - on restart, we resend last uncompleted file.
		//
		// We send: 'M', id (16 bytes), len (4 bytes), contents[len]

		bis = new BufferedInputStream(socket.getInputStream()) ;
		bos = new BufferedOutputStream(socket.getOutputStream()) ;

		while (true) {
			try {
				Message message = messageQueue.take() ;
				send(message) ;
				messageQueue.lastMessageSentSuccessfully() ;
			}
			catch (Exception e) {
				System.err.println("Error in MessageQueueSinkListener send:" + e) ;
				System.out.println("Sink session ended: " + remote) ;
				e.printStackTrace() ;
				return ;
			}				
		}
	}

	void send(Message message) throws Exception {

		bos.write(message.possibleReplay ? 'R' : 'M') ;
		bos.write(message.id) ;
		bos.write(ByteBuffer.allocate(4).putInt(message.contents.length).array()) ;
		bos.write(message.contents) ;
		bos.flush() ;
		byte b = (byte) bis.read() ;	// wait for ack
		if (b == -1) throw new Exception("end of input reading sink ack") ;
		if (b != 'Y') throw new Exception("didnt get sink ack: " + b) ;
	}		
}

class Message {

	static final byte[] OUR_ID_BASE = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array() ;
	static long seq ;

	final byte[] id ;
	final byte[] contents ;
	final boolean possibleReplay ;

	File messageIsLastInThisFile ;

	Message(byte[] id, byte[] contents, boolean possibleReplay) {

		this.id = id ;
		this.contents = contents ;
		this.possibleReplay = possibleReplay ;
	}

	Message(byte[] contents) {	// need to generate our own id

		this.id = genId() ;
		this.contents = contents ;
		this.possibleReplay = false ;
	}

	static synchronized long incId() {

		return ++seq ;
	}

	byte[] genId() {

		byte[] id = new byte[16] ;
		for (int i=0;i<8;i++) id[i] = OUR_ID_BASE[i] ;
		byte[] ourSeqBytes = ByteBuffer.allocate(8).putLong(incId()).array() ;
		for (int i=0;i<8;i++) id[i+8] = ourSeqBytes[i] ;
		return id ;
	}

	void setAsLastInFile(File f) {

		messageIsLastInThisFile = f ;
	}
}
