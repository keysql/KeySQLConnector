package keysql.connector;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

//singleton class that communicate with KeySQL server
//1. instance must be created with createInstance method 
//2. Query can be submitted: Connector.getInstance().submit
//3. To reconnect pair methods can be used: Connector.getInstance().stopClient(), 
//   then Connector.getInstance().startClient
final public class Connector {
	private static Connector instance = null;
	private static final String LOCAL_ADDRESS = "0.0.0.0"; 
	private static AtomicLong messageId;
	private KeySQLClient client = null;
	private static Object mutex = new Object();
	private String host = null;
	private short port = 0;
	
	private ConcurrentLinkedQueue<StatementSession> sessions;
	
	
	private Connector(String host,short port) {
		messageId = new AtomicLong(0);
		sessions = new ConcurrentLinkedQueue<>();
		this.host = host;
		this.port = port;
	}
	
	private Connector(short port) throws ConnectException, IOException {
		this(LOCAL_ADDRESS,port);
	}
	
	 
	public static Connector getInstance() {
		return instance;
	}
	
	public static Connector createInstance(String host,short port) {
		if (instance != null) {
			throw new RuntimeException("Instance initialized already.");
		}
		synchronized (mutex) {
			instance = new Connector(host, port);
		}
		return instance;
	}
	
	public static Connector createInstance(short port) throws IOException {
		return createInstance(LOCAL_ADDRESS, port);
	}
	
	public void stopClient() throws InterruptedException {
		synchronized (mutex) {
			if (client != null) {
				client.stop_running();
				client.join();
				client = null;
			}
		}
	}
	
	public boolean startClient(String host,short port) throws IOException {
		if (client != null && client.isAlive())
			return false;
		synchronized (mutex) {
			if (client == null) {
				client = new KeySQLClient(host,port,sessions);
				client.start();
			}
		}
		return client != null && client.isAlive();
	}
	
	public CompletableFuture<ReplyMessage> submit(int userId, final String query,boolean jsonReply) throws IOException {
		if (this.client == null)
			this.startClient(this.host,this.port);
		return processScriptQuery(userId,query,jsonReply);
	}
	
	public CompletableFuture<ReplyMessage> submit(int userId, final String query) throws IOException {
		return submit(userId, query,false);
	}
	
	public ReplyMessage[] submitScript(int userId, final String[] script) throws InterruptedException, ExecutionException {
		ReplyMessage[] r = new ReplyMessage[script.length];
		int pos = 0;
		for (String query : script) {
			CompletableFuture<ReplyMessage> promise = new CompletableFuture<ReplyMessage>();
			sessions.add(new StatementSession(userId, messageId.addAndGet(1),query,false,promise));
			r[pos++] = promise.get();
		}
		return r;
	}
	private CompletableFuture<ReplyMessage> processScriptQuery(int userId, final String query,boolean jsonReply) 
	{
		String[] script = ConnectorHelper.createScript(query);
		CompletableFuture<ReplyMessage> lastPromise = new CompletableFuture<ReplyMessage>();
		StatementSession lastSession = 
				new StatementSession(userId, messageId.addAndGet(1),script[script.length - 1],jsonReply,lastPromise);
		for (int i = 0; i < script.length - 1; ++i) {
			CompletableFuture<ReplyMessage> promise = new CompletableFuture<ReplyMessage>();
			sessions.add(new StatementSession(userId, messageId.addAndGet(1),script[i],jsonReply,promise));
			try {
				ReplyMessage msg = promise.get();
				lastSession.getReplyBuffer().append(msg.getReply()).append("\n\n");
				if (ReplyMessage.EXIT_CODE_COMUNICATION_ERROR == msg.getExitCode()) {
					stopClient();
					lastSession.setPromise();
					return lastPromise;
				}
			} catch (InterruptedException | ExecutionException e) {
				//e.printStackTrace();
			}
		}
		sessions.add(lastSession);
		return lastPromise;
	}
}
