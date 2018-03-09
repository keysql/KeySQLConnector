package keysql.connector;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class KeySQLClient extends Thread {
	private static final int DEFAULT_POOL_SIZE = 4;
	private static final int SUCCESS_SIZE = 4;
	private static final int WAIT_TIME = 20;
	private static final int REPEAT_TIMES = 4;
	private static final int READ_BUFFER_SIZE = 8192;
	private static final String CLIENT_VERSION = "keysql 0.1\n";
	private final AtomicBoolean running;
	private final AtomicBoolean closeChannels;
	private InetSocketAddress socketAddress;
	private final ConcurrentLinkedQueue<StatementSession> sessions;
	private final Selector selector;
	private final Queue<SocketChannel> openConnections;
	private final Map<SocketChannel, StatementSession> dataMapper;

	KeySQLClient(String host, short port, ConcurrentLinkedQueue<StatementSession> sessions)
			throws IOException, ConnectException {
		running = new AtomicBoolean(false);
		closeChannels = new AtomicBoolean(false);
		socketAddress = new InetSocketAddress(host, port);
		this.sessions = sessions;
		this.dataMapper = new HashMap<>();
		selector = initiateSelector();
		openConnections = new ArrayDeque<>(DEFAULT_POOL_SIZE);
		startPool();
	}

	@Override
	public void start() {
		running.set(true);
		super.start();
	}

	public void stop_running() {
		running.set(false);
	}

	public int activeConnections() {
		return dataMapper.size();
	}

	public int freeConnections() {
		return openConnections.size();
	}

	public boolean reconnect(String host, short port) {
		freeChannels();
		this.socketAddress = new InetSocketAddress(host, port);
		try {
			Thread.sleep(1000);
			closeChannels.set(false);
			for (int i = 0; i < DEFAULT_POOL_SIZE; ++i) {
				initiateConnection();
				System.out.println("added new channel");
			}
			Thread.sleep(5000);
			return !dataMapper.isEmpty();
		} catch (InterruptedException e) {
			return false;
		} catch (IOException e) {
			// Logger
			return false;
		}
	}

	@Override
	public void run() {
		int fail_times = 0;
		while (running.get()) {
			StatementSession session = null;
			try {
				checkCancelled();
				if (!sessions.isEmpty()) {
					if (openConnections.isEmpty()) {
						session = sessions.peek();
						initiateConnection();
					} else {
						SocketChannel channel = openConnections.poll();
						dataMapper.put(channel, sessions.poll());
					}
				}
				this.selector.select();
				// work on selected keys
				Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
				while (keys.hasNext()) {
					SelectionKey key = keys.next();
					// this is necessary to prevent the same key from coming up
					// again the next time around.
					keys.remove();
					if (!key.isValid()) {
						continue;
					}
					if (closeChannels.get()) {
						SocketChannel channel = (SocketChannel) key.channel();
						channel.close();
						key.cancel();
						System.out.println("Channel closed");
						continue;
					}
					if (key.isConnectable()) {
						if (establishConnection(key)) {
							SocketChannel channel = (SocketChannel) key.channel();
							openConnections.add(channel);
							dataMapper.put(channel, null);
						}
					} else if (key.isReadable()) {
						readData(key);
					} else if (key.isWritable()) {
						writeData(key);
					}
				}
				if (dataMapper.isEmpty()) {
					Thread.sleep(100);
				}
				fail_times = 0;
			} catch (IOException e) {
				if (++fail_times >= REPEAT_TIMES && session != null) {
					session = sessions.poll();
					session.setMessageHeader(new MessageHeader(ReplyMessage.EXIT_CODE_COMUNICATION_ERROR));
					session.setPromise();
				}
				// e.printStackTrace();
				// Logger.write("Exception: " + e.getMessage(), LogType.CLIENT);
			} catch (InterruptedException e) {
				// Logger.write("KeySQL client thread interrupted.", LogType.CLIENT);
			}
		}
		freeChannels();
	}

	private void startPool() throws IOException, ConnectException {
		for (int i = 0; i < DEFAULT_POOL_SIZE; ++i) {
			initiateConnection();
		}
		if (startPool(DEFAULT_POOL_SIZE) == 0) {
			throw new ConnectException();
		}
	}

	private void freeChannels() {
		closeChannels.set(true);
		dataMapper.clear();
		openConnections.clear();
	}

	private Selector initiateSelector() throws IOException {
		return SelectorProvider.provider().openSelector();
	}

	private SocketChannel initiateConnection() throws IOException {
		SocketChannel connection = SocketChannel.open();
		connection.configureBlocking(false);
		connection.connect(socketAddress);
		connection.register(selector, SelectionKey.OP_CONNECT);
		return connection;
	}

	private int startPool(int size) throws IOException {
		int i = 0, count = 0;
		while (i < size) {
			// wait for events
			this.selector.select();
			// work on selected keys
			Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
			while (keys.hasNext()) {
				SelectionKey key = keys.next();
				// this is necessary to prevent the same key from coming up
				// again the next time around.
				keys.remove();
				if (!key.isValid()) {
					continue;
				}
				if (key.isConnectable()) {
					++i;
					if (establishConnection(key)) {
						++count;
						openConnections.add((SocketChannel) key.channel());
					}
				}
			}
		}
		return count;
	}

	private void readData(SelectionKey key) {
		SocketChannel channel = (SocketChannel) key.channel();
		StatementSession session = dataMapper.get(channel);
		ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
		int numRead = -1;
		try {
			numRead = channel.read(buffer);
		} catch (IOException e) {
			// Logger.write("Failed to read request.", LogType.CLIENT);
			numRead = -1;
		}
		if (numRead == -1) {
			if (session != null) {
				session.setMessageHeader(new MessageHeader(ReplyMessage.EXIT_CODE_COMUNICATION_ERROR));
				session.setPromise();
			}
			this.dataMapper.remove(channel);
			// Socket socket = channel.socket();
			// SocketAddress remoteAddr = socket.getRemoteSocketAddress();
			// System.out.println("Connection closed by server: " + remoteAddr + ", got " + size + " messages.");
			try {
				channel.close();
			} catch (IOException e) {
			}
			key.cancel();
			return;
		}
		byte[] data = new byte[numRead];
		System.arraycopy(buffer.array(), 0, data, 0, numRead);
		String chunk = new String(data);
		if (session.getMessageHeader() == null) {
			int headerEnd = chunk.indexOf('\n');
			session.setMessageHeader(new MessageHeader(chunk));
			session.appendReply(chunk.substring(headerEnd + 1), numRead - headerEnd);
		} else {
			session.appendReply(chunk, numRead);
		}
		if (session.complete()) {
			dataMapper.put(channel, null);
			openConnections.add(channel);
			key.interestOps(SelectionKey.OP_WRITE);
		}
	}

	private void writeData(SelectionKey key) {
		SocketChannel channel = (SocketChannel) key.channel();
		StatementSession session = dataMapper.get(channel);
		try {
			if (session == null) {
				return;
			}
			ByteBuffer buffer = ByteBuffer.wrap(
					(session.getMessageHeader().toString() + session.getRequest()).getBytes());
			channel.write(buffer);
			buffer.clear();
			session.setMessageHeader(null);
			key.interestOps(SelectionKey.OP_READ);
		} catch (IOException e) {
			if (session != null) {
				session.setMessageHeader(new MessageHeader(ReplyMessage.EXIT_CODE_COMUNICATION_ERROR));
				session.setPromise();
			}
			this.dataMapper.remove(channel);
			// Socket socket = channel.socket();
			// SocketAddress remoteAddr = socket.getRemoteSocketAddress();
			// Logger.write("("Connection closed by server: " + remoteAddr);
			try {
				channel.close();
			} catch (IOException e1) {
				// Logger.write("Failed to close channel normally.", LogType.CLIENT);
			}
			key.cancel();
			// Logger.write("Failed to write request.", LogType.CLIENT);
		}
	}

	private Boolean establishConnection(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		try {
			if (channel.finishConnect()) {
				// handshake
				key.interestOps(SelectionKey.OP_WRITE);
				ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
				byte[] message = new String(CLIENT_VERSION).getBytes();
				ByteBuffer buffer = ByteBuffer.wrap(message);
				channel.write(buffer);
				buffer.clear();
				int times = 0;
				key.interestOps(SelectionKey.OP_READ);
				do {
					channel.read(readBuffer);
					if (readBuffer.position() != SUCCESS_SIZE) {
						sleep(WAIT_TIME * (times + 1));
					}
				} while (readBuffer.position() != SUCCESS_SIZE && ++times < REPEAT_TIMES);
				if (readBuffer.position() == SUCCESS_SIZE) {
					byte[] data = new byte[SUCCESS_SIZE - 1];
					System.arraycopy(readBuffer.array(), 0, data, 0, SUCCESS_SIZE - 1);
					if (Short.valueOf(new String(data)) == ExitStatus.EXIT_SUCCESS.valueOf()) {
						key.interestOps(SelectionKey.OP_WRITE);
						// Logger.write("Connection established.", LogType.CLIENT);
						return true;
					}
				}
				// Logger.write("Failed to establish connection.", LogType.CLIENT);
				key.channel().close();
				key.cancel();
				return false;
			}
		} catch (IOException e) {
			// Logger.write("Failed to establish connection.", LogType.CLIENT);
			key.channel().close();
			key.cancel();
			throw e;
			// return false;
		} catch (InterruptedException e) {
			// Logger.write("KeySQL client thread interrupted", LogType.CLIENT);
		}
		return true;
	}

	private void checkCancelled() {
		for (Map.Entry<SocketChannel, StatementSession> entry : dataMapper.entrySet()) {
			StatementSession session = entry.getValue();
			if (session != null && session.promiseCancelled()) {
				SocketChannel channel = entry.getKey();
				MessageHeader header = new MessageHeader(0,
						session.getMessageHeader().getUserId(),
						session.getMessageHeader().getClientMessageId(),
						TransactionCommand.ABORT);
				ByteBuffer buffer = ByteBuffer.wrap(header.toString().getBytes());
				try {
					channel.write(buffer);
				} catch (IOException e) {
					// Logger.write("Failed to abort transaction.", LogType.CLIENT);
					try {
						channel.close();
					} catch (IOException e1) {
						// Logger.write("Failed to close channel normally.", LogType.CLIENT);
					}
				}
			}
		}
	}
}
