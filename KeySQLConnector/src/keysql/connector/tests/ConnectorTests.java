package keysql.connector.tests;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CompletableFuture;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import keysql.connector.Connector;
import keysql.connector.ConnectorHelper;
import keysql.connector.ReplyMessage;
import keysql.connector.StatementType;

public class ConnectorTests {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Connector.createInstance((short) 5556);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Connector.getInstance().stopClient();
	}

	@Test
	public void oneQueryTest() {
		assertNotNull(Connector.getInstance());
		try {
			for (int iter = 0; iter < 100; ++iter) {
				final CompletableFuture<ReplyMessage> future = Connector.getInstance()
						.submit(0,"select * from world_bank_store limit 100;select * from Office_Rentals;select * from billiard_store;\n\n");
				final ReplyMessage reply = future.get();
				if (reply.getExitCode() != reply.EXIT_CODE_COMUNICATION_ERROR) {
					System.out.print("\n\nReply:\n" + reply.getReply() + "\n");
				} else {
					System.out.print("Communication error with KeySQL server\n");
					break;
				}
			}
		} catch (final Exception ex) {
			ex.printStackTrace();
		}
	}

	@Test
	public void multipleQueryTest() {
		assertNotNull(Connector.getInstance());
		final String[] queries = { "show catalog universe;", "show keyobject rooms from catalog universe",
				"show store billiard_store", "select * from billiard_store",
				"select ball from billiard_store where :color = 'red'",
				"select ball, brand from billiard_store where :color = 'red'",
				"select ball, brand, Style from billiard_store where :color = 'red' and :num > 1" };
		final Runnable test = () -> {
			try {
				for (int iter = 0; iter < 10; ++iter) {
					for (int i = 0; i < queries.length; ++i) {
						final CompletableFuture<ReplyMessage> future = 
								Connector.getInstance().submit(0,queries[i]);
						System.out.print(
								Thread.currentThread().getName() + ":" + i + ". Reply: " + future.get().getReply());
					}
				}
			} catch (final Exception ex) {
				ex.printStackTrace();
			}
		};
		final Thread[] tests = new Thread[10];
		for (int i = 0; i < tests.length; ++i) {
			tests[i] = new Thread(test, "test" + i);
			tests[i].start();
		}
		try {
			Thread.sleep(1000);
			for (final Thread test2 : tests) {
				test2.join();
			}
		} catch (final InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void scriptTest() {
		assertNotNull(Connector.getInstance());
		final String[] queries = { "show catalog universe; --this is a comment;\n",
				"show keyobject rooms from catalog universe;", "show store billiard_store;",
				"select * from billiard_store;\n", "select ball from billiard_store where :color = 'red';",
				"select ball, brand from billiard_store where :color = 'red';",
				"select ball, brand, Style from billiard_store\nwhere :color = 'red' and :num > 1;" };
		final StringBuilder buf = new StringBuilder();
		for (final String q : queries) {
			buf.append(q);
		}
		final String[] script = ConnectorHelper.createScript(buf.toString());
		final Runnable test = () -> {
			try {
				for (int iter = 0; iter < 100; ++iter) {
					final ReplyMessage[] replies = Connector.getInstance().submitScript(0,script);
					int i = 0;
					for (final ReplyMessage reply : replies) {
						System.out.print(Thread.currentThread().getName() + ":" + ++i + ". Reply: " + reply.getReply());
					}
				}
			} catch (final Exception ex) {
				ex.printStackTrace();
			}
		};
		final Thread[] tests = new Thread[10];
		for (int i = 0; i < tests.length; ++i) {
			tests[i] = new Thread(test, "test" + i);
			tests[i].start();
		}
		try {
			Thread.sleep(1000);
			for (final Thread test2 : tests) {
				test2.join();
			}
		} catch (final InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
