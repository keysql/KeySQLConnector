package keysql.connector.tests;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import keysql.connector.Connector;
import keysql.connector.ConnectorHelper;
import keysql.connector.ReplyMessage;
import keysql.connector.StatementType;


import static org.junit.Assert.*;

import org.junit.Test;

public class ReconnectTest {

	@Test
	public void test() {
		try {
			Connector.createInstance("0.0.0.0",(short)5556);
			for (int iter = 0; iter < 10; ++iter) {
				boolean json = iter % 2 == 1;
				CompletableFuture<ReplyMessage> future = Connector.getInstance().submit(0,"select * from Office_Rentals", json);
				ReplyMessage reply = future.get();
				if (reply.getExitCode() != reply.EXIT_CODE_COMUNICATION_ERROR)
						System.out.print("Reply:\n" + ( 
								reply.getStatementType() == StatementType.SELECT ?
										ConnectorHelper.formatSelectReply(reply.getReply(), "  ", false)
										: reply.getReply()));
					
				else {
					System.out.println("Communication error with KeySQL server");
					for (int attemps = 0; attemps < 10; ++attemps) {
						Thread.sleep(10000);
						
						try {
							Connector.getInstance().stopClient();
							if (Connector.getInstance().startClient("0.0.0.0",(short)5556))
								break;
						}
						catch (IOException ex) {
							System.out.println("Unable to connect");
						}
					}
				}
				Thread.sleep(5000);
			}
			Connector.getInstance().stopClient();
		}
		catch (Exception ex){
			ex.printStackTrace();
		}
	}

}
