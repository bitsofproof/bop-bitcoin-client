package com.bitsofproof.supernode.connector4.jms;

import com.bitsofproof.supernode.connector4.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.ConnectionFactory;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class JMSConnectorTest
{
	private BrokerService broker;

	private JMSConnector clientConnector;

	private JMSConnector serverConnector;

	private ExecutorService requestExecutor;

	@Before
	public void setUp () throws Exception
	{
		broker = new BrokerService ();
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory ("vm://localhost");


		ImmutableMap<String, ActiveMQDestination> destinations = ImmutableMap.of ("commandQueue", new ActiveMQQueue ("commandQueue"),
		                                                                          "requestQueue", new ActiveMQQueue ("requestQueue"),
		                                                                          "topic", new ActiveMQTopic ("topic"),
		                                                                          "requestTopic", new ActiveMQTopic ("requestTopic"));

		broker.setDestinations (destinations.values ().toArray (new ActiveMQDestination[destinations.size ()]));
		broker.setPersistent (false);
		broker.setPersistenceAdapter (new MemoryPersistenceAdapter ());
		broker.start ();

		clientConnector = new JMSConnector (connectionFactory,
		                                    destinations,
		                                    Executors.newCachedThreadPool (),
		                                    Executors.newCachedThreadPool ());
		serverConnector = new JMSConnector (connectionFactory,
		                                    destinations,
		                                    Executors.newCachedThreadPool (),
		                                    Executors.newCachedThreadPool ());

		requestExecutor = Executors.newCachedThreadPool ();
	}

	@After
	public void tearDown () throws Exception
	{
		clientConnector.close ();
		serverConnector.close ();

		broker.stop ();

		requestExecutor.shutdownNow ();
	}

	@Test
	public void testPubSub () throws Exception
	{
		final Set<String> messages = Collections.synchronizedSet (new HashSet<String> ());

		clientConnector.setSubscription ("topic", new Subscription ()
		{
			@Override
			public void onMessage (byte[] bytes)
			{
				messages.add (new String (bytes));
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		for (int i = 0; i < 100; i++)
		{
			final String message = "message" + i;
			requestExecutor.execute (new Runnable ()
			{
				@Override
				public void run ()
				{
					try
					{
						serverConnector.publish ("topic", message.getBytes ());
					}
					catch (Connector4Exception ignored)
					{
					}
				}
			});
		}

		clientConnector.close ();
		serverConnector.close ();

		for (int i = 0; i < 100; i++)
		{
			assertThat (messages).contains ("message" + i);
		}
	}

	@Test
	public void testTimingOutMultipartRequest () throws Exception
	{
		serverConnector.setRequestHandler ("requestQueue", new MultipartResponseRequestHandler ()
		{

			@Override
			public void onRequest (byte[] request, MultipartResponse response)
			{
				try
				{
					Thread.sleep (1000);
				}
				catch (InterruptedException ignored)
				{
				}

				response.send (request);
				response.send (request);
				response.eof ();
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		try
		{
			clientConnector.request ("requestQueue", "still waiting".getBytes (), 100, TimeUnit.MILLISECONDS);
			fail ("Should've timed out");
		}
		catch (Connector4Exception e)
		{
		}


		clientConnector.close ();
		serverConnector.close ();
	}

	@Test
	public void testMultipartRequest () throws Exception
	{
		serverConnector.setRequestHandler ("requestQueue", new MultipartResponseRequestHandler ()
		{
			@Override
			public void onRequest (byte[] request, MultipartResponse response)
			{
				String req = new String (request);
				response.send ((req + "1").getBytes ());
				response.send ((req + "2").getBytes ());
				response.send ((req + "3").getBytes ());
				response.eof ();
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		List<Future<String>> results = new ArrayList<> ();
		for (int i = 0; i < 100; i++)
		{
			final String request = "message" + i;
			final List<String> result = new ArrayList<> ();
			results.add (requestExecutor.submit (new Callable<String> ()
			{
				@Override
				public String call () throws Exception
				{
					clientConnector.multipartRequest ("requestQueue", request.getBytes (), new MultipartResponseHandler ()
					{
						@Override
						public void part (byte[] bytes)
						{
							result.add (new String (bytes));
						}

						@Override
						public void eof ()
						{
						}
					});
					return String.format ("%s|%s", request, Joiner.on (',').join (result));
				}
			}));
		}

		clientConnector.close ();
		serverConnector.close ();

		for (Future<String> result : results)
		{
			String[] parts = result.get ().split ("\\|");

			assertThat (parts[1]).isEqualTo (String.format ("%1$s1,%1$s2,%1$s3", parts[0]));
		}
	}

	@Test
	public void testTimingOutRequest () throws Exception
	{
		serverConnector.setRequestHandler ("requestQueue", new RequestHandler ()
		{
			@Override
			public byte[] onRequest (byte[] req)
			{
				try
				{
					Thread.sleep (1000);
				}
				catch (InterruptedException ignored)
				{
				}
				return req;
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		try
		{
			clientConnector.request ("requestQueue", "still waiting".getBytes (), 100, TimeUnit.MILLISECONDS);
			fail ("Should've timed out");
		}
		catch (Connector4Exception e)
		{
		}

		clientConnector.close ();
		serverConnector.close ();
	}

	@Test
	public void testRequestReply () throws Exception
	{
		serverConnector.setRequestHandler ("requestQueue", new RequestHandler ()
		{
			@Override
			public byte[] onRequest (byte[] req)
			{
				String reqString = new String (req);
				return ("reply: " + reqString).getBytes ();
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		List<Future<String>> results = new ArrayList<> ();
		for (int i = 0; i < 100; i++)
		{
			final String message = "hello" + i;
			results.add (requestExecutor.submit (new Callable<String> ()
			{
				@Override
				public String call () throws Exception
				{
					byte[] response = clientConnector.request ("requestQueue", message.getBytes ());
					return message + "|" + new String (response);

				}
			}));
		}

		clientConnector.close ();
		serverConnector.close ();

		for (Future<String> result : results)
		{
			String[] parts = result.get ().split ("\\|");
			assertThat ("reply: " + parts[0]).isEqualTo (parts[1]);
		}
	}

	@Test
	public void testNullReply () throws Exception
	{
		serverConnector.setRequestHandler ("requestQueue", new RequestHandler ()
		{
			@Override
			public byte[] onRequest (byte[] req)
			{
				return null;
			}
		});

		serverConnector.start ();
		clientConnector.start ();

		byte[] result = clientConnector.request ("requestQueue", "".getBytes ());
		assertThat (result).isNull ();

	}
}
