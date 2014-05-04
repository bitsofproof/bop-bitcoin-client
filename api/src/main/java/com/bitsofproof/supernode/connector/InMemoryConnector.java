/*
 * Copyright 2014 bits of proof zrt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bitsofproof.supernode.connector;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryConnector implements Connector
{
	private static final Logger log = LoggerFactory.getLogger (InMemoryConnector.class);

	@Override
	public ConnectorSession createSession () throws ConnectorException
	{
		InMemorySession session = new InMemorySession ();
		consumerExecutor.execute (session);
		return session;
	}

	private final ExecutorService consumerExecutor = Executors.newCachedThreadPool ();
	private final Map<String, Set<InMemoryConsumer>> consumer = Collections.synchronizedMap (new HashMap<String, Set<InMemoryConsumer>> ());
	private final LinkedBlockingQueue<MessageWithDestination> queue = new LinkedBlockingQueue<> ();

	private static class MessageWithDestination
	{
		private final String destination;
		private final ConnectorMessage message;

		public String getDestination ()
		{
			return destination;
		}

		public ConnectorMessage getMessage ()
		{
			return message;
		}

		public MessageWithDestination (String destination, ConnectorMessage message)
		{
			this.destination = destination;
			this.message = message;
		}
	}

	public static class InMemoryConnectorDestination implements ConnectorDestination
	{
		private String name;

		public InMemoryConnectorDestination (String name)
		{
			this.name = name;
		}

		@Override
		public String getName ()
		{
			return name;
		}

	}

	public static class InMemoryConnectorMessage implements ConnectorMessage
	{
		private byte[] payload;
		private ConnectorDestination replyTo;
		InMemorySession session;

		public InMemoryConnectorMessage (InMemorySession inMemorySession)
		{
			this.session = inMemorySession;
		}

		@Override
		public void setPayload (byte[] payload)
		{
			if ( payload == null )
			{
				this.payload = null;
			}
			else
			{
				this.payload = Arrays.clone (payload);
			}
		}

		@Override
		public byte[] getPayload ()
		{
			if ( payload == null )
			{
				return null;
			}
			return Arrays.clone (payload);
		}

		@Override
		public ConnectorProducer getReplyProducer () throws ConnectorException
		{
			return session.createProducer (replyTo);
		}

		@Override
		public void setReplyTo (ConnectorDestination replyTo)
		{
			this.replyTo = replyTo;
		}
	}

	private static class InMemoryConsumer implements ConnectorConsumer
	{
		private final LinkedBlockingQueue<ConnectorMessage> queue = new LinkedBlockingQueue<> ();
		private ConnectorListener listener;
		private String name;

		public InMemoryConsumer (String name)
		{
			this.name = name;
		}

		public String getName ()
		{
			return name;
		}

		public void putMessage (ConnectorMessage m) throws ConnectorException
		{
			if ( listener != null )
			{
				throw new ConnectorException ("Use either setListener or receive");
			}
			queue.offer (m);
		}

		@Override
		public void setMessageListener (ConnectorListener listener) throws ConnectorException
		{
			this.listener = listener;
		}

		public ConnectorListener getListener ()
		{
			return listener;
		}

		@Override
		public ConnectorMessage receive (long timeout) throws ConnectorException
		{
			if ( listener != null )
			{
				throw new ConnectorException ("either listen or receive with consumer");
			}
			try
			{
				return queue.poll (timeout, TimeUnit.MILLISECONDS);
			}
			catch ( InterruptedException e )
			{
				return null;
			}
		}

		@Override
		public ConnectorMessage receive () throws ConnectorException
		{
			if ( listener != null )
			{
				throw new ConnectorException ("either listen or receive with consumer");
			}
			try
			{
				return queue.take ();
			}
			catch ( InterruptedException e )
			{
				return null;
			}
		}

		@Override
		public ConnectorMessage receiveNoWait () throws ConnectorException
		{
			if ( listener != null )
			{
				throw new ConnectorException ("either listen or receive with consumer");
			}
			return queue.poll ();
		}

		@Override
		public void close () throws ConnectorException
		{
		}
	}

	private class InMemoryProducer implements ConnectorProducer
	{
		private final String name;
		private final LinkedBlockingQueue<MessageWithDestination> queue;

		public InMemoryProducer (String name, LinkedBlockingQueue<MessageWithDestination> queue)
		{
			this.name = name;
			this.queue = queue;
		}

		@Override
		public void send (ConnectorMessage message) throws ConnectorException
		{
			if ( consumer.containsKey (name) )
			{
				queue.add (new MessageWithDestination (name, message));
			}
		}

		@Override
		public void close () throws ConnectorException
		{
		}
	}

	private static class InMemoryQueue implements ConnectorQueue
	{
		private final String name;

		public InMemoryQueue (String name)
		{
			this.name = name;
		}

		@Override
		public String getName ()
		{
			return name;
		}
	}

	private static class InMemoryTemporaryQueue extends InMemoryQueue implements ConnectorTemporaryQueue
	{
		public InMemoryTemporaryQueue ()
		{
			super ("temp" + String.valueOf (new SecureRandom ().nextLong ()));
		}

		@Override
		public void delete ()
		{
		}
	}

	private static class InMemoryTopic implements ConnectorTopic
	{
		private final String name;

		public InMemoryTopic (String name)
		{
			this.name = name;
		}

		@Override
		public String getName ()
		{
			return name;
		}
	}

	private class InMemorySession implements ConnectorSession, Runnable
	{
		private volatile boolean run = true;
		private Set<InMemoryConsumer> consumerSet = Collections.synchronizedSet (new HashSet<InMemoryConsumer> ());

		@Override
		public ConnectorMessage createMessage () throws ConnectorException
		{
			return new InMemoryConnectorMessage (this);
		}

		@Override
		public ConnectorProducer createProducer (ConnectorDestination destination) throws ConnectorException
		{
			return new InMemoryProducer (destination.getName (), queue);
		}

		@Override
		public ConnectorConsumer createConsumer (ConnectorDestination destination) throws ConnectorException
		{
			InMemoryConsumer c = new InMemoryConsumer (destination.getName ());
			consumerSet.add (c);
			Set<InMemoryConsumer> cl;
			synchronized ( consumer )
			{
				cl = consumer.get (destination.getName ());
				if ( cl == null )
				{
					cl = new HashSet<InMemoryConsumer> ();
					consumer.put (destination.getName (), cl);
				}
				cl.add (c);
			}
			return c;
		}

		@Override
		public ConnectorTemporaryQueue createTemporaryQueue () throws ConnectorException
		{
			return new InMemoryTemporaryQueue ();
		}

		@Override
		public ConnectorQueue createQueue (String name) throws ConnectorException
		{
			return new InMemoryQueue (name);
		}

		@Override
		public ConnectorTopic createTopic (String name) throws ConnectorException
		{
			return new InMemoryTopic (name);
		}

		@Override
		public void run ()
		{
			MessageWithDestination md = null;
			do
			{
				try
				{
					md = queue.poll (1, TimeUnit.SECONDS);
					if ( md != null )
					{
						Set<InMemoryConsumer> cl;
						cl = consumer.get (md.getDestination ());
						if ( cl != null )
						{
							for ( InMemoryConsumer c : cl )
							{
								if ( c.getListener () != null )
								{
									try
									{
										c.getListener ().onMessage (md.getMessage ());
									}
									catch ( Exception e )
									{
										log.error ("Uncaught exception in connector listener", e);
									}
								}
								else
								{
									c.putMessage (md.getMessage ());
								}
							}
						}
					}
				}
				catch ( InterruptedException | ConnectorException e )
				{
					log.error ("Exception in connector consumer thread", e);
					break;
				}
			} while ( md != null || run );
			synchronized ( consumerSet )
			{
				for ( InMemoryConsumer c : consumerSet )
				{
					synchronized ( consumer )
					{
						Set<InMemoryConsumer> cs = consumer.get (c.getName ());
						if ( cs != null )
						{
							cs.remove (c);
							if ( cs.isEmpty () )
							{
								consumer.remove (c.getName ());
							}
						}
					}
				}
			}
		}

		@Override
		public void close ()
		{
			run = false;
		}
	}

	@Override
	public void start ()
	{
	}

	@Override
	public void close ()
	{
	}

	@Override
	public void setClientID (String string)
	{
	}
}
