/*
 * Copyright 2013 bits of proof zrt.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bitsofproof.supernode.api.Address;
import com.bitsofproof.supernode.api.AlertListener;
import com.bitsofproof.supernode.api.BCSAPI;
import com.bitsofproof.supernode.api.BCSAPIException;
import com.bitsofproof.supernode.api.BCSAPIMessage;
import com.bitsofproof.supernode.api.Block;
import com.bitsofproof.supernode.api.RejectListener;
import com.bitsofproof.supernode.api.Transaction;
import com.bitsofproof.supernode.api.TransactionListener;
import com.bitsofproof.supernode.api.TrunkListener;
import com.bitsofproof.supernode.common.ExtendedKey;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ValidationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class BCSAPIConnector implements BCSAPI
{
	private static final Logger log = LoggerFactory.getLogger (BCSAPIConnector.class);

	private ConnectorFactory connectionFactory;
	private Connector connection;

	private Boolean production = null;

	private final Map<String, MessageDispatcher> messageDispatcher = new HashMap<String, MessageDispatcher> ();

	private long timeout = 10 * 60 * 1000; // 2 min

	public void setTimeout (long timeout)
	{
		this.timeout = timeout;
	}

	private interface ByteArrayMessageListener
	{
		public void onMessage (byte[] array);
	}

	private class MessageDispatcher
	{
		private final Map<Object, ByteArrayMessageListener> wrapperMap = new HashMap<Object, ByteArrayMessageListener> ();

		private final ConnectorConsumer consumer;

		public MessageDispatcher (ConnectorConsumer consumer)
		{
			this.consumer = consumer;
			try
			{
				consumer.setMessageListener (new ConnectorListener ()
				{
					@Override
					public void onMessage (ConnectorMessage message)
					{
						List<ByteArrayMessageListener> listenerList = new ArrayList<ByteArrayMessageListener> ();
						synchronized ( wrapperMap )
						{
							listenerList.addAll (wrapperMap.values ());
						}
						for ( ByteArrayMessageListener listener : listenerList )
						{
							try
							{
								listener.onMessage (message.getPayload ());
							}
							catch ( ConnectorException e )
							{
								log.error ("Unable to extract payload", e);
							}
						}
					}
				});
			}
			catch ( ConnectorException e )
			{
				log.error ("Can not attach message listener ", e);
			}
		}

		public void addListener (Object inner, ByteArrayMessageListener listener)
		{
			synchronized ( wrapperMap )
			{
				wrapperMap.put (inner, listener);
			}
		}

		public void removeListener (Object inner)
		{
			synchronized ( wrapperMap )
			{
				wrapperMap.remove (inner);
			}
		}

		public boolean isListened ()
		{
			synchronized ( wrapperMap )
			{
				return !wrapperMap.isEmpty ();
			}
		}

		public ConnectorConsumer getConsumer ()
		{
			return consumer;
		}
	}

	public void setConnectionFactory (ConnectorFactory connectionFactory)
	{
		this.connectionFactory = connectionFactory;
	}

	private void addTopicListener (String topic, Object inner, ByteArrayMessageListener listener) throws ConnectorException
	{
		synchronized ( messageDispatcher )
		{
			MessageDispatcher dispatcher = messageDispatcher.get (topic);
			if ( dispatcher == null )
			{
				ConnectorSession session = connection.createSession ();
				ConnectorConsumer consumer = session.createConsumer (session.createTopic (topic));
				messageDispatcher.put (topic, dispatcher = new MessageDispatcher (consumer));
			}
			dispatcher.addListener (inner, listener);
		}
	}

	private void removeTopicListener (String topic, Object inner)
	{
		synchronized ( messageDispatcher )
		{
			MessageDispatcher dispatcher = messageDispatcher.get (topic);
			if ( dispatcher != null )
			{
				dispatcher.removeListener (inner);
				if ( !dispatcher.isListened () )
				{
					messageDispatcher.remove (topic);
					try
					{
						dispatcher.getConsumer ().close ();
					}
					catch ( ConnectorException e )
					{
					}
				}
			}
		}
	}

	public void init ()
	{
		try
		{
			log.debug ("Initialize BCSAPI connector");
			connection = connectionFactory.getConnector ();
			connection.start ();
		}
		catch ( Exception e )
		{
			log.error ("Can not create connector", e);
		}
	}

	public void destroy ()
	{
		try
		{
			if ( connection != null )
			{
				connection.close ();
			}
		}
		catch ( Exception e )
		{
		}
	}

	@Override
	public long ping (long nonce) throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			log.trace ("ping " + nonce);

			session = connection.createSession ();
			ConnectorMessage m = session.createMessage ();
			BCSAPIMessage.Ping.Builder builder = BCSAPIMessage.Ping.newBuilder ();
			builder.setNonce (nonce);
			m.setPayload (builder.build ().toByteArray ());
			ConnectorProducer pingProducer = session.createProducer (session.createQueue ("ping"));
			byte[] response = synchronousRequest (session, pingProducer, m);
			if ( response != null )
			{
				BCSAPIMessage.Ping echo = BCSAPIMessage.Ping.parseFrom (response);
				if ( echo.getNonce () != nonce )
				{
					throw new BCSAPIException ("Incorrect echo nonce from ping");
				}
				return echo.getNonce ();
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		catch ( InvalidProtocolBufferException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
		return 0;
	}

	@Override
	public void addAlertListener (final AlertListener alertListener) throws BCSAPIException
	{
		try
		{
			addTopicListener ("alert", alertListener, new ByteArrayMessageListener ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					BCSAPIMessage.Alert alert;
					try
					{
						alert = BCSAPIMessage.Alert.parseFrom (body);
						alertListener.alert (alert.getAlert (), alert.getSeverity ());
					}
					catch ( InvalidProtocolBufferException e )
					{
						log.error ("Message format error", e);
					}
				}
			});
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void removeAlertListener (AlertListener listener)
	{
		removeTopicListener ("alert", listener);
	}

	@Override
	public boolean isProduction () throws BCSAPIException
	{
		if ( production != null )
		{
			return production;
		}
		return production = getBlockHeader ("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f") != null;
	}

	@Override
	public void scanTransactionsForAddresses (Set<Address> addresses, long after, TransactionListener listener) throws BCSAPIException
	{
		List<byte[]> al = new ArrayList<> (addresses.size ());
		for ( Address a : addresses )
		{
			try
			{
				al.add (a.getAddressScript ());
			}
			catch ( ValidationException e )
			{
			}
		}
		scanRequest (al, after, listener, "matchRequest");
	}

	@Override
	public void scanUTXOForAddresses (Set<Address> addresses, TransactionListener listener) throws BCSAPIException
	{
		List<byte[]> al = new ArrayList<> (addresses.size ());
		for ( Address a : addresses )
		{
			try
			{
				al.add (a.getAddressScript ());
			}
			catch ( ValidationException e )
			{
			}
		}
		scanRequest (al, 0, listener, "utxoMatchRequest");
	}

	@Override
	public void scanTransactions (ExtendedKey master, int firstIndex, int lookAhead, long after, final TransactionListener listener) throws BCSAPIException
	{
		scanRequest (master, firstIndex, lookAhead, after, listener, "accountRequest");
	}

	private void scanRequest (Collection<byte[]> match, long after, final TransactionListener listener, String requestQueue)
			throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			session = connection.createSession ();
			ConnectorMessage m = session.createMessage ();

			ConnectorProducer exactMatchProducer = session.createProducer (session.createQueue (requestQueue));
			BCSAPIMessage.ExactMatchRequest.Builder builder = BCSAPIMessage.ExactMatchRequest.newBuilder ();
			for ( byte[] d : match )
			{
				builder.addMatch (ByteString.copyFrom (d));
			}
			if ( after != 0 )
			{
				builder.setAfter (after);
			}
			m.setPayload (builder.build ().toByteArray ());
			final ConnectorTemporaryQueue answerQueue = session.createTemporaryQueue ();
			final ConnectorConsumer consumer = session.createConsumer (answerQueue);
			m.setReplyTo (answerQueue);
			final Semaphore ready = new Semaphore (0);
			consumer.setMessageListener (new ConnectorListener ()
			{
				@Override
				public void onMessage (ConnectorMessage message)
				{
					try
					{
						byte[] body = message.getPayload ();
						if ( body != null )
						{
							Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (body));
							t.computeHash ();
							listener.process (t);
						}
						else
						{
							consumer.close ();
							answerQueue.delete ();
							ready.release ();
						}
					}
					catch ( ConnectorException e )
					{
						log.error ("Malformed message received for scan matching transactions", e);
					}
					catch ( InvalidProtocolBufferException e )
					{
						log.error ("Malformed message received for scan matching transactions", e);
					}
				}
			});

			exactMatchProducer.send (m);
			ready.acquireUninterruptibly ();
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	private void scanRequest (ExtendedKey master, int firstIndex, int lookAhead, long after, final TransactionListener listener, String request)
			throws BCSAPIException
	{
		if ( !master.isReadOnly () )
		{
			master = master.getReadOnly ();
		}
		ConnectorSession session = null;
		try
		{
			session = connection.createSession ();
			ConnectorMessage m = session.createMessage ();

			ConnectorProducer scanAccountProducer = session.createProducer (session.createQueue (request));
			BCSAPIMessage.AccountRequest.Builder builder = BCSAPIMessage.AccountRequest.newBuilder ();
			builder.setPublicKey (master.serialize (isProduction ()));
			builder.setLookAhead (lookAhead);
			builder.setAfter (after);
			m.setPayload (builder.build ().toByteArray ());

			final ConnectorTemporaryQueue answerQueue = session.createTemporaryQueue ();
			final ConnectorConsumer consumer = session.createConsumer (answerQueue);
			m.setReplyTo (answerQueue);
			final Semaphore ready = new Semaphore (0);
			consumer.setMessageListener (new ConnectorListener ()
			{
				@Override
				public void onMessage (ConnectorMessage message)
				{
					try
					{
						byte[] body = message.getPayload ();
						if ( body != null )
						{
							Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (body));
							t.computeHash ();
							listener.process (t);
						}
						else
						{
							consumer.close ();
							answerQueue.delete ();
							ready.release ();
						}
					}
					catch ( ConnectorException e )
					{
						log.error ("Malformed message received for account scan transactions", e);
					}
					catch ( InvalidProtocolBufferException e )
					{
						log.error ("Malformed message received for account scan transactions", e);
					}
				}
			});

			scanAccountProducer.send (m);
			ready.acquireUninterruptibly ();
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	@Override
	public void catchUp (List<String> inventory, int limit, boolean headers, final TrunkListener listener) throws BCSAPIException
	{
		log.trace ("catchUp");
		ConnectorMessage m;
		ConnectorSession session = null;
		try
		{
			session = connection.createSession ();
			ConnectorProducer transactionRequestProducer = session.createProducer (session.createQueue ("catchUpRequest"));

			m = session.createMessage ();
			BCSAPIMessage.CatchUpRequest.Builder builder = BCSAPIMessage.CatchUpRequest.newBuilder ();
			builder.setLimit (limit);
			builder.setHeaders (true);
			for ( String hash : inventory )
			{
				builder.addInventory (ByteString.copyFrom (new Hash (hash).toByteArray ()));
			}
			m.setPayload (builder.build ().toByteArray ());
			byte[] response = synchronousRequest (session, transactionRequestProducer, m);
			if ( response != null )
			{
				BCSAPIMessage.TrunkUpdate blockMessage = BCSAPIMessage.TrunkUpdate.parseFrom (response);
				List<Block> blockList = new ArrayList<Block> ();
				for ( BCSAPIMessage.Block b : blockMessage.getAddedList () )
				{
					blockList.add (Block.fromProtobuf (b));
				}
				listener.trunkUpdate (blockList);
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		catch ( InvalidProtocolBufferException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	@Override
	public void registerTransactionListener (final TransactionListener listener) throws BCSAPIException
	{
		try
		{
			addTopicListener ("transaction", listener, new ByteArrayMessageListener ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					try
					{
						Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (body));
						t.computeHash ();
						listener.process (t);
					}
					catch ( Exception e )
					{
						log.debug ("Transaction message error", e);
					}
				}
			});
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void removeTransactionListener (TransactionListener listener)
	{
		removeTopicListener ("transaction", listener);
	}

	@Override
	public void registerTrunkListener (final TrunkListener listener) throws BCSAPIException
	{
		try
		{
			addTopicListener ("trunk", listener, new ByteArrayMessageListener ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					try
					{
						BCSAPIMessage.TrunkUpdate blockMessage = BCSAPIMessage.TrunkUpdate.parseFrom (body);
						List<Block> blockList = new ArrayList<Block> ();
						for ( BCSAPIMessage.Block b : blockMessage.getAddedList () )
						{
							blockList.add (Block.fromProtobuf (b));
						}
						listener.trunkUpdate (blockList);
					}
					catch ( Exception e )
					{
						log.debug ("Block message error", e);
					}
				}
			});
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void removeTrunkListener (TrunkListener listener)
	{
		removeTopicListener ("trunk", listener);
	}

	private byte[] synchronousRequest (ConnectorSession session, ConnectorProducer producer, ConnectorMessage m) throws BCSAPIException
	{
		ConnectorTemporaryQueue answerQueue = null;
		ConnectorConsumer consumer = null;
		try
		{
			answerQueue = session.createTemporaryQueue ();
			m.setReplyTo (answerQueue);
			consumer = session.createConsumer (answerQueue);
			producer.send (m);
			ConnectorMessage reply = consumer.receive (timeout);
			if ( reply == null )
			{
				throw new BCSAPIException ("timeout");
			}
			return reply.getPayload ();
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				if ( consumer != null )
				{
					consumer.close ();
				}
				answerQueue.delete ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	@Override
	public Transaction getTransaction (String hash) throws BCSAPIException
	{
		log.trace ("get transaction " + hash);
		ConnectorMessage m;
		ConnectorSession session = null;
		try
		{
			session = connection.createSession ();
			ConnectorProducer transactionRequestProducer = session.createProducer (session.createQueue ("transactionRequest"));

			m = session.createMessage ();
			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (new Hash (hash).toByteArray ()));
			m.setPayload (builder.build ().toByteArray ());
			byte[] response = synchronousRequest (session, transactionRequestProducer, m);
			if ( response != null )
			{
				Transaction t;
				t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (response));
				t.computeHash ();
				return t;
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		catch ( InvalidProtocolBufferException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
		return null;
	}

	@Override
	public Block getBlock (String hash) throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			log.trace ("get block " + hash);

			session = connection.createSession ();
			ConnectorProducer blockRequestProducer = session.createProducer (session.createQueue ("blockRequest"));

			ConnectorMessage m = session.createMessage ();
			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (new Hash (hash).toByteArray ()));
			m.setPayload (builder.build ().toByteArray ());
			byte[] response = synchronousRequest (session, blockRequestProducer, m);
			if ( response != null )
			{
				Block b = Block.fromProtobuf (BCSAPIMessage.Block.parseFrom (response));
				return b;
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		catch ( InvalidProtocolBufferException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
		return null;
	}

	@Override
	public Block getBlockHeader (String hash) throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			log.trace ("get block header" + hash);

			session = connection.createSession ();
			ConnectorProducer blockHeaderRequestProducer = session.createProducer (session.createQueue ("headerRequest"));

			ConnectorMessage m = session.createMessage ();
			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (new Hash (hash).toByteArray ()));
			m.setPayload (builder.build ().toByteArray ());
			byte[] response = synchronousRequest (session, blockHeaderRequestProducer, m);
			if ( response != null )
			{
				Block b = Block.fromProtobuf (BCSAPIMessage.Block.parseFrom (response));
				return b;
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		catch ( InvalidProtocolBufferException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
		return null;
	}

	@Override
	public void sendTransaction (Transaction transaction) throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			transaction.computeHash ();
			log.trace ("send transaction " + transaction.getHash ());

			session = connection.createSession ();
			ConnectorProducer transactionProducer = session.createProducer (session.createTopic ("newTransaction"));

			ConnectorMessage m = session.createMessage ();
			m.setPayload (transaction.toProtobuf ().toByteArray ());
			byte[] reply = synchronousRequest (session, transactionProducer, m);
			if ( reply != null )
			{
				try
				{
					BCSAPIMessage.ExceptionMessage em = BCSAPIMessage.ExceptionMessage.parseFrom (reply);
					throw new BCSAPIException (em.getMessage (0));
				}
				catch ( InvalidProtocolBufferException e )
				{
					throw new BCSAPIException ("Invalid response", e);
				}
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	@Override
	public void sendBlock (Block block) throws BCSAPIException
	{
		ConnectorSession session = null;
		try
		{
			log.trace ("send block " + block.getHash ());
			session = connection.createSession ();
			ConnectorProducer blockProducer = session.createProducer (session.createTopic ("newBlock"));

			ConnectorMessage m = session.createMessage ();
			m.setPayload (block.toProtobuf ().toByteArray ());
			byte[] reply = synchronousRequest (session, blockProducer, m);
			if ( reply != null )
			{
				try
				{
					BCSAPIMessage.ExceptionMessage em = BCSAPIMessage.ExceptionMessage.parseFrom (reply);
					throw new BCSAPIException (em.getMessage (0));
				}
				catch ( InvalidProtocolBufferException e )
				{
					throw new BCSAPIException ("Invalid response", e);
				}
			}
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
		finally
		{
			try
			{
				session.close ();
			}
			catch ( ConnectorException e )
			{
			}
		}
	}

	@Override
	public void registerRejectListener (final RejectListener rejectListener) throws BCSAPIException
	{
		try
		{
			addTopicListener ("reject", rejectListener, new ByteArrayMessageListener ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					try
					{
						BCSAPIMessage.Reject reject = BCSAPIMessage.Reject.parseFrom (body);
						rejectListener.rejected (reject.getCommand (), new Hash (reject.getHash ().toByteArray ()).toString (), reject.getReason (),
								reject.getRejectCode ());
					}
					catch ( Exception e )
					{
						log.debug ("Transaction message error", e);
					}
				}
			});
		}
		catch ( ConnectorException e )
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void removeRejectListener (RejectListener rejectListener)
	{
		removeTopicListener ("reject", rejectListener);
	}
}
