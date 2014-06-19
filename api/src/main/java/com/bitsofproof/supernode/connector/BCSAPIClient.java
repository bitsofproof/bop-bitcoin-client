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

import com.bitsofproof.supernode.api.*;
import com.bitsofproof.supernode.common.ExtendedKey;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ValidationException;
import com.bitsofproof.supernode.connector4.Connector4;
import com.bitsofproof.supernode.connector4.Connector4Exception;
import com.bitsofproof.supernode.connector4.MultipartResponseHandler;
import com.bitsofproof.supernode.connector4.Subscription;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class BCSAPIClient implements BCSAPI
{
	private static final Logger log = LoggerFactory.getLogger (BCSAPIClient.class);

	private Boolean production = null;

	private long timeout = 10 * 60 * 1000; // 2 min

	private Connector4 connector;

	private final CopyOnWriteArrayList<RejectListener> rejectListeners = new CopyOnWriteArrayList<> ();

	private final CopyOnWriteArrayList<AlertListener> alertListeners = new CopyOnWriteArrayList<> ();

	private final CopyOnWriteArrayList<TransactionListener> transactionListeners = new CopyOnWriteArrayList<> ();

	private final CopyOnWriteArrayList<TrunkListener> trunkListeners = new CopyOnWriteArrayList<> ();

	public BCSAPIClient ()
	{
	}

	public BCSAPIClient (Connector4 connector)
	{
		this.connector = connector;
	}

	public void setTimeout (long timeout)
	{
		this.timeout = timeout;
	}

	public void setConnector (Connector4 connector)
	{
		this.connector = connector;
	}

	public void init ()
	{
		try
		{
			log.debug ("Initialize BCSAPI connector");

			connector.setSubscription ("alert", new Subscription ()
			{
				@Override
				public void onMessage (byte[] bytes)
				{
					notifyAlertListeners (bytes);
				}
			});

			connector.setSubscription ("transaction", new Subscription ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					notifyTransactionListeners (body);
				}
			});

			connector.setSubscription ("trunk", new Subscription ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					notifyTrunkListeners (body);
				}
			});

			connector.setSubscription ("reject", new Subscription ()
			{
				@Override
				public void onMessage (byte[] body)
				{
					notifyRejectListeners (body);
				}
			});

			connector.start ();
		}
		catch (Exception e)
		{
			log.error ("Can not create connector", e);
		}
	}

	public void destroy ()
	{
		try
		{
			if ( connector != null )
			{
				connector.close ();
			}
		}
		catch (Exception ignored)
		{
		}
	}

	@Override
	public long ping (long nonce) throws BCSAPIException
	{
		try
		{
			BCSAPIMessage.Ping.Builder builder = BCSAPIMessage.Ping.newBuilder ();
			builder.setNonce (nonce);

			byte[] response = connector.request ("ping", builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
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
		catch (Connector4Exception | InvalidProtocolBufferException e)
		{
			throw new BCSAPIException (e);
		}

		return 0;
	}

	@Override
	public void addAlertListener (final AlertListener alertListener) throws BCSAPIException
	{
		alertListeners.add (alertListener);
	}

	@Override
	public void removeAlertListener (AlertListener listener)
	{
		alertListeners.remove (listener);
	}

	private void notifyAlertListeners (byte[] bytes)
	{
		try
		{
			BCSAPIMessage.Alert alert = BCSAPIMessage.Alert.parseFrom (bytes);
			for (AlertListener listener : alertListeners)
			{
				listener.alert (alert.getAlert (), alert.getSeverity ());
			}
		}
		catch (InvalidProtocolBufferException e)
		{
			log.error ("Message format error", e);
		}
	}

	@Override
	public boolean isProduction () throws BCSAPIException
	{
		if ( production != null )
		{
			return production;
		}
		return production = getBlockHeader (new Hash ("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")) != null;
	}

	@Override
	public void scanTransactionsForAddresses (Set<Address> addresses, long after, TransactionListener listener) throws BCSAPIException
	{
		List<byte[]> al = new ArrayList<> (addresses.size ());
		for (Address a : addresses)
		{
			try
			{
				al.add (a.getAddressScript ());
			}
			catch (ValidationException ignored)
			{
			}
		}
		scanRequest (al, after, listener, "matchRequest");
	}

	@Override
	public void scanUTXOForAddresses (Set<Address> addresses, TransactionListener listener) throws BCSAPIException
	{
		List<byte[]> al = new ArrayList<> (addresses.size ());
		for (Address a : addresses)
		{
			try
			{
				al.add (a.getAddressScript ());
			}
			catch (ValidationException ignored)
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
		BCSAPIMessage.ExactMatchRequest.Builder builder = BCSAPIMessage.ExactMatchRequest.newBuilder ();
		for (byte[] d : match)
		{
			builder.addMatch (ByteString.copyFrom (d));
		}
		if ( after != 0 )
		{
			builder.setAfter (after);
		}

		try
		{
			connector.multipartRequest (requestQueue, builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS, new MultipartResponseHandler ()
			{
				@Override
				public void part (byte[] bytes)
				{
					try
					{
						Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (bytes));
						t.computeHash ();
						listener.process (t);
					}
					catch (InvalidProtocolBufferException e)
					{
						log.error ("Malformed message received for scan matching transactions", e);
					}
				}

				@Override
				public void eof ()
				{
				}
			});
		}
		catch (Connector4Exception e)
		{
			throw new BCSAPIException (e);
		}
	}

	private void scanRequest (ExtendedKey master, int firstIndex, int lookAhead, long after, final TransactionListener listener, String request)
			throws BCSAPIException
	{
		if ( !master.isReadOnly () )
		{
			master = master.getReadOnly ();
		}

		BCSAPIMessage.AccountRequest.Builder builder = BCSAPIMessage.AccountRequest.newBuilder ();
		builder.setPublicKey (master.serialize (isProduction ()));
		builder.setLookAhead (lookAhead);
		builder.setAfter (after);
		builder.setFirstIndex (firstIndex);

		try
		{
			connector.multipartRequest (request, builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS, new MultipartResponseHandler ()
			{
				@Override
				public void part (byte[] bytes)
				{
					try
					{
						Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (bytes));
						t.computeHash ();
						listener.process (t);
					}
					catch (InvalidProtocolBufferException e)
					{
						log.error ("Malformed message received for account scan transactions", e);
					}
				}

				@Override
				public void eof ()
				{
				}
			});
		}
		catch (Connector4Exception e)
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void catchUp (List<Hash> inventory, int limit, boolean headers, final TrunkListener listener) throws BCSAPIException
	{
		log.trace ("catchUp");

		BCSAPIMessage.CatchUpRequest.Builder builder = BCSAPIMessage.CatchUpRequest.newBuilder ();
		builder.setLimit (limit);
		builder.setHeaders (true);
		for (Hash hash : inventory)
		{
			builder.addInventory (ByteString.copyFrom (hash.toByteArray ()));
		}

		try
		{
			byte[] response = connector.request ("catchUpRequest", builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( response != null )
			{
				BCSAPIMessage.TrunkUpdate blockMessage = BCSAPIMessage.TrunkUpdate.parseFrom (response);
				List<Block> blockList = new ArrayList<> ();
				for (BCSAPIMessage.Block b : blockMessage.getAddedList ())
				{
					blockList.add (Block.fromProtobuf (b));
				}
				listener.trunkUpdate (blockList);
			}
		}
		catch (Connector4Exception | InvalidProtocolBufferException e)
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void registerTransactionListener (final TransactionListener listener) throws BCSAPIException
	{
		transactionListeners.add (listener);
	}

	@Override
	public void removeTransactionListener (TransactionListener listener)
	{
		transactionListeners.remove (listener);
	}

	private void notifyTransactionListeners (byte[] body)
	{
		try
		{
			Transaction t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (body));
			t.computeHash ();
			for (TransactionListener listener : transactionListeners)
			{
				listener.process (t);
			}
		}
		catch (InvalidProtocolBufferException e)
		{
			log.debug ("Transaction message error", e);
		}
	}

	@Override
	public void registerTrunkListener (final TrunkListener listener) throws BCSAPIException
	{
		trunkListeners.add (listener);
	}

	@Override
	public void removeTrunkListener (TrunkListener listener)
	{
		trunkListeners.remove (listener);
	}


	private void notifyTrunkListeners (byte[] body)
	{
		try
		{
			BCSAPIMessage.TrunkUpdate blockMessage = BCSAPIMessage.TrunkUpdate.parseFrom (body);
			List<Block> blockList = new ArrayList<> ();
			for (BCSAPIMessage.Block b : blockMessage.getAddedList ())
			{
				blockList.add (Block.fromProtobuf (b));
			}

			for (TrunkListener listener : trunkListeners)
			{
				listener.trunkUpdate (blockList);
			}
		}
		catch (InvalidProtocolBufferException e)
		{
			log.debug ("Block message error", e);
		}
	}

	@Override
	public Transaction getTransaction (Hash hash) throws BCSAPIException
	{
		try
		{
			log.trace ("get transaction " + hash);

			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (hash.toByteArray ()));

			Transaction t = null;
			byte[] response = connector.request ("transactionRequest", builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( response != null )
			{
				t = Transaction.fromProtobuf (BCSAPIMessage.Transaction.parseFrom (response));
				t.computeHash ();
				return t;
			}

			return t;
		}
		catch (Connector4Exception | InvalidProtocolBufferException e)
		{
			throw new BCSAPIException (e);
		}

	}

	@Override
	public Block getBlock (Hash hash) throws BCSAPIException
	{
		try
		{
			log.trace ("get block " + hash);

			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (hash.toByteArray ()));

			byte[] response = connector.request ("blockRequest", builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( response != null )
			{
				return Block.fromProtobuf (BCSAPIMessage.Block.parseFrom (response));
			}

			return null;
		}
		catch (Connector4Exception | InvalidProtocolBufferException e)
		{
			throw new BCSAPIException (e);
		}

	}

	@Override
	public Block getBlockHeader (Hash hash) throws BCSAPIException
	{
		try
		{
			log.trace ("get block header" + hash);

			BCSAPIMessage.Hash.Builder builder = BCSAPIMessage.Hash.newBuilder ();
			builder.addHash (ByteString.copyFrom (hash.toByteArray ()));

			byte[] response = connector.request ("headerRequest", builder.build ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( response != null )
			{
				return Block.fromProtobuf (BCSAPIMessage.Block.parseFrom (response));
			}

			return null;
		}
		catch (Connector4Exception | InvalidProtocolBufferException e)
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void sendTransaction (Transaction transaction) throws BCSAPIException
	{
		try
		{
			transaction.computeHash ();
			log.trace ("send transaction " + transaction.getHash ());

			byte[] reply = connector.request ("newTransaction", transaction.toProtobuf ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( reply != null )
			{
				try
				{
					BCSAPIMessage.ExceptionMessage em = BCSAPIMessage.ExceptionMessage.parseFrom (reply);
					throw new BCSAPIException (em.getMessage (0));
				}
				catch (InvalidProtocolBufferException e)
				{
					throw new BCSAPIException ("Invalid response", e);
				}
			}
		}
		catch (Connector4Exception e)
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void sendBlock (Block block) throws BCSAPIException
	{
		try
		{
			log.trace ("send block " + block.getHash ());

			byte[] reply = connector.request ("newBlock", block.toProtobuf ().toByteArray (), timeout, TimeUnit.MILLISECONDS);
			if ( reply != null )
			{
				try
				{
					BCSAPIMessage.ExceptionMessage em = BCSAPIMessage.ExceptionMessage.parseFrom (reply);
					throw new BCSAPIException (em.getMessage (0));
				}
				catch (InvalidProtocolBufferException e)
				{
					throw new BCSAPIException ("Invalid response", e);
				}
			}
		}
		catch (Connector4Exception e)
		{
			throw new BCSAPIException (e);
		}
	}

	@Override
	public void registerRejectListener (final RejectListener rejectListener) throws BCSAPIException
	{
		rejectListeners.add (rejectListener);
	}

	@Override
	public void removeRejectListener (RejectListener rejectListener)
	{
		rejectListeners.remove (rejectListener);
	}

	private void notifyRejectListeners (byte[] body)
	{
		try
		{
			BCSAPIMessage.Reject reject = BCSAPIMessage.Reject.parseFrom (body);
			for (RejectListener listener : rejectListeners)
			{
				listener.rejected (reject.getCommand (), new Hash (reject.getHash ().toByteArray ()), reject.getReason (),
				                   reject.getRejectCode ());
			}
		}
		catch (InvalidProtocolBufferException e)
		{
			log.debug ("Transaction message error", e);
		}
	}
}
