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
package com.bitsofproof.supernode.api;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.bitsofproof.supernode.common.ByteUtils;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ScriptFormat;
import com.bitsofproof.supernode.common.ValidationException;
import com.bitsofproof.supernode.common.WireFormat;

public class Transaction implements Serializable, Cloneable
{
	private static final long serialVersionUID = 690918485496086537L;

	private long version = 1;

	private long lockTime = 0;

	private List<TransactionInput> inputs;
	private List<TransactionOutput> outputs;

	// below are not part of P2P wire format
	private String hash;
	private boolean expired = false;
	// below is not part of server messages, but populated in the client library
	private String blockHash;
	private String offendingTx;
	private int height = 0;
	private long blocktime = new Date ().getTime () / 1000;

	public static Transaction createCoinbase (Address address, long value, int blockHeight) throws ValidationException
	{
		Transaction cb = new Transaction ();

		cb.setInputs (new ArrayList<TransactionInput> ());
		cb.setOutputs (new ArrayList<TransactionOutput> ());

		TransactionOutput out = new TransactionOutput ();
		out.setValue (value);
		cb.getOutputs ().add (out);

		out.setScript (address.getAddressScript ());

		TransactionInput in = new TransactionInput ();
		in.setSourceHash (Hash.ZERO_HASH_STRING);
		in.setIx (0);
		cb.getInputs ().add (in);

		ScriptFormat.Writer writer = new ScriptFormat.Writer ();
		writer.writeInt32 (blockHeight);
		in.setScript (writer.toByteArray ());

		cb.computeHash ();
		return cb;
	}

	public long getVersion ()
	{
		return version;
	}

	public String getBlockHash ()
	{
		return blockHash;
	}

	public void setBlockHash (String blockHash)
	{
		this.blockHash = blockHash;
	}

	public void setVersion (long version)
	{
		this.version = version;
	}

	public long getLockTime ()
	{
		return lockTime;
	}

	public void setLockTime (long lockTime)
	{
		this.lockTime = lockTime;
	}

	public long getBlocktime ()
	{
		return blocktime;
	}

	public void setBlocktime (long blocktime)
	{
		this.blocktime = blocktime;
	}

	public void computeHash ()
	{
		WireFormat.Writer writer = new WireFormat.Writer ();
		toWire (writer);
		WireFormat.Reader reader = new WireFormat.Reader (writer.toByteArray ());
		hash = reader.hash ().toString ();

		long ix = 0;
		for ( TransactionOutput o : outputs )
		{
			o.setIx (ix);
			o.setTxHash (hash);
			++ix;
		}
	}

	public String getHash ()
	{
		if ( hash == null )
		{
			computeHash ();
		}
		return hash;
	}

	public void setHash (String hash)
	{
		this.hash = hash;
	}

	public List<TransactionInput> getInputs ()
	{
		return inputs;
	}

	public void setInputs (List<TransactionInput> inputs)
	{
		this.inputs = inputs;
	}

	public List<TransactionOutput> getOutputs ()
	{
		return outputs;
	}

	public void setOutputs (List<TransactionOutput> outputs)
	{
		this.outputs = outputs;
	}

	public boolean isExpired ()
	{
		return expired;
	}

	public void setExpired (boolean expired)
	{
		this.expired = expired;
	}

	public String getOffendingTx ()
	{
		return offendingTx;
	}

	public void setOffendingTx (String offendingTx)
	{
		this.offendingTx = offendingTx;
	}

	public int getHeight ()
	{
		return height;
	}

	public void setHeight (int height)
	{
		this.height = height;
	}

	public void toWire (WireFormat.Writer writer)
	{
		writer.writeUint32 (version);
		if ( inputs != null )
		{
			writer.writeVarInt (inputs.size ());
			for ( TransactionInput input : inputs )
			{
				input.toWire (writer);
			}
		}
		else
		{
			writer.writeVarInt (0);
		}

		if ( outputs != null )
		{
			writer.writeVarInt (outputs.size ());
			for ( TransactionOutput output : outputs )
			{
				output.toWire (writer);
			}
		}
		else
		{
			writer.writeVarInt (0);
		}

		writer.writeUint32 (lockTime);
	}

	public static Transaction fromWire (WireFormat.Reader reader)
	{
		Transaction t = new Transaction ();

		int cursor = reader.getCursor ();

		t.version = reader.readUint32 ();
		long nin = reader.readVarInt ();
		if ( nin > 0 )
		{
			t.inputs = new ArrayList<TransactionInput> ();
			for ( int i = 0; i < nin; ++i )
			{
				t.inputs.add (TransactionInput.fromWire (reader));
			}
		}
		else
		{
			t.inputs = null;
		}

		long nout = reader.readVarInt ();
		if ( nout > 0 )
		{
			t.outputs = new ArrayList<TransactionOutput> ();
			for ( long i = 0; i < nout; ++i )
			{
				t.outputs.add (TransactionOutput.fromWire (reader));
			}
		}
		else
		{
			t.outputs = null;
		}

		t.lockTime = reader.readUint32 ();

		t.hash = reader.hash (cursor, reader.getCursor () - cursor).toString ();

		return t;
	}

	public static Transaction fromWireDump (String dump)
	{
		return fromWire (new WireFormat.Reader (ByteUtils.fromHex (dump)));
	}

	public String toWireDump ()
	{
		WireFormat.Writer writer = new WireFormat.Writer ();
		toWire (writer);
		return ByteUtils.toHex (writer.toByteArray ());
	}

	@Override
	public Transaction clone () throws CloneNotSupportedException
	{
		Transaction t = (Transaction) super.clone ();

		t.version = version;
		if ( inputs != null )
		{
			t.inputs = new ArrayList<TransactionInput> (inputs.size ());
			for ( TransactionInput i : inputs )
			{
				t.inputs.add (i.clone ());
			}
		}
		if ( outputs != null )
		{
			t.outputs = new ArrayList<TransactionOutput> (outputs.size ());
			for ( TransactionOutput o : outputs )
			{
				t.outputs.add (o.clone ());
			}
		}

		t.lockTime = lockTime;

		t.hash = hash;

		t.blockHash = blockHash;

		t.blocktime = blocktime;

		return t;
	}

	public BCSAPIMessage.Transaction toProtobuf ()
	{
		BCSAPIMessage.Transaction.Builder builder = BCSAPIMessage.Transaction.newBuilder ();
		builder.setLocktime ((int) lockTime);
		builder.setVersion ((int) version);
		if ( inputs != null )
		{
			for ( TransactionInput i : inputs )
			{
				builder.addInputs (i.toProtobuf ());
			}
		}
		if ( outputs != null && outputs.size () > 0 )
		{
			for ( TransactionOutput o : outputs )
			{
				builder.addOutputs (o.toProtobuf ());
			}
		}
		if ( expired )
		{
			builder.setExpired (true);
		}
		return builder.build ();
	}

	public static Transaction fromProtobuf (BCSAPIMessage.Transaction pt)
	{
		Transaction transaction = new Transaction ();
		transaction.setLockTime (pt.getLocktime ());
		transaction.setVersion (pt.getVersion ());
		if ( pt.getInputsCount () > 0 )
		{
			transaction.setInputs (new ArrayList<TransactionInput> ());
			for ( BCSAPIMessage.TransactionInput i : pt.getInputsList () )
			{
				transaction.getInputs ().add (TransactionInput.fromProtobuf (i));
			}
		}

		if ( pt.getOutputsCount () > 0 )
		{
			transaction.setOutputs (new ArrayList<TransactionOutput> ());
			for ( BCSAPIMessage.TransactionOutput o : pt.getOutputsList () )
			{
				transaction.getOutputs ().add (TransactionOutput.fromProtobuf (o));
			}
		}
		if ( pt.hasExpired () && pt.getExpired () )
		{
			transaction.expired = true;
		}
		transaction.computeHash ();
		return transaction;
	}

	public byte[] hashTransaction (int inr, int hashType, byte[] script) throws ValidationException
	{
		Transaction copy;
		try
		{
			copy = clone ();
		}
		catch ( CloneNotSupportedException e1 )
		{
			return null;
		}

		// implicit SIGHASH_ALL
		int i = 0;
		for ( TransactionInput in : copy.getInputs () )
		{
			if ( i == inr )
			{
				in.setScript (script);
			}
			else
			{
				in.setScript (new byte[0]);
			}
			++i;
		}

		if ( (hashType & 0x1f) == ScriptFormat.SIGHASH_NONE )
		{
			copy.getOutputs ().clear ();
			i = 0;
			for ( TransactionInput in : copy.getInputs () )
			{
				if ( i != inr )
				{
					in.setSequence (0);
				}
				++i;
			}
		}
		else if ( (hashType & 0x1f) == ScriptFormat.SIGHASH_SINGLE )
		{
			int onr = inr;
			if ( onr >= copy.getOutputs ().size () )
			{
				// this is a Satoshi client bug.
				// This case should throw an error but it instead retuns 1 that is not checked and interpreted as below
				return ByteUtils.fromHex ("0100000000000000000000000000000000000000000000000000000000000000");
			}
			for ( i = copy.getOutputs ().size () - 1; i > onr; --i )
			{
				copy.getOutputs ().remove (i);
			}
			for ( i = 0; i < onr; ++i )
			{
				copy.getOutputs ().get (i).setScript (new byte[0]);
				copy.getOutputs ().get (i).setValue (-1L);
			}
			i = 0;
			for ( TransactionInput in : copy.getInputs () )
			{
				if ( i != inr )
				{
					in.setSequence (0);
				}
				++i;
			}
		}
		if ( (hashType & ScriptFormat.SIGHASH_ANYONECANPAY) != 0 )
		{
			List<TransactionInput> oneIn = new ArrayList<> ();
			oneIn.add (copy.getInputs ().get (inr));
			copy.setInputs (oneIn);
		}

		WireFormat.Writer writer = new WireFormat.Writer ();
		copy.toWire (writer);

		byte[] txwire = writer.toByteArray ();
		byte[] hash = null;
		try
		{
			MessageDigest a = MessageDigest.getInstance ("SHA-256");
			a.update (txwire);
			a.update (new byte[] { (byte) (hashType & 0xff), 0, 0, 0 });
			hash = a.digest (a.digest ());
		}
		catch ( NoSuchAlgorithmException ignored )
		{
		}
		return hash;
	}

	@Override
	public int hashCode ()
	{
		return hash.hashCode ();
	}

	@Override
	public boolean equals (Object obj)
	{
		return hash.equals (((Transaction) obj).getHash ());
	}

}
