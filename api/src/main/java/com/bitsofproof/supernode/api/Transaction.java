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
import java.util.Objects;

import com.bitsofproof.supernode.common.ByteUtils;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ScriptFormat;
import com.bitsofproof.supernode.common.ValidationException;
import com.bitsofproof.supernode.common.WireFormat;
import com.google.protobuf.ByteString;

public class Transaction implements Serializable, Cloneable
{
	private static final long serialVersionUID = 690918485496086537L;

	private long version = 1;

	private long lockTime = 0;

	private List<TransactionInput> inputs;
	private List<TransactionOutput> outputs;

	// below are not part of P2P wire format
	private boolean expired = false;
	private Hash blockHash;
	private int height = 0;
	private long blocktime = new Date ().getTime () / 1000;
	// below is not part of server messages, but populated in the client library
	private Hash hash;
	private Hash offendingTx;

	public static Transaction createCoinbase (Address address, long value, int blockHeight) throws ValidationException
	{
		Transaction cb = new Transaction ();

		TransactionOutput out = new TransactionOutput ();
		out.setValue (value);
		cb.addOutput (out);

		out.setScript (address.getAddressScript ());

		TransactionInput in = new TransactionInput (Hash.ZERO_HASH, 0, ScriptFormat.writer ()
		                                                                           .writeInt32 (blockHeight)
		                                                                           .toByteArray ());
		cb.addInput (in);

		cb.computeHash ();
		return cb;
	}

	public Transaction ()
	{
		inputs = new ArrayList<> ();
		outputs = new ArrayList<> ();
	}

	public long getVersion ()
	{
		return version;
	}

	public Hash getBlockHash ()
	{
		return blockHash;
	}

	public void setBlockHash (Hash blockHash)
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
		hash = reader.hash ();

		long ix = 0;
		for ( TransactionOutput o : outputs )
		{
			o.setIx (ix);
			o.setTxHash (hash);
			++ix;
		}
	}

	public Hash getHash ()
	{
		if ( hash == null )
		{
			computeHash ();
		}
		return hash;
	}

	public void setHash (Hash hash)
	{
		this.hash = hash;
	}

	public List<TransactionInput> getInputs ()
	{
		return inputs;
	}

	public TransactionInput getInput(int i)
	{
		return inputs.get(i);
	}

	public void addInput(TransactionInput input)
	{
		inputs.add(input);
	}

	public List<TransactionOutput> getOutputs ()
	{
		return outputs;
	}

	public TransactionOutput getOutput(int i)
	{
		return outputs.get(i);
	}

	public void addOutput(TransactionOutput output)
	{
		outputs.add(output);
	}

	public boolean isExpired ()
	{
		return expired;
	}

	public void setExpired (boolean expired)
	{
		this.expired = expired;
	}

	public Hash getOffendingTx ()
	{
		return offendingTx;
	}

	public void setOffendingTx (Hash offendingTx)
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
		writer.writeVarInt (inputs.size ());
		for ( TransactionInput input : inputs )
		{
			input.toWire (writer);
		}

		writer.writeVarInt (outputs.size ());
		for ( TransactionOutput output : outputs )
		{
			output.toWire (writer);
		}

		writer.writeUint32 (lockTime);
	}

	public static Transaction fromWire (WireFormat.Reader reader)
	{
		Transaction t = new Transaction ();

		int cursor = reader.getCursor ();

		t.version = reader.readUint32 ();
		long nin = reader.readVarInt ();
		for ( int i = 0; i < nin; ++i )
		{
			t.addInput (TransactionInput.fromWire (reader));
		}

		long nout = reader.readVarInt ();
		for ( long i = 0; i < nout; ++i )
		{
			t.addOutput (TransactionOutput.fromWire (reader));
		}

		t.lockTime = reader.readUint32 ();
		t.hash = reader.hash (cursor, reader.getCursor () - cursor);

		for ( int i = 0; i < t.getOutputs().size (); i++)
		{
			TransactionOutput output = t.getOutputs ().get(i);
			output.setTxHash (t.hash);
			output.setIx (i);
		}

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
		t.inputs = new ArrayList<>();
		t.outputs = new ArrayList<>();

		for ( TransactionInput i : inputs )
		{
			t.addInput (i.clone ());
		}
		for ( TransactionOutput o : outputs )
		{
			t.addOutput (o.clone ());
		}

		return t;
	}

	public BCSAPIMessage.Transaction toProtobuf ()
	{
		BCSAPIMessage.Transaction.Builder builder = BCSAPIMessage.Transaction.newBuilder ();
		builder.setLocktime ((int) lockTime);
		builder.setVersion ((int) version);
		for ( TransactionInput i : inputs )
		{
			builder.addInputs (i.toProtobuf ());
		}
		for ( TransactionOutput o : outputs )
		{
			builder.addOutputs (o.toProtobuf ());
		}
		if ( blockHash != null )
		{
			builder.setBlock (ByteString.copyFrom (blockHash.toByteArray ()));
		}
		if ( expired )
		{
			builder.setExpired (true);
		}
		if ( height != 0 )
		{
			builder.setHeight (height);
		}
		if ( blocktime != 0 )
		{
			builder.setBlocktime ((int) blocktime);
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
			for ( BCSAPIMessage.TransactionInput i : pt.getInputsList () )
			{
				transaction.addInput (TransactionInput.fromProtobuf (i));
			}
		}

		if ( pt.getOutputsCount () > 0 )
		{
			for ( BCSAPIMessage.TransactionOutput o : pt.getOutputsList () )
			{
				transaction.addOutput (TransactionOutput.fromProtobuf (o));
			}
		}
		if ( pt.hasBlock () )
		{
			transaction.blockHash = new Hash (pt.getBlock ().toByteArray ());
		}
		if ( pt.hasExpired () && pt.getExpired () )
		{
			transaction.expired = true;
		}
		if ( pt.hasHeight () )
		{
			transaction.height = pt.getHeight ();
		}
		if ( pt.hasBlocktime () )
		{
			transaction.blocktime = pt.getBlocktime ();
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
		for ( TransactionInput in : copy.inputs )
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
			copy.outputs.clear ();
			i = 0;
			for ( TransactionInput in : copy.inputs )
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
			if ( onr >= copy.outputs.size () )
			{
				// this is a Satoshi client bug.
				// This case should throw an error but it instead retuns 1 that is not checked and interpreted as below
				return ByteUtils.fromHex ("0100000000000000000000000000000000000000000000000000000000000000");
			}
			for ( i = copy.outputs.size () - 1; i > onr; --i )
			{
				copy.outputs.remove (i);
			}
			for ( i = 0; i < onr; ++i )
			{
				copy.outputs.get (i).setScript (new byte[0]);
				copy.outputs.get (i).setValue (-1L);
			}
			i = 0;
			for ( TransactionInput in : copy.inputs )
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
			TransactionInput oneInput = copy.inputs.get (inr);
			copy.inputs.clear ();
			copy.addInput (oneInput);
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
		return Objects.hash (hash);
	}

	@Override
	public boolean equals (Object obj)
	{
		if ( this == obj )
		{
			return true;
		}
		if ( obj == null || getClass () != obj.getClass () )
		{
			return false;
		}
		final Transaction other = (Transaction) obj;
		return Objects.equals (this.hash, other.hash);
	}
}
