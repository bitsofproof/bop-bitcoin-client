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
import java.util.Objects;

import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.WireFormat;
import com.google.protobuf.ByteString;

/**
 * Input of a transaction (reference to a previous transaction output)
 */
public class TransactionInput implements Serializable, Cloneable
{
	private static final long serialVersionUID = -7019826355856117874L;

	public static Builder create()
	{
		return new Builder();
	}

	public static final class Builder
	{
		private String sourceHash;
		private long ix;
		private long sequence = 0xFFFFFFFFL;
		private byte[] script;

		public Builder sourceHash(String sourceHash)
		{
			this.sourceHash = sourceHash;
			return this;
		}

		public Builder ix(long ix)
		{
			this.ix = ix;
			return this;
		}

		public Builder sequence(long sequence)
		{
			this.sequence = sequence;
			return this;
		}

		public Builder script(byte[] script)
		{
			this.script = script;
			return this;
		}

		public TransactionInput build()
		{
			Objects.requireNonNull(script, "TransactionInput script must not be null");

			return new TransactionInput(sourceHash, ix, sequence, script);
		}
	}

	private String sourceHash;
	private long ix;
	private long sequence = 0xFFFFFFFFL;
	private byte[] script;

	public TransactionInput ()
	{
	}

	public TransactionInput(String sourceHash, long ix, long sequence, byte[] script)
	{
		this.sourceHash = sourceHash;
		this.ix = ix;
		this.sequence = sequence;
		this.script = script;
	}

	/**
	 * Referenced transaction
	 * 
	 * @return
	 */
	public String getSourceHash ()
	{
		return sourceHash;
	}

	/**
	 * Set reference
	 * 
	 * @param sourceHash
	 */
	public void setSourceHash (String sourceHash)
	{
		this.sourceHash = sourceHash;
	}

	/**
	 * Spend this output number of the transaction referenced. (0 is first)
	 * 
	 * @return
	 */
	public long getIx ()
	{
		return ix;
	}

	/**
	 * Set the output number to spend from referenced transaction
	 * 
	 * @param ix
	 */
	public void setIx (long ix)
	{
		this.ix = ix;
	}

	/**
	 * Sequence (version) number of the input. 0xfffff means final
	 * 
	 * @return
	 */
	public long getSequence ()
	{
		return sequence;
	}

	/**
	 * Sequence (version) number of the input. 0xfffff means final
	 * 
	 * @param sequence
	 */
	public void setSequence (long sequence)
	{
		this.sequence = sequence;
	}

	/**
	 * Script of the input. Signatures and script for P2SH
	 * 
	 * @return
	 */
	public byte[] getScript ()
	{
		if ( script != null )
		{
			byte[] copy = new byte[script.length];
			System.arraycopy (script, 0, copy, 0, script.length);
			return copy;
		}
		return null;
	}

	/**
	 * Set input script. Signatures and script for P2SH
	 * 
	 * @param script
	 */
	public void setScript (byte[] script)
	{
		if ( script != null )
		{
			this.script = new byte[script.length];
			System.arraycopy (script, 0, this.script, 0, script.length);
		}
		else
		{
			this.script = null;
		}
	}

	/**
	 * Write the input to a wire format writer
	 * 
	 * @param writer
	 */
	public void toWire (WireFormat.Writer writer)
	{
		if ( sourceHash != null && !sourceHash.equals (Hash.ZERO_HASH.toString ()) )
		{
			writer.writeHash (new Hash (sourceHash));
			writer.writeUint32 (ix);
		}
		else
		{
			writer.writeBytes (Hash.ZERO_HASH.toByteArray ());
			writer.writeUint32 (-1);
		}
		writer.writeVarBytes (script);
		writer.writeUint32 (sequence);
	}

	/**
	 * Recreate transaction input reference from its wire format
	 * 
	 * @param reader
	 * @return
	 */
	public static TransactionInput fromWire (WireFormat.Reader reader)
	{
		TransactionInput i = new TransactionInput ();

		i.sourceHash = reader.readHash ().toString ();
		i.ix = reader.readUint32 ();
		i.script = reader.readVarBytes ();
		i.sequence = reader.readUint32 ();

		return i;
	}

	@Override
	public TransactionInput clone () throws CloneNotSupportedException
	{
		TransactionInput i = (TransactionInput) super.clone ();

		i.sourceHash = sourceHash;
		i.ix = ix;
		i.sequence = sequence;
		if ( script != null )
		{
			i.script = new byte[script.length];
			System.arraycopy (script, 0, i.script, 0, script.length);
		}

		return i;
	}

	/**
	 * Transaction input to protobuf conversion for server communication
	 * 
	 * @return
	 */
	public BCSAPIMessage.TransactionInput toProtobuf ()
	{
		BCSAPIMessage.TransactionInput.Builder builder = BCSAPIMessage.TransactionInput.newBuilder ();
		builder.setScript (ByteString.copyFrom (script));
		builder.setSequence ((int) sequence);
		builder.setSource (ByteString.copyFrom (new Hash (sourceHash).toByteArray ()));
		builder.setSourceix ((int) ix);
		return builder.build ();
	}

	/**
	 * Recreate transaction input reference from protobuf message
	 * 
	 * @param pi
	 * @return
	 */
	public static TransactionInput fromProtobuf (BCSAPIMessage.TransactionInput pi)
	{
		TransactionInput input = new TransactionInput ();
		input.setIx (pi.getSourceix ());
		input.setScript (pi.getScript ().toByteArray ());
		input.setSequence (pi.getSequence ());
		input.setSourceHash (new Hash (pi.getSource ().toByteArray ()).toString ());
		return input;
	}
}
