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

import com.bitsofproof.supernode.common.ScriptFormat;
import com.bitsofproof.supernode.common.ValidationException;
import com.bitsofproof.supernode.common.WireFormat;
import com.google.protobuf.ByteString;

/**
 * An output of a transaction Contains the value reallocated and a script that needs to be fulfilled to spend it again.
 */
public class TransactionOutput implements Serializable, Cloneable
{
	private static final long serialVersionUID = 3028618872354766234L;

	public static Builder create()
	{
		return new Builder();
	}

	public static final class Builder
	{
		private long value;
		private byte[] script;

		public Builder value(long v)
		{
			value = v;
			return this;
		}

		public Builder script(byte[] s)
		{
			script = s;
			return this;
		}

		public Builder payTo(Address address) throws ValidationException
		{
			script = address.getAddressScript();
			return this;
		}

		public TransactionOutput build()
		{
			Objects.requireNonNull(script, "TransactionOutput script must be not null");

			return new TransactionOutput(value, script);
		}
	}

	private String txHash;
	private long ix;
	private long value;
	private byte[] script;

	public TransactionOutput()
	{
	}

	public TransactionOutput(long value, byte[] script)
	{
		this.value = value;
		this.script = script;
	}

	/**
	 * Hash of the transaction this output is part of. This is computed if hash of the transaction is computed.
	 */
	public String getTxHash ()
	{
		return txHash;
	}

	/**
	 * Set the transaction hash
	 */
	public void setTxHash (String txHash)
	{
		this.txHash = txHash;
	}

	/**
	 * Get the order number of this output within the transaction (starts with 0)
	 */
	public long getIx ()
	{
		return ix;
	}

	/**
	 * Set the order number of this output within the transaction (starts with 0)
	 */
	public void setIx (long ix)
	{
		this.ix = ix;
	}

	/**
	 * get the value reallocated
	 */
	public long getValue ()
	{
		return value;
	}

	/**
	 * set value to be reallocated
	 */
	public void setValue (long value)
	{
		this.value = value;
	}

	/**
	 * get the script a transaction spending this output has to fulfill This is usually an Address script, @see Address.getAddressScript
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
	 * set the script a transaction spending this output has to fulfill This is usually an Address script, @see Address.getAddressScript
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
	 * write the output in wire format to a writer
	 */
	public void toWire (WireFormat.Writer writer)
	{
		writer.writeUint64 (value);
		writer.writeVarBytes (script);
	}

	/**
	 * recreate the output from wire format
	 */
	public static TransactionOutput fromWire (WireFormat.Reader reader)
	{
		TransactionOutput o = new TransactionOutput ();
		o.value = reader.readUint64 ();
		o.script = reader.readVarBytes ();
		return o;
	}

	/**
	 * Get the address referenced in the script. The parse might fail for not plain-vanilla transactions and return null
	 */
	public Address getOutputAddress ()
	{
		return ScriptFormat.getAddress (script);
	}

	@Override
	public TransactionOutput clone () throws CloneNotSupportedException
	{
		TransactionOutput o = (TransactionOutput) super.clone ();
		o.value = value;
		if ( script != null )
		{
			o.script = new byte[script.length];
			System.arraycopy (script, 0, o.script, 0, script.length);
		}
		o.ix = ix;
		o.txHash = txHash;
		return o;
	}

	/**
	 * Write the output in protobuf format for server communication
	 */
	public BCSAPIMessage.TransactionOutput toProtobuf ()
	{
		BCSAPIMessage.TransactionOutput.Builder builder = BCSAPIMessage.TransactionOutput.newBuilder ();
		builder.setScript (ByteString.copyFrom (script));
		builder.setValue (value);
		return builder.build ();
	}

	/**
	 * recreate the output from protobuf server message
	 */
	public static TransactionOutput fromProtobuf (BCSAPIMessage.TransactionOutput po)
	{
		TransactionOutput output = new TransactionOutput ();
		output.setScript (po.getScript ().toByteArray ());
		output.setValue (po.getValue ());
		return output;
	}

	@Override
	public int hashCode ()
	{
		return txHash.hashCode ();
	}

	@Override
	public boolean equals (Object obj)
	{
		if ( obj instanceof TransactionOutput )
		{
			return ((TransactionOutput) obj).txHash.equals (txHash) && ((TransactionOutput) obj).ix == ix;
		}
		return false;
	}
}
