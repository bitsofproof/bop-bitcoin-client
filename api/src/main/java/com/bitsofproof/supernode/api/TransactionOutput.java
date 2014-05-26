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
import java.util.Arrays;
import java.util.Objects;

import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ScriptFormat;
import com.bitsofproof.supernode.common.WireFormat;
import com.google.protobuf.ByteString;

public class TransactionOutput implements Serializable, Cloneable
{
	private static final long serialVersionUID = 3028618872354766234L;

	private Hash txHash;
	private long ix;
	private long value;
	private byte[] script;

	public TransactionOutput ()
	{
	}

	public TransactionOutput (long value, byte[] script)
	{
		this.value = value;
		this.setScript (script);
	}

	public TransactionOutput (Hash txHash, long ix, long value, byte[] script)
	{
		this(value, script);
		this.txHash = txHash;
		this.ix = ix;
	}

	public Hash getTxHash ()
	{
		return txHash;
	}

	public void setTxHash (Hash txHash)
	{
		this.txHash = txHash;
	}

	public long getIx ()
	{
		return ix;
	}

	public void setIx (long ix)
	{
		this.ix = ix;
	}

	public long getValue ()
	{
		return value;
	}

	public void setValue (long value)
	{
		this.value = value;
	}

	public byte[] getScript ()
	{
		if ( script != null )
		{
			return Arrays.copyOf(script, script.length);
		}
		return null;
	}

	public void setScript (byte[] script)
	{
		if ( script != null )
		{
			this.script = Arrays.copyOf(script, script.length);
		}
		else
		{
			this.script = null;
		}
	}

	public void toWire (WireFormat.Writer writer)
	{
		writer.writeUint64 (value);
		writer.writeVarBytes (script);
	}

	public static TransactionOutput fromWire (WireFormat.Reader reader)
	{
		return new TransactionOutput(reader.readUint64(),
		                             reader.readVarBytes ());
	}

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
			o.script = Arrays.copyOf (script, script.length);
		}
		o.ix = ix;
		o.txHash = txHash;
		return o;
	}

	public BCSAPIMessage.TransactionOutput toProtobuf ()
	{
		BCSAPIMessage.TransactionOutput.Builder builder = BCSAPIMessage.TransactionOutput.newBuilder ();
		builder.setScript (ByteString.copyFrom (script));
		builder.setValue (value);
		return builder.build ();
	}

	public static TransactionOutput fromProtobuf (BCSAPIMessage.TransactionOutput po)
	{
		return new TransactionOutput (po.getValue(),
		                              po.getScript ().toByteArray ());
	}

	@Override
	public int hashCode ()
	{
		return Objects.hash (txHash, ix);
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
		final TransactionOutput other = (TransactionOutput) obj;
		return Objects.equals (this.txHash, other.txHash) && Objects.equals (this.ix, other.ix);
	}
}
