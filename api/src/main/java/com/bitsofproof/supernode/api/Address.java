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

import org.bouncycastle.util.Arrays;

import com.bitsofproof.supernode.common.ByteUtils;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.ScriptFormat;
import com.bitsofproof.supernode.common.ScriptFormat.Opcode;
import com.bitsofproof.supernode.common.ValidationException;

/**
 * A public address in the ledger
 */
public class Address
{
	/**
	 * Supported Address types are
	 * 
	 * <pre>
	 * COMMON - digest of a single public key
	 * P2SH - digest of a script
	 * </pre>
	 */
	public enum Type
	{
		COMMON, P2SH
	};

	private final Type type;
	private final byte[] bytes;

	private Network network = Network.PRODUCTION;

	public Network getNetwork ()
	{
		return network;
	}

	public void setNetwork (Network network)
	{
		this.network = network;
	}

	/**
	 * Create an address
	 * 
	 * @param network
	 *            - Network PRODUCTION or TEST
	 * @param type
	 *            - COMMON or P2SH
	 * @param address
	 *            - digest of key (COMMON) or script (P2SH)
	 * @throws ValidationException
	 *             - thrown if digest length is not 20 bytes
	 */
	public Address (Network network, Type type, byte[] address) throws ValidationException
	{
		this.network = network;
		this.type = type;
		if ( address.length != 20 )
		{
			throw new ValidationException ("invalid digest length for an address");
		}
		this.bytes = Arrays.clone (address);
	}

	/**
	 * Create an address
	 * 
	 * @param type
	 *            - COMMON or P2SH
	 * @param address
	 *            - digest of key (COMMON) or script (P2SH)
	 * @throws ValidationException
	 *             - thrown if digest length is not 20 bytes
	 */
	public Address (Type type, byte[] address) throws ValidationException
	{
		this.type = type;
		if ( address.length != 20 )
		{
			throw new ValidationException ("invalid digest length for an address");
		}
		this.bytes = Arrays.clone (address);
	}

	/**
	 * Create an address
	 * 
	 * @param network
	 *            - Network PRODUCTION or TEST
	 * @param address
	 *            - an other address
	 */
	public Address (Network network, Address address) throws ValidationException
	{
		this.network = network;
		this.type = address.type;
		this.bytes = Arrays.clone (address.bytes);
	}

	public Type getType ()
	{
		return type;
	}

	/**
	 * get the address digest
	 * 
	 * @return digest
	 */
	public byte[] toByteArray ()
	{
		return Arrays.clone (bytes);
	}

	/**
	 * get the transaction output script suitable to refer to this address
	 * 
	 * @return transaction output script
	 * @throws ValidationException
	 *             - if output script is unknown for this address
	 */
	public byte[] getAddressScript () throws ValidationException
	{
		ScriptFormat.Writer writer = new ScriptFormat.Writer ();
		if ( type == Address.Type.COMMON )
		{
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_DUP));
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_HASH160));
			writer.writeData (bytes);
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_EQUALVERIFY));
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_CHECKSIG));
		}
		else if ( type == Address.Type.P2SH )
		{
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_HASH160));
			writer.writeData (bytes);
			writer.writeToken (new ScriptFormat.Token (Opcode.OP_EQUAL));
		}
		else
		{
			throw new ValidationException ("unknown sink address type");
		}
		return writer.toByteArray ();
	}

	@Override
	public int hashCode ()
	{
		return Arrays.hashCode (bytes) + type.ordinal ();
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
		return Arrays.areEqual (bytes, ((Address) obj).bytes) && type == ((Address) obj).type;
	}

	/**
	 * @return - human readable representation of the address
	 */
	@Override
	public String toString ()
	{
		try
		{
			return toSatoshiStyle (this);
		}
		catch ( ValidationException e )
		{
			return network.name () + ":" + type.name () + ":" + ByteUtils.toHex (bytes);
		}
	}

	/**
	 * Convert a human readable address string to an address object
	 * 
	 * @param address
	 *            - the human readable address
	 * @return an address object with type and network ad encoded in the readable string
	 * @throws ValidationException
	 *             - for malformed address strings
	 */
	public static Address fromSatoshiStyle (String address) throws ValidationException
	{
		try
		{
			Network network = Network.PRODUCTION;
			Address.Type type = Type.COMMON;
			byte[] raw = ByteUtils.fromBase58 (address);
			if ( (raw[0] & 0xff) == 0x0 )
			{
				network = Network.PRODUCTION;
				type = Address.Type.COMMON;
			}
			if ( (raw[0] & 0xff) == 5 )
			{
				network = Network.PRODUCTION;
				type = Address.Type.P2SH;
			}
			if ( (raw[0] & 0xff) == 0x6f )
			{
				network = Network.TEST;
				type = Address.Type.COMMON;
			}
			if ( (raw[0] & 0xff) == 196 )
			{
				network = Network.TEST;
				type = Address.Type.P2SH;
			}
			byte[] check = Hash.hash (raw, 0, raw.length - 4);
			for ( int i = 0; i < 4; ++i )
			{
				if ( check[i] != raw[raw.length - 4 + i] )
				{
					throw new ValidationException ("Address checksum mismatch");
				}
			}
			byte[] keyDigest = new byte[raw.length - 5];
			System.arraycopy (raw, 1, keyDigest, 0, raw.length - 5);
			return new Address (network, type, keyDigest);
		}
		catch ( Exception e )
		{
			throw new ValidationException (e);
		}
	}

	/**
	 * Convert a human readable address string to an address object
	 * 
	 * @param address
	 *            - the human readable address
	 * @param addressFlag
	 *            - a flag encoded in the first byte of string representation
	 * @return
	 * @throws ValidationException
	 */
	@Deprecated
	public static byte[] fromSatoshiStyle (String address, int addressFlag) throws ValidationException
	{
		try
		{
			byte[] raw = ByteUtils.fromBase58 (address);
			if ( raw[0] != (byte) (addressFlag & 0xff) )
			{
				throw new ValidationException ("invalid address for this chain");
			}
			byte[] check = Hash.hash (raw, 0, raw.length - 4);
			for ( int i = 0; i < 4; ++i )
			{
				if ( check[i] != raw[raw.length - 4 + i] )
				{
					throw new ValidationException ("Address checksum mismatch");
				}
			}
			byte[] keyDigest = new byte[raw.length - 5];
			System.arraycopy (raw, 1, keyDigest, 0, raw.length - 5);
			return keyDigest;
		}
		catch ( Exception e )
		{
			throw new ValidationException (e);
		}
	}

	/**
	 * Convert to a human readable address string from a digest
	 * 
	 * @param keyDigest
	 *            - the digest
	 * @param addressFlag
	 *            - a flag encoded in the first byte of string representation
	 * @return
	 * @throws ValidationException
	 */
	@Deprecated
	public static String toSatoshiStyle (byte[] keyDigest, int addressFlag)
	{
		byte[] addressBytes = new byte[1 + keyDigest.length + 4];
		addressBytes[0] = (byte) (addressFlag & 0xff);
		System.arraycopy (keyDigest, 0, addressBytes, 1, keyDigest.length);
		byte[] check = Hash.hash (addressBytes, 0, keyDigest.length + 1);
		System.arraycopy (check, 0, addressBytes, keyDigest.length + 1, 4);
		return ByteUtils.toBase58 (addressBytes);
	}

	/**
	 * Convert to a human readable (BASE58) address with network and type prefixes.
	 * 
	 * @param address
	 * @return human readable representation of the address
	 * @throws ValidationException
	 */
	public static String toSatoshiStyle (Address address) throws ValidationException
	{
		byte[] keyDigest = address.toByteArray ();
		int addressFlag;
		if ( address.getNetwork () == Network.PRODUCTION )
		{
			if ( address.getType () == Address.Type.COMMON )
			{
				addressFlag = 0x0;
			}
			else if ( address.getType () == Address.Type.P2SH )
			{
				addressFlag = 0x5;
			}
			else
			{
				throw new ValidationException ("unknown address type");
			}
		}
		else if ( address.getNetwork () == Network.TEST )
		{
			if ( address.getType () == Address.Type.COMMON )
			{
				addressFlag = 0x6f;
			}
			else if ( address.getType () == Address.Type.P2SH )
			{
				addressFlag = 196;
			}
			else
			{
				throw new ValidationException ("unknown address type");
			}
		}
		else
		{
			throw new ValidationException ("unknown network");
		}
		byte[] addressBytes = new byte[1 + keyDigest.length + 4];
		addressBytes[0] = (byte) (addressFlag & 0xff);
		System.arraycopy (keyDigest, 0, addressBytes, 1, keyDigest.length);
		byte[] check = Hash.hash (addressBytes, 0, keyDigest.length + 1);
		System.arraycopy (check, 0, addressBytes, keyDigest.length + 1, 4);
		return ByteUtils.toBase58 (addressBytes);
	}
}
