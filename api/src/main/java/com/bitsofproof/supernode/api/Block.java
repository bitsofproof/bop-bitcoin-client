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
import java.util.List;

import com.bitsofproof.supernode.common.BinaryAggregator;
import com.bitsofproof.supernode.common.ByteUtils;
import com.bitsofproof.supernode.common.Hash;
import com.bitsofproof.supernode.common.WireFormat;
import com.google.protobuf.ByteString;

public class Block implements Serializable, Cloneable
{
	private static final long serialVersionUID = 2846027750944390897L;

	private Hash hash;
	private long version;
	private Hash previousHash;
	private Hash merkleRoot;
	private long createTime;
	private long difficultyTarget;
	private long nonce;
	List<Transaction> transactions;
	private int height;

	public Block ()
	{
		transactions = new ArrayList<> ();
	}

	@Override
	public Block clone () throws CloneNotSupportedException
	{
		Block c = (Block) super.clone ();
		c.hash = hash;
		c.previousHash = hash;
		c.merkleRoot = merkleRoot;
		c.transactions = new ArrayList<> (transactions.size ());
		for ( Transaction t : transactions )
		{
			c.transactions.add (t.clone ());
		}

		return c;
	}

	public Hash getHash ()
	{
		return hash;
	}

	public long getVersion ()
	{
		return version;
	}

	public void setVersion (long version)
	{
		this.version = version;
	}

	public Hash getPreviousHash ()
	{
		return previousHash;
	}

	public void setPreviousHash (Hash previousHash)
	{
		this.previousHash = previousHash;
	}

	public void computeHash (boolean merkle)
	{
		if ( merkle )
		{
			merkleRoot = computeMerkleRoot ();
		}

		WireFormat.Writer writer = new WireFormat.Writer ();
		toWireHeaderOnly (writer);
		WireFormat.Reader reader = new WireFormat.Reader (writer.toByteArray ());

		hash = reader.hash ();
		if ( transactions != null )
		{
			for ( Transaction t : transactions )
			{
				t.setBlockHash (hash);
			}
		}
	}

	public Hash computeMerkleRoot ()
	{
		if ( transactions != null )
		{
			ArrayList<byte[]> tree = new ArrayList<> ();
			for ( Transaction t : transactions )
			{
				t.computeHash ();
				tree.add (t.getHash ().toByteArray ());
			}
			BinaryAggregator<byte[]> aggregator = new BinaryAggregator<byte[]> ()
			{
				@Override
				public byte[] merge (byte[] a, byte[] b)
				{
					try
					{
						MessageDigest digest = MessageDigest.getInstance ("SHA-256");
						digest.update (a);
						return digest.digest (digest.digest (b));
					}
					catch ( NoSuchAlgorithmException e )
					{
						return null;
					}
				}
			};
			return new Hash (aggregator.aggregate (tree));
		}
		return null;
	}

	public Hash getMerkleRoot ()
	{
		return merkleRoot;
	}

	public void setHash (Hash hash)
	{
		this.hash = hash;
	}

	public void setMerkleRoot (Hash merkleRoot)
	{
		this.merkleRoot = merkleRoot;
	}

	public long getCreateTime ()
	{
		return createTime;
	}

	public void setCreateTime (long createTime)
	{
		this.createTime = createTime;
	}

	public long getDifficultyTarget ()
	{
		return difficultyTarget;
	}

	public void setDifficultyTarget (long difficultyTarget)
	{
		this.difficultyTarget = difficultyTarget;
	}

	public long getNonce ()
	{
		return nonce;
	}

	public void setNonce (long nonce)
	{
		this.nonce = nonce;
	}

	public List<Transaction> getTransactions ()
	{
		return transactions;
	}

	public void setTransactions (List<Transaction> transactions)
	{
		this.transactions = transactions;
	}

	public int getHeight ()
	{
		return height;
	}

	public void setHeight (int height)
	{
		this.height = height;
	}

	public void toWireHeaderOnly (WireFormat.Writer writer)
	{
		writer.writeUint32 (version);
		writer.writeHash (previousHash);
		writer.writeHash (merkleRoot);
		writer.writeUint32 (createTime);
		writer.writeUint32 (difficultyTarget);
		writer.writeUint32 (nonce);
	}

	public void toWire (WireFormat.Writer writer)
	{
		toWireHeaderOnly (writer);
		if ( transactions != null )
		{
			writer.writeVarInt (transactions.size ());
			for ( Transaction t : transactions )
			{
				t.toWire (writer);
			}
		}
		else
		{
			writer.writeVarInt (0);
		}
	}

	public static Block fromWire (WireFormat.Reader reader)
	{
		Block b = new Block ();

		int cursor = reader.getCursor ();
		b.version = reader.readUint32 ();

		b.previousHash = reader.readHash ();
		b.merkleRoot = reader.readHash ();
		b.createTime = reader.readUint32 ();
		b.difficultyTarget = reader.readUint32 ();
		b.nonce = reader.readUint32 ();
		b.hash = reader.hash (cursor, 80);
		long nt = reader.readVarInt ();
		if ( nt > 0 )
		{
			b.transactions = new ArrayList<> ();
			for ( long i = 0; i < nt; ++i )
			{
				b.transactions.add (Transaction.fromWire (reader));
			}
		}
		return b;
	}

	public static Block fromWireDump (String dump)
	{
		return fromWire (new WireFormat.Reader (ByteUtils.fromHex (dump)));
	}

	public String toWireDump ()
	{
		WireFormat.Writer writer = new WireFormat.Writer ();
		toWire (writer);
		return ByteUtils.toHex (writer.toByteArray ());
	}

	public BCSAPIMessage.Block toProtobuf ()
	{
		BCSAPIMessage.Block.Builder builder = BCSAPIMessage.Block.newBuilder ();
		builder.setVersion ((int) version);
		builder.setDifficulty ((int) difficultyTarget);
		builder.setNonce ((int) nonce);
		builder.setTimestamp ((int) createTime);
		builder.setMerkleRoot (ByteString.copyFrom (merkleRoot.toByteArray ()));
		builder.setPreviousBlock (ByteString.copyFrom (previousHash.toByteArray ()));
		if ( transactions != null )
		{
			for ( Transaction t : transactions )
			{
				builder.addTransactions (t.toProtobuf ());
			}
		}
		if ( height >= 0 )
		{
			builder.setHeight (height);
		}
		return builder.build ();
	}

	public static Block fromProtobuf (BCSAPIMessage.Block pb)
	{
		Block block = new Block ();
		block.setVersion (pb.getVersion ());
		block.setDifficultyTarget (pb.getDifficulty ());
		block.setNonce (pb.getNonce ());
		block.setCreateTime (pb.getTimestamp ());
		block.setPreviousHash (new Hash (pb.getPreviousBlock ().toByteArray ()));
		block.setMerkleRoot (new Hash (pb.getMerkleRoot ().toByteArray ()));
		if ( pb.getTransactionsCount () > 0 )
		{
			block.setTransactions (new ArrayList<Transaction> ());
			for ( BCSAPIMessage.Transaction t : pb.getTransactionsList () )
			{
				block.getTransactions ().add (Transaction.fromProtobuf (t));
			}
		}
		if ( pb.hasHeight () )
		{
			block.height = pb.getHeight ();
		}
		block.computeHash (false);
		return block;
	}
}
