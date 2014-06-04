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

/**
 * A Bitcoin block.
 * 
 * A Block consists of a header that features the proof of work and an optional transaction list.
 */
public class Block implements Serializable, Cloneable
{
	private static final long serialVersionUID = 2846027750944390897L;

	private String hash;
	private long version;
	private String previousHash;
	private String merkleRoot;
	private long createTime;
	private long difficultyTarget;
	private long nonce;
	List<Transaction> transactions;
	private int height;

	@Override
	public Block clone () throws CloneNotSupportedException
	{
		Block c = (Block) super.clone ();
		c.hash = hash;
		c.version = version;
		c.previousHash = previousHash;
		c.merkleRoot = merkleRoot;
		c.difficultyTarget = difficultyTarget;
		c.nonce = nonce;
		if ( transactions != null )
		{
			c.transactions = new ArrayList<Transaction> (transactions.size ());
			for ( Transaction t : transactions )
			{
				c.transactions.add (t.clone ());
			}
		}
		return c;
	}

	/**
	 * Get block hash. Return the hash if known (already computed)
	 * 
	 * @return block hash
	 */
	public String getHash ()
	{
		return hash;
	}

	/**
	 * Block version
	 * 
	 * @return version
	 */
	public long getVersion ()
	{
		return version;
	}

	/**
	 * set block version
	 * 
	 * @param version
	 */
	public void setVersion (long version)
	{
		this.version = version;
	}

	/**
	 * previous block's hash
	 * 
	 * @return hash
	 */
	public String getPreviousHash ()
	{
		return previousHash;
	}

	/**
	 * set previous block's hash
	 * 
	 * @param previousHash
	 */
	public void setPreviousHash (String previousHash)
	{
		this.previousHash = previousHash;
	}

	/**
	 * Compute and set block hash using current content.
	 * 
	 * @param merkle
	 *            - true if merkle root of transactions should also be recomputed. if false header will be hashed assuming merkle root is already correct.
	 */
	public void computeHash (boolean merkle)
	{
		if ( merkle )
		{
			merkleRoot = computeMerkleRoot ();
		}

		WireFormat.Writer writer = new WireFormat.Writer ();
		toWireHeaderOnly (writer);
		WireFormat.Reader reader = new WireFormat.Reader (writer.toByteArray ());

		hash = reader.hash ().toString ();
		if ( transactions != null )
		{
			for ( Transaction t : transactions )
			{
				t.setBlockHash (hash);
			}
		}
	}

	/**
	 * Compute and return merkle root of transactions. This does not change the stored merkle root, just calculates. Comparing the result with the expected
	 * value gives a proof of the block content.
	 * 
	 * @return computed merkle root
	 */
	public String computeMerkleRoot ()
	{
		if ( transactions != null )
		{
			ArrayList<byte[]> tree = new ArrayList<byte[]> ();
			for ( Transaction t : transactions )
			{
				t.computeHash ();
				tree.add (new Hash (t.getHash ()).toByteArray ());
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
			return new Hash (aggregator.aggregate (tree)).toString ();
		}
		return null;
	}

	/**
	 * Get merkle root as currently stored in the block. Does not recompute it.
	 * 
	 * @return
	 */
	public String getMerkleRoot ()
	{
		return merkleRoot;
	}

	/**
	 * Set the hash of the block to a value.
	 * 
	 * @param hash
	 */
	public void setHash (String hash)
	{
		this.hash = hash;
	}

	/**
	 * Set the merkle root to a value
	 * 
	 * @param merkleRoot
	 */
	public void setMerkleRoot (String merkleRoot)
	{
		this.merkleRoot = merkleRoot;
	}

	/**
	 * Get time point miner assigned to the block. Note that result is seconds in the Unix epoch.
	 * 
	 * @return
	 */
	public long getCreateTime ()
	{
		return createTime;
	}

	/**
	 * set block time point. Value is seconds in the Unix epoch.
	 * 
	 * @param createTime
	 */
	public void setCreateTime (long createTime)
	{
		this.createTime = createTime;
	}

	/**
	 * get compact representation of work target.
	 * 
	 * @return
	 */
	public long getDifficultyTarget ()
	{
		return difficultyTarget;
	}

	/**
	 * set work target
	 * 
	 * @param difficultyTarget
	 */
	public void setDifficultyTarget (long difficultyTarget)
	{
		this.difficultyTarget = difficultyTarget;
	}

	/**
	 * get the nonce that miner alter to meet work target
	 * 
	 * @return
	 */
	public long getNonce ()
	{
		return nonce;
	}

	/**
	 * get nonce
	 * 
	 * @param nonce
	 */
	public void setNonce (long nonce)
	{
		this.nonce = nonce;
	}

	/**
	 * get the list of transactions as stored in the block
	 * 
	 * @return transactions list
	 */
	public List<Transaction> getTransactions ()
	{
		return transactions;
	}

	/**
	 * set transaction list
	 * 
	 * @param transactions
	 */
	public void setTransactions (List<Transaction> transactions)
	{
		this.transactions = transactions;
	}

	/**
	 * get block height. Note that this is not part of the protocol header, but will be filled by the server.
	 * 
	 * @return
	 */
	public int getHeight ()
	{
		return height;
	}

	/**
	 * Set block height. Note that this is not part of the protocol header.
	 * 
	 * @param height
	 */
	public void setHeight (int height)
	{
		this.height = height;
	}

	/**
	 * Write block header to writer. Useful to calculate header digest (the block hash)
	 * 
	 * @param writer
	 *            a wire format writer
	 */
	public void toWireHeaderOnly (WireFormat.Writer writer)
	{
		writer.writeUint32 (version);
		writer.writeHash (new Hash (previousHash));
		writer.writeHash (new Hash (merkleRoot));
		writer.writeUint32 (createTime);
		writer.writeUint32 (difficultyTarget);
		writer.writeUint32 (nonce);
	}

	/**
	 * Write the block in wire format to writer
	 * 
	 * @param writer
	 *            a wire format writer
	 */
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

	/**
	 * recreate a block from wire format input through a reader
	 * 
	 * @param reader
	 *            wire format reader
	 * @return block recreated as object
	 */
	public static Block fromWire (WireFormat.Reader reader)
	{
		Block b = new Block ();

		int cursor = reader.getCursor ();
		b.version = reader.readUint32 ();

		b.previousHash = reader.readHash ().toString ();
		b.merkleRoot = reader.readHash ().toString ();
		b.createTime = reader.readUint32 ();
		b.difficultyTarget = reader.readUint32 ();
		b.nonce = reader.readUint32 ();
		b.hash = reader.hash (cursor, 80).toString ();
		long nt = reader.readVarInt ();
		if ( nt > 0 )
		{
			b.transactions = new ArrayList<Transaction> ();
			for ( long i = 0; i < nt; ++i )
			{
				b.transactions.add (Transaction.fromWire (reader));
			}
		}
		return b;
	}

	/**
	 * Recreate a block from wire format input as a hexadecimal string
	 * 
	 * @param dump
	 *            - hex dump of wire
	 * @return block recreated as object
	 */
	public static Block fromWireDump (String dump)
	{
		return fromWire (new WireFormat.Reader (ByteUtils.fromHex (dump)));
	}

	/**
	 * Hex dump of this block on wire
	 * 
	 * @return - hex dump of wire
	 */
	public String toWireDump ()
	{
		WireFormat.Writer writer = new WireFormat.Writer ();
		toWire (writer);
		return ByteUtils.toHex (writer.toByteArray ());
	}

	/**
	 * Convert to a protobuf message used to communicate with the server
	 * 
	 * @return protobuf representation of the block
	 */
	public BCSAPIMessage.Block toProtobuf ()
	{
		BCSAPIMessage.Block.Builder builder = BCSAPIMessage.Block.newBuilder ();
		builder.setVersion ((int) version);
		builder.setDifficulty ((int) difficultyTarget);
		builder.setNonce ((int) nonce);
		builder.setTimestamp ((int) createTime);
		builder.setMerkleRoot (ByteString.copyFrom (new Hash (merkleRoot).toByteArray ()));
		builder.setPreviousBlock (ByteString.copyFrom (new Hash (previousHash).toByteArray ()));
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

	/**
	 * Recreate the block object from a protobuf message
	 * 
	 * @param pb
	 *            prtobuf message
	 * @return block object
	 */
	public static Block fromProtobuf (BCSAPIMessage.Block pb)
	{
		Block block = new Block ();
		block.setVersion (pb.getVersion ());
		block.setDifficultyTarget (pb.getDifficulty ());
		block.setNonce (pb.getNonce ());
		block.setCreateTime (pb.getTimestamp ());
		block.setPreviousHash (new Hash (pb.getPreviousBlock ().toByteArray ()).toString ());
		block.setMerkleRoot (new Hash (pb.getMerkleRoot ().toByteArray ()).toString ());
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
