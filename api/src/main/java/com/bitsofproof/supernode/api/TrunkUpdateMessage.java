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
import java.util.ArrayList;
import java.util.List;

import com.bitsofproof.supernode.common.Hash;
import com.google.protobuf.ByteString;

public class TrunkUpdateMessage implements Serializable
{
	private static final long serialVersionUID = 3218705325909725834L;
	private final List<String> added;
	private final List<String> removed;

	public List<String> getAdded ()
	{
		return added;
	}

	public List<String> getRemoved ()
	{
		return removed;
	}

	public TrunkUpdateMessage (List<String> added, List<String> removed)
	{
		super ();
		this.added = added;
		this.removed = removed;
	}

	public BCSAPIMessage.TrunkUpdate toProtobuf ()
	{
		BCSAPIMessage.TrunkUpdate.Builder builder = BCSAPIMessage.TrunkUpdate.newBuilder ();
		if ( added != null )
		{
			for ( String a : added )
			{
				builder.addAdded (ByteString.copyFrom (new Hash (a).toByteArray ()));
			}
		}
		if ( removed != null )
		{
			for ( String r : removed )
			{
				builder.addRemoved (ByteString.copyFrom (new Hash (r).toByteArray ()));
			}
		}

		return builder.build ();
	}

	public static TrunkUpdateMessage fromProtobuf (BCSAPIMessage.TrunkUpdate pu)
	{
		List<String> added = new ArrayList<String> ();
		List<String> removed = new ArrayList<String> ();
		if ( pu.getAddedCount () > 0 )
		{
			added = new ArrayList<String> ();
			for ( ByteString b : pu.getAddedList () )
			{
				added.add (new Hash (b.toByteArray ()).toString ());
			}
		}
		if ( pu.getRemovedCount () > 0 )
		{
			removed = new ArrayList<String> ();
			for ( ByteString b : pu.getRemovedList () )
			{
				removed.add (new Hash (b.toByteArray ()).toString ());
			}
		}
		return new TrunkUpdateMessage (added, removed);
	}
}
