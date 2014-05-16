package com.bitsofproof.supernode.api;

import com.bitsofproof.supernode.common.Hash;

public interface RejectListener
{
	public void rejected (String command, Hash hash, String reason, int rejectionCode);
}
