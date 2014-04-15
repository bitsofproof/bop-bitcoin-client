package com.bitsofproof.supernode.api;

public interface RejectListener
{
	public void rejected (String command, String hash, String reason, int rejectionCode);
}
