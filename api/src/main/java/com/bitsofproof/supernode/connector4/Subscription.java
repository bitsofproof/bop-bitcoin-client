package com.bitsofproof.supernode.connector4;

public interface Subscription
{
	void onMessage(byte[] bytes);
}
