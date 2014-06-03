package com.bitsofproof.supernode.connector4;

public interface MultipartResponse
{
	void send (byte[] bytes);

	void eof ();
}
