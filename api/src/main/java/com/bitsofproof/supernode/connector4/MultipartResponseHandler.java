package com.bitsofproof.supernode.connector4;

public interface MultipartResponseHandler
{
	void part (byte[] bytes);

	void eof ();
}
