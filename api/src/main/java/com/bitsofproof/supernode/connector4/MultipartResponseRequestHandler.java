package com.bitsofproof.supernode.connector4;

public interface MultipartResponseRequestHandler
{
	void onRequest (byte[] request, MultipartResponse response);
}
