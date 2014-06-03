package com.bitsofproof.supernode.connector4;

import java.util.concurrent.TimeUnit;

public interface Connector4 extends AutoCloseable
{
	void publish (String topic, byte[] msg) throws Connector4Exception;

	void setSubscription (String topic, Subscription l) throws Connector4Exception;

	void setRequestHandler (String queue, RequestHandler h) throws Connector4Exception;

	void setRequestHandler (String queue, MultipartResponseRequestHandler h) throws Connector4Exception;

	byte[] request(String queue, byte[] request) throws Connector4Exception;

	byte[][] multipartRequest(String queue, byte[] request) throws Connector4Exception;

	byte[] request(String queue, byte[] request, long timeout, TimeUnit unit) throws Connector4Exception;

	byte[][] multipartRequest(String queue, byte[] request, long timeout, TimeUnit unit) throws Connector4Exception;

	void close() throws Connector4Exception;

	void start() throws Connector4Exception;

	void stop() throws Connector4Exception;

}
