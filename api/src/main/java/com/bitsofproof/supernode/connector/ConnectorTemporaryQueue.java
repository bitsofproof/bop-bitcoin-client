package com.bitsofproof.supernode.connector;

public interface ConnectorTemporaryQueue extends ConnectorQueue
{
	public void delete () throws ConnectorException;
}
