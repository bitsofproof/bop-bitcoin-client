package com.bitsofproof.supernode.connector;

public interface ConnectorFactory
{
	public Connector getConnector () throws ConnectorException;
}
