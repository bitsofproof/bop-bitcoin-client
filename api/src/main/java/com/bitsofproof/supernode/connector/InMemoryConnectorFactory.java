package com.bitsofproof.supernode.connector;

public class InMemoryConnectorFactory implements ConnectorFactory
{
	private Connector connector;

	@Override
	public Connector getConnector ()
	{
		if ( connector == null )
		{
			connector = new InMemoryConnector ();
		}
		return connector;
	}
}
