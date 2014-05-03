package com.bitsofproof.supernode.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorTemporaryQueue;

public class JMSConnectorTemporaryQueue implements ConnectorTemporaryQueue, JMSDestination
{
	private TemporaryQueue queue;

	public JMSConnectorTemporaryQueue (Session session) throws JMSException
	{
		queue = session.createTemporaryQueue ();
	}

	@Override
	public String getName () throws ConnectorException
	{
		try
		{
			return queue.getQueueName ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public void delete () throws ConnectorException
	{
		try
		{
			queue.delete ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public Destination getDestination ()
	{
		return queue;
	}
}
