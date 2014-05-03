package com.bitsofproof.supernode.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorQueue;

public class JMSConnectorQueue implements ConnectorQueue, JMSDestination
{
	private Queue queue;

	public JMSConnectorQueue (Session session, String name) throws JMSException
	{
		queue = session.createQueue (name);
	}

	public Queue getQueue ()
	{
		return queue;
	}

	@Override
	public Destination getDestination ()
	{
		return queue;
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
}
