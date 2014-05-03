package com.bitsofproof.supernode.jms;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.bitsofproof.supernode.connector.ConnectorConsumer;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorListener;
import com.bitsofproof.supernode.connector.ConnectorMessage;

public class JMSConnectorConsumer implements ConnectorConsumer
{
	private final MessageConsumer consumer;
	private final Session session;

	public JMSConnectorConsumer (Session session, MessageConsumer consumer)
	{
		this.consumer = consumer;
		this.session = session;
	}

	@Override
	public void setMessageListener (ConnectorListener listener) throws ConnectorException
	{
		try
		{
			consumer.setMessageListener (new JMSConnectorListener (session, listener));
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorMessage receive () throws ConnectorException
	{
		try
		{
			return new JMSConnectorMessage (session, consumer.receive ());
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorMessage receiveNoWait () throws ConnectorException
	{
		try
		{
			return new JMSConnectorMessage (session, consumer.receiveNoWait ());
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorMessage receive (long timeout) throws ConnectorException
	{
		try
		{
			return new JMSConnectorMessage (session, consumer.receive (timeout));
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public void close () throws ConnectorException
	{
		try
		{
			consumer.close ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
