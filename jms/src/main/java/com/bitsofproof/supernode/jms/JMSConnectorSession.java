package com.bitsofproof.supernode.jms;

import javax.jms.JMSException;
import javax.jms.Session;

import com.bitsofproof.supernode.connector.ConnectorConsumer;
import com.bitsofproof.supernode.connector.ConnectorDestination;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorMessage;
import com.bitsofproof.supernode.connector.ConnectorProducer;
import com.bitsofproof.supernode.connector.ConnectorQueue;
import com.bitsofproof.supernode.connector.ConnectorSession;
import com.bitsofproof.supernode.connector.ConnectorTemporaryQueue;
import com.bitsofproof.supernode.connector.ConnectorTopic;

public class JMSConnectorSession implements ConnectorSession
{
	private Session session;

	public JMSConnectorSession (Session session)
	{
		this.session = session;
	}

	public Session getJMSSession ()
	{
		return session;
	}

	@Override
	public ConnectorMessage createMessage () throws ConnectorException
	{
		try
		{
			return new JMSConnectorMessage (session);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorProducer createProducer (ConnectorDestination destination) throws ConnectorException
	{
		try
		{
			if ( destination instanceof JMSConnectorQueue )
			{
				return new JMSConnectorProducer (session.createProducer (((JMSConnectorQueue) destination).getQueue ()));
			}
			if ( destination instanceof JMSConnectorTopic )
			{
				return new JMSConnectorProducer (session.createProducer (((JMSConnectorTopic) destination).getTopic ()));
			}
			return null;
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorTemporaryQueue createTemporaryQueue () throws ConnectorException
	{
		try
		{
			return new JMSConnectorTemporaryQueue (session);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorQueue createQueue (String name) throws ConnectorException
	{
		try
		{
			return new JMSConnectorQueue (session, name);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorTopic createTopic (String name) throws ConnectorException
	{
		try
		{
			return new JMSConnectorTopic (session, name);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public ConnectorConsumer createConsumer (ConnectorDestination destination) throws ConnectorException
	{
		try
		{
			if ( destination instanceof JMSConnectorQueue )
			{
				return new JMSConnectorConsumer (session, session.createConsumer (((JMSConnectorQueue) destination).getDestination ()));
			}
			if ( destination instanceof JMSConnectorTopic )
			{
				return new JMSConnectorConsumer (session, session.createConsumer (((JMSConnectorTopic) destination).getDestination ()));
			}
			return null;
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
			session.close ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
