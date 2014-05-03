package com.bitsofproof.supernode.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import com.bitsofproof.supernode.connector.Connector;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorSession;

public class JMSConnector implements Connector
{
	private Connection connection;

	public JMSConnector (ConnectionFactory connectionFactory) throws JMSException
	{
		connection = connectionFactory.createConnection ();
	}

	@Override
	public ConnectorSession createSession () throws ConnectorException
	{
		try
		{
			return new JMSConnectorSession (connection.createSession (false, Session.AUTO_ACKNOWLEDGE));
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public void start () throws ConnectorException
	{
		try
		{
			connection.start ();
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
			connection.close ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public void setClientID (String string) throws ConnectorException
	{
		try
		{
			connection.setClientID (string);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
