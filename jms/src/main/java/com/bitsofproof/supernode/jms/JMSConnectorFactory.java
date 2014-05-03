package com.bitsofproof.supernode.jms;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import com.bitsofproof.supernode.connector.Connector;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorFactory;

public class JMSConnectorFactory implements ConnectorFactory
{
	private String username;
	private String password;
	private String brokerUrl;

	@Override
	public Connector getConnector () throws ConnectorException
	{
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory (username, password, brokerUrl);
		final ConnectionFactory pooledConnectionFactory = new PooledConnectionFactory (connectionFactory);

		try
		{
			return new JMSConnector (pooledConnectionFactory);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	public String getUsername ()
	{
		return username;
	}

	public void setUsername (String username)
	{
		this.username = username;
	}

	public String getPassword ()
	{
		return password;
	}

	public void setPassword (String password)
	{
		this.password = password;
	}

	public String getBrokerUrl ()
	{
		return brokerUrl;
	}

	public void setBrokerUrl (String brokerUrl)
	{
		this.brokerUrl = brokerUrl;
	}
}
