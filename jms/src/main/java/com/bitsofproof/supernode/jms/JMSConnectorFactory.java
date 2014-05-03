package com.bitsofproof.supernode.jms;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.bitsofproof.supernode.connector.Connector;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorFactory;

public class JMSConnectorFactory implements ConnectorFactory
{
	private String username;
	private String password;
	private String brokerUrl;
	private String clientId;

	public JMSConnectorFactory (String username, String password, String brokerUrl, String clientId)
	{
		this.username = username;
		this.password = password;
		this.brokerUrl = brokerUrl;
		this.clientId = clientId;
	}

	@Override
	public Connector getConnector () throws ConnectorException
	{
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory (username, password, brokerUrl);
		connectionFactory.setClientID (clientId);
		try
		{
			return new JMSConnector (connectionFactory);
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

	public String getClientId ()
	{
		return clientId;
	}

	public void setClientId (String clientId)
	{
		this.clientId = clientId;
	}
}
