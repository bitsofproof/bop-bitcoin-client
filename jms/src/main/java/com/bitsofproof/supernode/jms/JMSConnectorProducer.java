package com.bitsofproof.supernode.jms;

import javax.jms.JMSException;
import javax.jms.MessageProducer;

import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorMessage;
import com.bitsofproof.supernode.connector.ConnectorProducer;

public class JMSConnectorProducer implements ConnectorProducer
{
	private MessageProducer producer;

	public JMSConnectorProducer (MessageProducer producer)
	{
		this.producer = producer;
	}

	@Override
	public void send (ConnectorMessage message) throws ConnectorException
	{
		try
		{
			producer.send (((JMSConnectorMessage) message).getMessage ());
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
			producer.close ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
