package com.bitsofproof.supernode.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import com.bitsofproof.supernode.connector.ConnectorDestination;
import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorMessage;
import com.bitsofproof.supernode.connector.ConnectorProducer;

public class JMSConnectorMessage implements ConnectorMessage
{
	private Session session;
	private BytesMessage message;

	public JMSConnectorMessage (Session session) throws JMSException
	{
		message = session.createBytesMessage ();
		this.session = session;
	}

	public JMSConnectorMessage (Session session, Message message) throws JMSException
	{
		this.message = (BytesMessage) message;
		this.session = session;
	}

	public BytesMessage getMessage ()
	{
		return message;
	}

	@Override
	public void setPayload (byte[] payload) throws ConnectorException
	{
		try
		{
			message.writeBytes (payload);
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public byte[] getPayload () throws ConnectorException
	{
		byte[] body = null;
		try
		{
			if ( message.getBodyLength () > 0 )
			{
				body = new byte[(int) message.getBodyLength ()];
				message.readBytes (body);
				message.reset ();
			}
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
		return body;
	}

	@Override
	public ConnectorProducer getReplyProducer () throws ConnectorException
	{
		try
		{
			return new JMSConnectorProducer (session.createProducer (message.getJMSReplyTo ()));
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}

	@Override
	public void setReplyTo (ConnectorDestination replyTo) throws ConnectorException
	{
		try
		{
			message.setJMSReplyTo (((JMSDestination) replyTo).getDestination ());
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
