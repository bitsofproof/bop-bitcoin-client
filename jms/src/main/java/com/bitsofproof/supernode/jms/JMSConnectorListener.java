package com.bitsofproof.supernode.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bitsofproof.supernode.connector.ConnectorListener;

public class JMSConnectorListener implements MessageListener
{
	private static final Logger log = LoggerFactory.getLogger (JMSConnectorListener.class);

	private final ConnectorListener listener;
	private final Session session;

	public JMSConnectorListener (Session session, ConnectorListener listener)
	{
		this.listener = listener;
		this.session = session;
	}

	@Override
	public void onMessage (Message message)
	{
		try
		{
			JMSConnectorMessage m = new JMSConnectorMessage (session, message);
			listener.onMessage (m);
		}
		catch ( JMSException e )
		{
			log.error ("Unable to parse message", e);
		}
	}
}
