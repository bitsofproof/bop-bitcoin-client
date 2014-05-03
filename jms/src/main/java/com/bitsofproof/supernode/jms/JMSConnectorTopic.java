package com.bitsofproof.supernode.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import com.bitsofproof.supernode.connector.ConnectorException;
import com.bitsofproof.supernode.connector.ConnectorTopic;

public class JMSConnectorTopic implements ConnectorTopic, JMSDestination
{
	private Topic topic;

	public JMSConnectorTopic (Session session, String name) throws JMSException
	{
		topic = session.createTopic (name);
	}

	public Topic getTopic ()
	{
		return topic;
	}

	@Override
	public Destination getDestination ()
	{
		return topic;
	}

	@Override
	public String getName () throws ConnectorException
	{
		try
		{
			return topic.getTopicName ();
		}
		catch ( JMSException e )
		{
			throw new ConnectorException (e);
		}
	}
}
