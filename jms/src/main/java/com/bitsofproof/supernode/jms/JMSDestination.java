package com.bitsofproof.supernode.jms;

import javax.jms.Destination;

import com.bitsofproof.supernode.connector.ConnectorDestination;

public interface JMSDestination extends ConnectorDestination
{
	Destination getDestination ();
}
