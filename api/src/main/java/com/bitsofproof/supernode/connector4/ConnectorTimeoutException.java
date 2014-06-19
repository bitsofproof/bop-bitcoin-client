package com.bitsofproof.supernode.connector4;

public class ConnectorTimeoutException extends Connector4Exception
{
	public ConnectorTimeoutException (String message)
	{
		super (message);
	}

	public ConnectorTimeoutException (String message, Throwable cause)
	{
		super (message, cause);
	}

	public ConnectorTimeoutException (Throwable cause)
	{
		super (cause);
	}

	public ConnectorTimeoutException (String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super (message, cause, enableSuppression, writableStackTrace);
	}
}
