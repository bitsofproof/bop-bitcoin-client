package com.bitsofproof.supernode.connector4.jms;

import com.bitsofproof.supernode.connector4.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class JMSConnector implements Connector4
{
	private static final Logger log = LoggerFactory.getLogger (JMSConnector.class);

	private final ResponseWorker responseWorker;

	private final ConsumerProducerCache consumerProducerCache;

	private final Map<String, ? extends Destination> destinations;

	private final Connection connection;

	private final ExecutorService requestHandlerExecutor;

	private final ExecutorService requestSenderExecutor;

	private final TemporaryQueue responseQueue;

	public JMSConnector (ConnectionFactory connectionFactory,
	                     Map<String, ? extends Destination> destinations,
	                     ExecutorService requestHandlerExecutor,
	                     ExecutorService requestSenderExecutor) throws Connector4Exception
	{
		try
		{
			this.connection = connectionFactory.createConnection ();
			Session session = connection.createSession (false, Session.AUTO_ACKNOWLEDGE);

			this.consumerProducerCache = new ConsumerProducerCache (session, destinations);
			this.responseQueue = session.createTemporaryQueue ();
			this.responseWorker = new ResponseWorker (connection, destinations);


			this.requestHandlerExecutor = requestHandlerExecutor;
			this.requestSenderExecutor = requestSenderExecutor;
			this.destinations = destinations;
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	// public API methods

	@Override
	public void publish (String topic, byte[] bytes) throws Connector4Exception
	{
		Destination destination = destinations.get (topic);
		sendMessage (new OutputMessage (bytes, null, destination));
	}

	@Override
	public void setSubscription (String topic, final Subscription l) throws Connector4Exception
	{
		try
		{
			MessageConsumer consumer = consumerProducerCache.getConsumer (topic);
			consumer.setMessageListener (new RequestHandlerMessageListener ()
			{
				@Override
				public void handleRequest (byte[] bytes, Destination replyTo, String correlationId)
				{
					l.onMessage (bytes);
				}
			});
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	@Override
	public void setRequestHandler (String queue, final RequestHandler h) throws Connector4Exception
	{
		try
		{
			MessageConsumer consumer = consumerProducerCache.getConsumer (queue);
			consumer.setMessageListener (new RequestHandlerMessageListener ()
			{
				@Override
				public void handleRequest (byte[] bytes, Destination replyTo, String correlationId)
				{
					requestHandlerExecutor.execute (new RequestHandlerTask (JMSConnector.this, h, bytes, replyTo, correlationId));
				}
			});
		}
		catch (JMSException e)
		{
			log.error ("Error listening to " + queue, e);
			throw new Connector4Exception (e);
		}
	}

	@Override
	public void setRequestHandler (String queue, final MultipartResponseRequestHandler h) throws Connector4Exception
	{
		try
		{
			MessageConsumer consumer = consumerProducerCache.getConsumer (queue);
			consumer.setMessageListener (new RequestHandlerMessageListener ()
			{
				@Override
				public void handleRequest (byte[] bytes, Destination replyTo, String correlationId)
				{
					requestHandlerExecutor.execute (new MultipartRequestWorker (JMSConnector.this, h, bytes, replyTo, correlationId));
				}
			});
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	@Override
	public byte[] request (String queue, byte[] request) throws Connector4Exception
	{
		return request (queue, request, 0, TimeUnit.MILLISECONDS);
	}

	@Override
	public byte[] request (String queue, byte[] request, long timeout, TimeUnit unit) throws Connector4Exception
	{
		Destination destination = destinations.get (queue);
		if ( destination == null )
		{
			throw new Connector4Exception ("Unknown destination");
		}

		return executeRequest (new SimpleRequestSenderTask (connection,
		                                                    responseQueue,
		                                                    new OutputMessage (request, UUID.randomUUID ().toString (), destination),
		                                                    timeout, unit));
	}

	@Override
	public void multipartRequest (String queue, byte[] request, MultipartResponseHandler responseHandler) throws Connector4Exception
	{
		multipartRequest (queue, request, 0, TimeUnit.MILLISECONDS, responseHandler);
	}

	@Override
	public void multipartRequest (String queue, byte[] request, long timeout, TimeUnit unit, MultipartResponseHandler responseHandler) throws
	                                                                                                                                   Connector4Exception
	{
		Destination destination = destinations.get (queue);
		if ( destination == null )
		{
			throw new Connector4Exception ("Unknown destination");
		}

		executeRequest (new MultipartResponseRequestSenderTask (connection,
		                                                        responseQueue,
		                                                        new OutputMessage (request, UUID.randomUUID ().toString (), destination),
		                                                        responseHandler,
		                                                        timeout, unit));
	}

	@Override
	public void start () throws Connector4Exception
	{
		try
		{
			responseWorker.start ();
			connection.start ();
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	@Override
	public void close () throws Connector4Exception
	{
		try
		{
			responseWorker.shutdown ();
			shutdownExecutor (requestHandlerExecutor);
			shutdownExecutor (requestSenderExecutor);
			connection.close ();
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	@Override
	public void stop () throws Connector4Exception
	{
		try
		{
			connection.stop ();
		}
		catch (JMSException e)
		{
			throw new Connector4Exception (e);
		}
	}

	// generic utility methods and classes

	private void shutdownExecutor (ExecutorService executor)
	{
		executor.shutdown ();
		try
		{
			executor.awaitTermination (1, TimeUnit.SECONDS);
		}
		catch (InterruptedException ignored)
		{
		}
		if ( !executor.isTerminated () )
		{
			executor.shutdownNow ();
		}
	}

	/**
	 * Sends a response message asynchonrously, where the replyTo shouldn't be set.
	 */
	private void sendMessage (OutputMessage r)
	{
		responseWorker.send (r);
	}

	/**
	 * Caching producers and consumers for long-lived JMS sessions.
	 */
	public static class ConsumerProducerCache
	{
		private final Session session;

		private final ConcurrentHashMap<Destination, MessageProducer> producers;

		private final ConcurrentHashMap<Destination, MessageConsumer> consumers;

		private final Map<String, ? extends Destination> destinations;


		public ConsumerProducerCache (Session session, Map<String, ? extends Destination> destinations)
		{
			this.session = session;
			this.destinations = destinations;
			this.producers = new ConcurrentHashMap<> ();
			this.consumers = new ConcurrentHashMap<> ();
		}

		public MessageConsumer getConsumer (String name) throws JMSException, Connector4Exception
		{
			Destination destination = destinations.get (name);
			if ( destination == null )
			{
				throw new Connector4Exception ("Unknown destination " + name);
			}

			return getConsumer (destination);
		}

		public MessageConsumer getConsumer (Destination destination) throws JMSException
		{
			MessageConsumer consumer = consumers.get (destination);
			if ( consumer == null )
			{

				MessageConsumer newConsumer = session.createConsumer (destination);
				consumer = consumers.putIfAbsent (destination, newConsumer);
				if ( consumer == null )
				{
					consumer = newConsumer;
				}
				else
				{
					newConsumer.close ();
				}
			}

			return consumer;
		}

		public MessageProducer getProducer (String name) throws JMSException, Connector4Exception
		{
			Destination destination = destinations.get (name);
			if ( destination == null )
			{
				throw new Connector4Exception ("Unknown destination " + name);
			}

			return getProducer (destination);
		}

		public MessageProducer getProducer (Destination destination) throws JMSException
		{
			MessageProducer producer = producers.get (destination);
			if ( producer == null )
			{
				MessageProducer newProducer = session.createProducer (destination);
				producer = producers.putIfAbsent (destination, newProducer);
				if ( producer == null )
				{
					producer = newProducer;
				}
				else
				{
					newProducer.close ();
				}
			}

			return producer;
		}
	}

	/**
	 * Value object representing a message sent to a gived destination with an optional correlationId.
	 */
	private static class OutputMessage
	{
		final String correlationId;

		final Destination destination;

		final byte[] bytes;

		private OutputMessage (byte[] bytes, String correlationId, Destination destination)
		{
			this.bytes = bytes;
			this.destination = destination;
			this.correlationId = correlationId;
		}

		private BytesMessage createJMSMessage (Session session) throws JMSException
		{
			BytesMessage message = session.createBytesMessage ();
			if ( correlationId != null )
			{
				message.setJMSCorrelationID (correlationId);
			}

			if ( bytes != null )
			{
				message.writeBytes (bytes);
			}

			return message;
		}

	}


	private static class ResponseWorker extends Thread
	{
		private static final Logger log = LoggerFactory.getLogger (ResponseWorker.class);

		private final LinkedBlockingQueue<OutputMessage> outputMessageQueue = new LinkedBlockingQueue<> ();

		private final Session session;

		private final ConsumerProducerCache producerCache;

		private ResponseWorker (Connection connection, Map<String, ? extends Destination> destinations) throws JMSException
		{
			super ("connector-response-worker");
			this.session = connection.createSession (false, Session.AUTO_ACKNOWLEDGE);
			this.producerCache = new ConsumerProducerCache (session, destinations);
			setDaemon (true);
		}

		public void shutdown ()
		{
			this.interrupt ();

			try
			{
				this.join (TimeUnit.SECONDS.toMillis (1));
			}
			catch (InterruptedException ignored)
			{
			}
		}

		public void send (OutputMessage outputMessage)
		{
			try
			{
				outputMessageQueue.put (outputMessage);
			}
			catch (InterruptedException ignored)
			{
			}
		}

		@Override
		public void run ()
		{
			try
			{
				while (!Thread.currentThread ().isInterrupted ())
				{
					try
					{
						OutputMessage outputMessage = outputMessageQueue.take ();

						BytesMessage message = outputMessage.createJMSMessage (session);

						MessageProducer producer = producerCache.getProducer (outputMessage.destination);
						producer.send (message);
					}
					catch (InterruptedException ignored)
					{
					}
					catch (JMSException e)
					{
						log.error ("Error sending response", e);
					}
				}
			}
			finally
			{
				log.info ("Shutting down response worker");

				try
				{
					session.close ();
				}
				catch (JMSException ignored)
				{
				}
			}
		}
	}

	private static class RequestHandlerTask implements Runnable
	{

		private final JMSConnector connector;

		private final RequestHandler handler;

		private final byte[] request;

		private final Destination replyTo;

		private final String correlationId;

		private RequestHandlerTask (JMSConnector connector, RequestHandler handler, byte[] request, Destination replyTo, String correlationId)
		{
			this.connector = connector;
			this.handler = handler;
			this.request = request;
			this.replyTo = replyTo;
			this.correlationId = correlationId;
		}

		@Override
		public void run ()
		{
			byte[] responseBytes = handler.onRequest (request);

			connector.sendMessage (new OutputMessage (responseBytes, correlationId, replyTo));
		}
	}

	private static class MultipartRequestWorker implements Runnable
	{
		private final JMSConnector connector;

		private final MultipartResponseRequestHandler handler;

		private final byte[] request;

		private final Destination replyTo;

		private final String correlationId;

		private MultipartRequestWorker (JMSConnector connector, MultipartResponseRequestHandler handler, byte[] request, Destination replyTo,
		                                String correlationId)
		{
			this.connector = connector;
			this.handler = handler;
			this.request = request;
			this.replyTo = replyTo;
			this.correlationId = correlationId;
		}

		@Override
		public void run ()
		{
			handler.onRequest (request, new MultipartResponse ()
			{
				@Override
				public void send (byte[] bytes)
				{
					connector.sendMessage (new OutputMessage (bytes, correlationId, replyTo));
				}

				@Override
				public void eof ()
				{
					connector.sendMessage (new OutputMessage (null, correlationId, replyTo));
				}
			});
		}
	}

	public static abstract class RequestHandlerMessageListener implements MessageListener
	{
		@Override
		public void onMessage (Message message)
		{
			try
			{
				if ( message instanceof BytesMessage )
				{
					BytesMessage bm = (BytesMessage) message;
					long len = bm.getBodyLength ();
					if ( len < Integer.MAX_VALUE )
					{
						byte[] bytes = new byte[(int) len];
						bm.readBytes (bytes);
						Destination replyTo = bm.getJMSReplyTo ();
						String correlationId = bm.getJMSCorrelationID ();

						handleRequest (bytes, replyTo, correlationId);
					}
					else
					{
						log.error ("Message size too big: ", len);
					}
				}
			}
			catch (JMSException e)
			{
				log.error ("Error handling request", e);
			}
		}

		public abstract void handleRequest (byte[] bytes, Destination replyTo, String correlationId);

	}

	private static abstract class RequestSenderTask<T> implements Callable<T>
	{
		private final OutputMessage request;

		private final Connection connection;

		private final Destination responseTo;

		protected final long timeout;

		protected RequestSenderTask (Connection connection, Destination responseTo, OutputMessage request, long timeout, TimeUnit unit)
		{
			this.request = request;
			this.connection = connection;
			this.responseTo = responseTo;
			this.timeout = unit.toMillis (timeout);
		}

		@Override
		public T call () throws Exception
		{
			Session session = connection.createSession (false, Session.AUTO_ACKNOWLEDGE);

			BytesMessage message = request.createJMSMessage (session);
			message.setJMSReplyTo (responseTo);
			MessageConsumer responseConsumer = session.createConsumer (responseTo, "JMSCorrelationID = '" + message.getJMSCorrelationID () + "'");

			MessageProducer producer = session.createProducer (request.destination);
			producer.send (message);
			producer.close ();

			T result = handleResponse (responseConsumer);
			responseConsumer.close ();
			return result;
		}

		protected abstract T handleResponse (MessageConsumer responseConsumer) throws JMSException, Connector4Exception;

		protected byte[] toBytes (Message message) throws Connector4Exception, JMSException
		{
			if ( message == null )
			{
				throw new ConnectorTimeoutException ("Response timed out");
			}
			if ( !(message instanceof BytesMessage) )
			{
				throw new Connector4Exception ("Unable to handle message " + message);
			}

			BytesMessage bm = (BytesMessage) message;

			long len = bm.getBodyLength ();
			if ( len == 0 )
			{
				return null;
			}
			else if ( len < Integer.MAX_VALUE )
			{
				byte[] bytes = new byte[(int) len];
				bm.readBytes (bytes);

				return bytes;
			}
			else
			{
				throw new Connector4Exception ("Message size too big: " + len);
			}
		}
	}

	private static class SimpleRequestSenderTask extends RequestSenderTask<byte[]>
	{

		protected SimpleRequestSenderTask (Connection connection, Destination responseTo, OutputMessage request, long timeout, TimeUnit unit)
		{
			super (connection, responseTo, request, timeout, unit);
		}

		@Override
		protected byte[] handleResponse (MessageConsumer responseConsumer) throws JMSException, Connector4Exception
		{
			Message response = responseConsumer.receive (timeout);

			return toBytes (response);
		}
	}

	private static class MultipartResponseRequestSenderTask extends RequestSenderTask<Void> implements MessageListener
	{

		private final CountDownLatch latch;

		private final MultipartResponseHandler responseHandler;

		private MessageConsumer responseConsumer;

		MultipartResponseRequestSenderTask (Connection connection,
		                                    Destination responseTo,
		                                    OutputMessage request,
		                                    MultipartResponseHandler responseHandler,
		                                    long timeout, TimeUnit unit)
		{
			super (connection, responseTo, request, timeout, unit);
			this.responseHandler = responseHandler;
			this.latch = new CountDownLatch (1);
		}

		@Override
		protected Void handleResponse (MessageConsumer responseConsumer) throws JMSException, Connector4Exception
		{
			try
			{
				this.responseConsumer = responseConsumer;
				responseConsumer.setMessageListener (this);

				latch.await (5, TimeUnit.SECONDS);
			}
			catch (InterruptedException ignored)
			{
			}

			return null;
		}

		@Override
		public void onMessage (Message message)
		{
			try
			{
				if ( message instanceof BytesMessage )
				{
					BytesMessage bm = (BytesMessage) message;
					if ( bm.getBodyLength () == 0 )
					{
						responseHandler.eof ();
					}
					else
					{
						responseHandler.part (toBytes (bm));
					}
				}
				else
				{
					log.error ("Invalid message");
				}

			}
			catch (Connector4Exception e)
			{
				log.error("Invalid multipart message", e);
			}
			catch (JMSException e)
			{
				log.error("JMS Error while receiving multipart message", e);
			}
		}
	}

	private <T> T executeRequest (Callable<T> task) throws Connector4Exception
	{
		try
		{
			return requestSenderExecutor.submit (task).get ();
		}
		catch (InterruptedException e)
		{
			throw new Connector4Exception (e);
		}
		catch (ExecutionException e)
		{
			Throwable cause = e.getCause ();
			if ( cause instanceof Connector4Exception )
			{
				throw (Connector4Exception) cause;
			}

			throw new Connector4Exception (cause);
		}

	}

}
