/*
 * Copyright 2014 bits of proof zrt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bitsofproof.supernode.connector;

public interface ConnectorSession
{
	public ConnectorMessage createMessage () throws ConnectorException;

	public ConnectorProducer createProducer (ConnectorDestination destination) throws ConnectorException;

	public ConnectorTemporaryQueue createTemporaryQueue () throws ConnectorException;

	public ConnectorQueue createQueue (String name) throws ConnectorException;

	public ConnectorTopic createTopic (String name) throws ConnectorException;

	public ConnectorConsumer createConsumer (ConnectorDestination destination) throws ConnectorException;

	public void close () throws ConnectorException;
}
