/*
 * Copyright 2002-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.grails.plugins.couchdb.elasticsearch

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings.Builder
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.springframework.beans.factory.FactoryBean
import static org.elasticsearch.node.NodeBuilder.*

class ElasticSearchClientFactoryBean implements FactoryBean {

	private static final SUPPORTED_MODES = ['local', 'transport', 'node']

	private ElasticSearchContextHolder contextHolder

	public setElasticSearchContextHolder(ElasticSearchContextHolder contextHolder) {
		this.contextHolder = contextHolder
	}

	public ElasticSearchContextHolder getElasticSearchContextHolder() {
		contextHolder
	}

	Object getObject() {

		// Retrieve client mode, default is "node"
		def clientMode = contextHolder.config.client.mode ?: 'node'
		if (!(clientMode in SUPPORTED_MODES)) {
			throw new IllegalArgumentException("Invalid client mode, expected values were ${SUPPORTED_MODES}.")
		}

		// get the name of the cluster
		def clusterName = contextHolder.config.cluster.name as String

		def nb = nodeBuilder()
		def transportClient = null

		switch (clientMode) {
			case 'transport':

				// used to store our settings
				Builder settings = ImmutableSettings.settingsBuilder();
				if (clusterName) {
					settings.put("cluster.name", clusterName)
				}

				transportClient = new TransportClient(settings.build())
				if (!contextHolder.config.client.hosts) {
					transportClient.addTransportAddress(new InetSocketTransportAddress('localhost', 9300))
				} else {
					contextHolder.config.client.hosts.each {
						transportClient.addTransportAddress(new InetSocketTransportAddress(it.host, it.port))
					}
				}

				return transportClient

			case 'local':
				nb.local(true)
				break

			case 'node':
				nb.client(true)
				break

		}

		// set the cluster name
		nb.clusterName(clusterName)

		// Avoiding this:
		// http://groups.google.com/a/elasticsearch.com/group/users/browse_thread/thread/2bb5d8dd6dd9b80b/e7db9e63fc305133?show_docid=e7db9e63fc305133&fwc=1
		def client = nb.node().client()

		return client
	}

	Class getObjectType() {
		Client
	}

	boolean isSingleton() {
		true
	}
}