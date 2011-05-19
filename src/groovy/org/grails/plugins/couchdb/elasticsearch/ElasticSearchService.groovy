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

import grails.converters.JSON
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.groovy.client.GClient

public class ElasticSearchService {

	private static final Log log = LogFactory.getLog(CouchChangesIndexer)

	GrailsApplication grailsApplication

	private GClient client
	private ElasticSearchContextHolder contextHolder

	public setElasticSearchContextHolder(ElasticSearchContextHolder contextHolder) {
		this.contextHolder = contextHolder
	}

	public ElasticSearchContextHolder getElasticSearchContextHolder() {
		contextHolder
	}

	public setElasticSearchClient(GClient client) {
		this.client = client
	}

	public GClient getElasticSearchClient() {
		client
	}

	def search(SearchRequest request) {

		// set the indices if they weren't already set
		if (request.indices() == null) {
			request.indices(contextHolder.indices)
		}

		client.search(request)
	}

	def search(Closure c) {
		SearchRequest request = searchRequest()

		c.resolveStrategy = Closure.DELEGATE_FIRST
		c.setDelegate request
		c.call()

		client.search(request)
	}

	def search(Map map) {
		client.search(searchRequest(map))
	}

	def search(String s) {
		client.search(searchRequest(s))
	}

	def search(JSON json) {
		client.search(searchRequest(json))
	}

	private SearchRequest searchRequest(Object source = null) {
		SearchRequest request = new SearchRequest().indices(contextHolder.indices)

		if (source instanceof Map || source instanceof String) {
			request.source(source)

		} else if (source instanceof JSON) {
			request.source(source as String)

		}

		return request
	}
}
