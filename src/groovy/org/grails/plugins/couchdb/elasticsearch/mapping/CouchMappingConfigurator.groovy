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
package org.grails.plugins.couchdb.elasticsearch.mapping

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClass
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.grails.plugins.couchdb.elasticsearch.ElasticSearchContextHolder
import static org.elasticsearch.client.Requests.clusterHealthRequest

class CouchMappingConfigurator {

	private static final Log log = LogFactory.getLog(CouchMappingConfigurator.class)

	GrailsApplication grailsApplication

	private GClient client

	private ElasticSearchContextHolder contextHolder

	public setElasticSearchClient(GClient client) {
		this.client = client
	}

	public GClient getElasticSearchClient() {
		client
	}

	public setElasticSearchContextHolder(ElasticSearchContextHolder contextHolder) {
		this.contextHolder = contextHolder
	}

	ElasticSearchContextHolder getElasticSearchContextHolder() {
		contextHolder
	}

	/**
	 * Init method. Resolve the ElasticSearch mapping from the static "searchable" property
	 * (closure or boolean) in couchdb domain classes.
	 *
	 * @param mappings searchable class mappings to be install.
	 */
	public void configureAndCreateMappings() {

		def classMappings = []
		def esMappings = [:]

		def settings = [:]
		def installedIndices = [:] as HashSet

		// get the mappings
		grailsApplication.CouchDomainClasses.each { CouchDomainClass dc ->

			def closureMapper = new ClosureSearchableDomainClassMapper(grailsApplication, contextHolder.config, dc)
			CouchSearchableClassMapping searchableClassMapping = closureMapper.buildClassMapping()

			if (searchableClassMapping != null) {
				contextHolder.addMappingContext(searchableClassMapping)
				classMappings.add(searchableClassMapping)
			}
		}

		// sort them by type hierarchy
		classMappings.sort { CouchSearchableClassMapping scm -> scm.indexName + '.' + scm.elasticTypeName }

		// collect all of the mappings
		classMappings.each { CouchSearchableClassMapping scm ->

			Map elasticMapping = scm.buildElasticMapping()

			// make sure the appropriate root indexes are created
			if (scm.isRoot()) {
				if (!installedIndices.contains(scm.indexName)) {
					try {
						log.debug("Creating index [${scm.indexName}].")

						client.admin.indices.prepareCreate(scm.indexName).setSettings(settings).execute().actionGet()
						installedIndices.add(scm.indexName)

					} catch (Exception e) {
						if (e instanceof IndexAlreadyExistsException || e.cause instanceof IndexAlreadyExistsException) {
							installedIndices.add(scm.indexName)

							// If the index already exists, ignore the exception
							log.trace("Index [${scm.indexName}] already exists.")

						} else {
							log.error("Unexpected error creating index [${scm.indexName}].", e)

						}
					}
				}

				// save the mapping
				if (!esMappings[scm.indexName]) {
					esMappings[scm.indexName] = [:]
				}
				esMappings[scm.indexName][scm.elasticTypeName] = elasticMapping

			} else {

				// get the root document
				def type = scm.elasticTypeName

				def i = type.indexOf('.')
				def docType = (i < 0) ? type : type.substring(0, i)

				// merge the two properties maps together
				def mapping = esMappings[scm.indexName][docType]
				if (mapping) {
					mapping[docType].properties = mapping[docType].properties + scm.buildElasticMapping()[docType].properties
				}
			}
		}

		// create the mappings
		esMappings.each { String index, Map indexMappings ->

			indexMappings.each { String type, Map elasticMapping ->

				log.debug("Setting mapping for [${index}, ${type}] to ${elasticMapping.toString()}.")
				client.admin.indices.putMapping(new PutMappingRequest(index).ignoreConflicts(true).source(elasticMapping)).actionGet()

			}
		}

		log.info("Checking cluster status...")
		ClusterHealthResponse response = client.admin.cluster.health(clusterHealthRequest().waitForYellowStatus()).actionGet()
		log.info("The current status of cluster [${response.clusterName}] is [${response.status()}].")
	}
}