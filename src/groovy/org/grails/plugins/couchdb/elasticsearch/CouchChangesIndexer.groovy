/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.grails.plugins.couchdb.elasticsearch

import grails.converters.JSON
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClass
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.action.bulk.BulkRequestBuilder
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.index.mapper.MapperException
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.elasticsearch.indices.IndexMissingException
import static org.elasticsearch.client.Requests.*

/**
 * Process that reads the CouchDB _changes feed and indexes the changes.
 *
 * @author Cory Hacking
 */
class CouchChangesIndexer {

	private static final Log log = LogFactory.getLog(CouchChangesIndexer)

	private final DaemonThreadFactory daemonThreadFactory = new DaemonThreadFactory()
	private volatile boolean closed = false

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

	public ElasticSearchContextHolder getElasticSearchContextHolder() {
		contextHolder
	}

	void start() {

		// get the list of domain objects that are "searchable"
		def searchableCouchDomainClasses = grailsApplication.CouchDomainClasses.findAll { CouchDomainClass dc ->

			if (!contextHolder.getMappingContextByType(dc.clazz)) {
				return false
			}

			return true
		}

		if (searchableCouchDomainClasses.size() == 0) {
			log.info("No 'searchable' Couch Domain classes were found.")

			return
		}

		// create a changes slurper for each referenced db
		searchableCouchDomainClasses.collect { it.databaseId }.unique().each { dbId ->

			// get the couchdb domain configuration
			def ds = grailsApplication.config.couchdb

			String host = ds?.host ?: "localhost"
			Integer port = (ds?.port ?: 5984) as Integer
			String database = ds?.database ?: (dbId ?: grailsApplication.metadata["app.name"])
			String username = ds?.username ?: ""
			String password = ds?.password ?: ""

			String realm = ds?.realm ?: null
			String scheme = ds?.scheme ?: null

			// get the datasource configuration for this specific db (if any)
			if (dbId && ds[dbId]) {
				ds = ds[dbId]

				host = ds.host ?: host
				port = ds.port ?: port
				database = ds.database ?: database
				username = ds.username ?: username
				password = ds.password ?: password
				realm = ds.realm ?: realm
				scheme = ds.scheme ?: scheme
			}

			log.info("Opening couchdb _changes stream for [${database}]...")

			// get the list of types for this db
			def types = searchableCouchDomainClasses.findAll { it.databaseId == dbId }.collect {

				def type = contextHolder.getMappingContextByType(it.clazz).elasticTypeName
				def i = type.indexOf('.')

				return (i < 0) ? type : type.substring(0, i)

			}.unique()

			// create a queue that will be used to transfer data from the slurper to the indexer
			LinkedBlockingQueue queue = new LinkedBlockingQueue()

			// create the slurper thread
			def slurperThread = daemonThreadFactory.newThread(new Slurper(queue, host, port, database, null, null))
			slurperThread.start()

			// create the indexer thread
			def indexerThread = daemonThreadFactory.newThread(new Indexer(queue, database, types))
			indexerThread.start()
		}
	}

	void stop() {

		if (!daemonThreadFactory.createdThreads.size()) {

			// nothing to shut down...just return
			return
		}

		log.info("Closing couchdb _changes streams...")

		if (!closed) {
			closed = true

			daemonThreadFactory.interruptAll()
		}
	}

	private class Slurper implements Runnable {

		private final String db
		private final String baseUri

		private final LinkedBlockingQueue transferQueue

		private final String index

		Slurper(LinkedBlockingQueue queue, String host, int port, String db, String filter, Map filterParams) {

			def uri = "http://${host}:${port}/${db}/_changes?feed=continuous&heartbeat=15000&include_docs=true"

			// add any defined couch filters
			if (filter != null) {
				uri = uri + "filter=" + URLEncoder.encode(filter)

				if (filterParams != null) {
					uri = uri + filterParams
				}
			}

			this.db = db
			this.baseUri = uri

			this.transferQueue = queue

			this.index = db
		}

		public void run() {

			try {
				createIndex db
				createCouchSequenceNoMapping db
			} catch (Exception e) {
				log.error("Error creating index [${db}].", e)

				sleep 5000
			}

			while (!closed) {

				// get the last sequence number
				String lastSeq = null
				try {
					lastSeq = getLastSeq(db)

				} catch (Exception e) {
					if (e instanceof IndexMissingException || e.cause instanceof IndexMissingException) {
						log.debug("Failed to get _seq for [${db}] because the [${db}] index is missing.", e)

						// create the index
						createIndex db

					} else if (e instanceof MapperException || e.cause instanceof MapperException) {
						log.debug("Failed to get _seq for [${db}] because the [${db}] mapping is missing.", e)

						// create the sequence mapping
						createCouchSequenceNoMapping db

					} else {
						log.warn("Failed to get _seq for [${db}].", e)

						// sleep for a bit
						sleep 30000
					}

					continue
				}

				HttpClient httpclient = new DefaultHttpClient()
				String uri = baseUri + (lastSeq ? "&since=" + lastSeq : '')

				try {

					HttpGet httpget = new HttpGet(uri)

					if (lastSeq) {
						log.info("Requesting [${db}] _changes since [${lastSeq}].")
					} else {
						log.info("Requesting all [${db}] _changes.")
					}

					if (log.isDebugEnabled()) {
						log.debug("Executing [${db}] _changes request [${httpget.getURI()}].")
					}

					// Execute request
					HttpResponse response = httpclient.execute(httpget)

					if (log.isTraceEnabled()) {
						log.trace("[${db}] _changes response was [${response.statusLine}].")
					}

					// Get the response entity
					HttpEntity entity = response.entity

					// If the response does not enclose an entity, there is no need
					// to bother about connection release
					if (entity != null) {

						final InputStream is = entity.content
						final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))

						try {

							String line
							while ((line = reader.readLine()) != null) {
								if (closed) {
									return
								}

								if (line.length() == 0) {
									if (log.isTraceEnabled()) {
										log.trace("[${db}] _changes heartbeat received.")
									}
									continue
								}

								if (log.isTraceEnabled()) {
									log.trace("[${db}] _changes [${line}].")
								}

								// put this change on the queue for processing by the Indexer
								transferQueue.add(line)
							}

						} catch (IOException e) {
							throw e

						} catch (RuntimeException e) {
							throw e

						} finally {

							// abort the HTTP request in order to shut down the underlying
							// connection immediately.
							httpget.abort()

							// Closing the input stream will trigger connection release
							try { reader.close() } catch (Exception ignore) {}
							try { is.close() } catch (Exception ignore) {}
						}
					}

					if (!closed) {

						// sleep for just a bit
						sleep 5000
					}

				} catch (Exception e) {
					log.error("Error reading ${db} _changes feed [${uri}].", e)

					// sleep for a little longer because of the error...
					sleep 30000

				} finally {

					// close all of our expired connections
					httpclient.connectionManager.closeExpiredConnections()
				}
			}
		}
	}

	private class Indexer implements Runnable {

		private final bulkTimeout = 500
		private final bulkSize = 99

		private final String db
		private final LinkedBlockingQueue transferQueue

		private final String index

		private final List types

		Indexer(LinkedBlockingQueue queue, String db, List types) {

			this.db = db
			this.transferQueue = queue

			this.index = db

			this.types = types
		}

		public void run() {
			while (true) {
				if (closed) {
					return
				}

				String s
				try {
					s = transferQueue.take()
				} catch (InterruptedException e) {
					if (closed) {
						return
					}
					continue
				}
				BulkRequestBuilder bulk = client.client.prepareBulk()

				String lastSeq = null
				String lineSeq = processChange(index, s, bulk)
				if (lineSeq != null) {
					lastSeq = lineSeq
				}

				// spin a bit to see if we can get some more changes
				try {
					while ((s = transferQueue.poll(bulkTimeout, TimeUnit.MILLISECONDS)) != null) {
						lineSeq = processChange(index, s, bulk)
						if (lineSeq != null) {
							lastSeq = lineSeq
						}

						if (bulk.numberOfActions() >= bulkSize) {
							break
						}
					}
				} catch (InterruptedException e) {
					if (closed) {
						return
					}
				}

				if (lastSeq != null) {
					try {
						if (log.isTraceEnabled()) {
							log.trace("[${db}] _changes setting _seq to [${lastSeq}].")
						}

						bulk.add(indexRequest(db).type("couchdb").id("_seq").source(["lastSequenceNo": lastSeq]))
					} catch (IOException e) {
						log.warn("[${db}] _changes failed to add [lastSequenceNo] indexRequest.")
					}
				}

				try {
					BulkResponse response = bulk.execute().actionGet()
					if (response.hasFailures()) {

						// TODO write to exception queue?
						log.warn("[${db}] _changes failed to execute bulk request [${response.buildFailureMessage()}].")
					} else {

						if (bulk.numberOfActions() > 5) {
							log.info("[${db}] _changes executed ${bulk.numberOfActions()} bulk index actions in ${response.tookInMillis()}ms.")
						}
					}
				} catch (Exception e) {
					log.warn("[${db}] _changes failed to execute bulk request.", e)
				}
			}
		}

		private String processChange(String index, String s, BulkRequestBuilder bulk) {

			def seq = null

			// do something useful with the response
			try {
				def json = JSON.parse(s)

				seq = json.seq

				String id = json.id
				String type = json.doc?.type ?: ''

				// ignore documents that begin with an '_' or are flagged to be ignored
				if (id.startsWith("_") || json.ignore) {
					if (log.isTraceEnabled()) {
						log.trace("[${db}] _changes ignoring [${id}].")
					}

				} else if (json.deleted) {

					// since the type doesn't come through in the changes feed, create a
					// delete request for each of our document types
					types.each {String it ->

						bulk.add(deleteRequest(db).type(it).id(id))

					}

					if (log.isDebugEnabled()) {
						log.debug("[${db}] _changes #${seq}: deleted [${id}].")
					}

				} else if (json.doc && type) {

					// get the base document type
					def i = type.indexOf('.')
					def docType = (i < 0) ? type : type.substring(0, i)

					// check to make sure this document should be indexed
					if (contextHolder.getMappingContext(index, docType)) {

						if (log.isDebugEnabled()) {
							log.debug("[${db}] _changes #${seq}: changed [${type}: ${id}].")
						}

						// get just the document
						def doc = json.doc

						def source = new JSON(doc).toString()

						// add this index request to the bulk requests
						bulk.add(indexRequest(index).type(docType).id(id).source(source))

					} else {
						if (log.isTraceEnabled()) {
							log.trace("[${db}] _changes #${seq}: ignoring [${type}: ${id}].")
						}
					}
				} else {
					log.warn("[${db}] _changes #${seq}: ignoring unknown change [${s}].")

				}
			} catch (Exception e) {
				log.error("Error processing [${db}] _changes JSON [${s}].", e)
			}

			return seq
		}
	}

	private void createIndex(String index) {

		def settings = [:]

		try {
			client.admin.indices.prepareCreate(index).setSettings(settings).execute().actionGet()
		} catch (Exception e) {
			if (e instanceof IndexAlreadyExistsException || e.cause instanceof IndexAlreadyExistsException) {
				// ignore
			} else {
				throw e
			}
		}
	}

	private void createCouchSequenceNoMapping(String index) {

		// create the sequence mapping
		def mapping = [
			"couchdb": [
				"properties": [
					"lastSequenceNo": [
						"type": "string",
						"store": "yes",
						"index": "not_analyzed",
						"include_in_all": "false"
					]
				]
			]
		]

		log.debug("Setting mapping for [${index}, couchdb] to ${mapping.toString()}].")
		client.admin.indices.putMapping(new PutMappingRequest(index).type("couchdb").ignoreConflicts(true).source(mapping)).actionGet()

	}

	private String getLastSeq(String index) {
		String lastSeq = null

		client.admin.indices.prepareRefresh(index).execute().actionGet()
		GetResponse lastSeqResponse = client.prepareGet(index, "couchdb", "_seq").execute().actionGet()
		if (lastSeqResponse.exists()) {
			Map<String, Object> couchdbState = (Map<String, Object>) lastSeqResponse.sourceAsMap()
			if (couchdbState != null) {
				lastSeq = couchdbState.get("lastSequenceNo").toString()
			}
		}

		return lastSeq
	}

	private void sleep(long milliseconds) {

		// sleep for the specified milliseconds, ignoring any interruption
		try {

			Thread.sleep(milliseconds)

		} catch (InterruptedException ignore) {
			// ignore
		}
	}

	private class DaemonThreadFactory implements ThreadFactory {

		private final ThreadFactory factory = Executors.defaultThreadFactory()
		private final String namePrefix = "couch.indexer-"
		private final List createdThreads = Collections.synchronizedList(new ArrayList())

		public Thread newThread(Runnable r) {
			Thread thread = factory.newThread(r)

			// add the prefix to our thread name
			thread.setName((namePrefix + thread.name).replaceAll("-pool-", "-").replaceAll("-thread-", ".").replaceAll("-1.", "-"))

			// set the daemon flag
			thread.setDaemon(true)

			// add this thread to our thread list
			createdThreads << thread

			return thread
		}

		public void interruptAll() {
			createdThreads.each {Thread t ->
				t.interrupt()
			}
		}
	}
}
