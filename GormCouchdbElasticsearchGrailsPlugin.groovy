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

import grails.util.GrailsUtil
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.elasticsearch.search.internal.InternalSearchHit
import org.grails.plugins.couchdb.elasticsearch.CouchChangesIndexer
import org.grails.plugins.couchdb.elasticsearch.ElasticSearchContextHolder
import org.grails.plugins.couchdb.elasticsearch.ElasticSearchGClientFactoryBean
import org.grails.plugins.couchdb.elasticsearch.ElasticSearchService
import org.grails.plugins.couchdb.elasticsearch.mapping.CouchMappingConfigurator
import org.springframework.context.ApplicationContext
import org.grails.plugins.couchdb.elasticsearch.mapping.CouchSearchableClassMapping
import grails.converters.JSON

/**
 *
 * @author Cory Hacking
 */
class GormCouchdbElasticsearchGrailsPlugin {

	static final Log log = LogFactory.getLog(GormCouchdbElasticsearchGrailsPlugin.class.name)

	// the plugin version
	def version = "0.1"

	// the version or versions of Grails the plugin is designed for
	def grailsVersion = "1.3.6 > *"

	// the other plugins this plugin depends on
	def dependsOn = [
			'gormCouchdb': "0.9.0 > *"
	]

	// resources that are excluded from plugin packaging
	def pluginExcludes = [
			"grails-app/controllers/*",
			"grails-app/views/*",
			"grails-app/domain/*",
	]

	def loadAfter = ['services', 'controllers', 'gormCouchdb']

	def author = "Cory Hacking"
	def authorEmail = ""
	def title = "Grails CouchDB ElasticSearch Plugin"
	def description = "Integrates CouchDB with ElasticSearch, indexing domain instances using the Couchdb _change feed."

	// URL to the plugin's documentation
	def documentation = "http://grails.org/plugin/gorm-couchdb-elasticsearch"

	def doWithWebDescriptor = { xml ->

	}

	def doWithSpring = {
		def esConfig = getConfiguration(parentCtx, application)

		elasticSearchContextHolder(ElasticSearchContextHolder) {
			config = esConfig
		}

		elasticSearchClient(ElasticSearchGClientFactoryBean) {
			elasticSearchContextHolder = ref("elasticSearchContextHolder")
		}

		elasticSearchService(ElasticSearchService) {
			grailsApplication = ref("grailsApplication")

			elasticSearchClient = ref("elasticSearchClient")
			elasticSearchContextHolder = ref("elasticSearchContextHolder")
		}

		elasticSearchCouchdbMappingConfigurator(CouchMappingConfigurator) { bean ->
			grailsApplication = ref("grailsApplication")

			elasticSearchClient = ref("elasticSearchClient")
			elasticSearchContextHolder = ref("elasticSearchContextHolder")

			bean.initMethod = 'configureAndCreateMappings'
		}

		elasticSearchCouchdbChangesIndexer(CouchChangesIndexer) {
			grailsApplication = ref("grailsApplication")

			elasticSearchClient = ref("elasticSearchClient")
			elasticSearchContextHolder = ref("elasticSearchContextHolder")
		}

	}

	def doWithDynamicMethods = { applicationContext ->
		def contextHolder = applicationContext.getBean("elasticSearchContextHolder")

		InternalSearchHit.metaClass.getDocument = { ->

			// get the class mapping
			CouchSearchableClassMapping scm = contextHolder.getMappingContext(delegate.index, delegate.type)
			if (!scm) {

				// can't convert the search hit without a mapping
				return null
			}

			scm.domainClass.clazz.parse(delegate.sourceAsMap() as JSON)
		}
	}

	def doWithApplicationContext = { applicationContext ->

		// start our indexer
		applicationContext.getBean("elasticSearchCouchdbChangesIndexer").start()

	}

	def onChange = { event ->
		// TODO Implement code that is executed when any artefact that this plugin is
		// watching is modified and reloaded. The event contains: event.source,
		// event.application, event.manager, event.ctx, and event.plugin.
	}

	def onConfigChange = { event ->
		// TODO Implement code that is executed when the project configuration changes.
		// The event is the same as for 'onChange'.
	}

	def onShutdown = { event ->

		// stop the indexer
		event.ctx.getBean("elasticSearchCouchdbChangesIndexer").stop()

		// close the ElasticSearch client
		event.ctx.getBean("elasticSearchClient").close()

	}

	private getConfiguration(ApplicationContext applicationContext, GrailsApplication application) {
		def config = application.config

		// try to load it from class file and merge into GrailsApplication#config
		// Config.groovy properties override the default one
		try {
			Class dataSourceClass = application.getClassLoader().loadClass("DefaultCouchdbElasticSearch")
			ConfigSlurper configSlurper = new ConfigSlurper(GrailsUtil.getEnvironment())

			Map binding = new HashMap()
			binding.userHome = System.properties['user.home']
			binding.grailsEnv = application.metadata["grails.env"]
			binding.appName = application.metadata["app.name"]
			binding.appVersion = application.metadata["app.version"]
			configSlurper.binding = binding

			def defaultConfig = configSlurper.parse(dataSourceClass)
			config = defaultConfig.merge(config)

			return config.elasticSearch

		} catch (ClassNotFoundException e) {
			log.error("Can't load default Couchdb ElasticSearch configuration.", e)

			throw e
		}
	}
}
