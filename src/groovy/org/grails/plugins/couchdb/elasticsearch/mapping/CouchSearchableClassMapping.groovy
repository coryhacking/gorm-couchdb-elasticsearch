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

import org.codehaus.groovy.grails.commons.GrailsApplication
import org.codehaus.groovy.grails.commons.GrailsClassUtils
import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClass
import org.springframework.util.ClassUtils

public class CouchSearchableClassMapping {

	private static final Set IGNORED_PROPERTIES = new HashSet(["_id", "_rev"])
	private static final Set SUPPORTED_FORMATS = new HashSet(["string", "integer", "long", "float", "double", "boolean", "null", "date"])
	private static final Class JODA_TIME

	static {
		try {
			JODA_TIME = Class.forName("org.joda.time.ReadableInstant")
		} catch (ClassNotFoundException e) {
		}
	}

	private ConfigObject esConfig

	/**
	 * All searchable properties
	 */
	private Collection<CouchSearchablePropertyMapping> propertiesMapping

	/**
	 * Owning domain class
	 */
	private CouchDomainClass domainClass

	private boolean all = true
	private String indexName
	private String elasticTypeName

	public CouchSearchableClassMapping(GrailsApplication grailsApplication, ConfigObject esConfig, CouchDomainClass domainClass, Collection<CouchSearchablePropertyMapping> propertiesMapping) {
		this.esConfig = esConfig
		this.domainClass = domainClass
		this.propertiesMapping = propertiesMapping

		def ds = grailsApplication.config.couchdb
		def dbId = domainClass.databaseId

		String database = ds?.database ?: (dbId ?: grailsApplication.metadata["app.name"])
		if (dbId && ds[dbId]) {
			database = ds.database ?: database
		}

		this.indexName = database
		this.elasticTypeName = domainClass.documentType
	}

	public Boolean isRoot() {
		return domainClass.isRoot()
	}

	public boolean isAll() {
		return all
	}

	public Collection<CouchSearchablePropertyMapping> getPropertiesMapping() {
		return propertiesMapping
	}

	public CouchDomainClass getDomainClass() {
		return domainClass
	}

	/**
	 * @return ElasticSearch index name
	 */
	public String getIndexName() {
		return indexName
	}

	/**
	 * @return type name for ES mapping.
	 */
	public String getElasticTypeName() {
		return elasticTypeName
	}

	public Map buildElasticMapping() {

		def elasticTypeMappingProperties = [:]

		if (!isAll()) {
			elasticTypeMappingProperties["_all"] = [enabled: false]
		}

		// Map each domain properties in supported format, or object for complex type
		propertiesMapping.each { CouchSearchablePropertyMapping scpm ->

			// some properties are just ignored...
			if (IGNORED_PROPERTIES.contains(scpm.jsonPropertyName)) {
				return
			}

			def propOptions = [:]

			// Add the custom mapping (searchable static property in domain model)
			propOptions.putAll(scpm.attributes)

			// Does it have custom mapping?
			String propType = propOptions["type"] ?: scpm.grailsProperty.typePropertyName

			if (!(SUPPORTED_FORMATS.contains(propType))) {

				// Handle embedded persistent collections, ie List<String> listOfThings
				if (scpm.grailsProperty.isBasicCollectionType()) {

					String basicType = ClassUtils.getShortName(scpm.grailsProperty.referencedPropertyType).toLowerCase()
					if (SUPPORTED_FORMATS.contains(basicType)) {
						propType = basicType
					} else {
						propType = "object"
					}

				} else if (scpm.grailsProperty.referencedPropertyType.isArray()) {

					// Handle arrays
					String basicType = ClassUtils.getShortName(scpm.grailsProperty.referencedPropertyType.componentType).toLowerCase()
					if (SUPPORTED_FORMATS.contains(basicType)) {
						propType = basicType
					} else {
						propType = "object"
					}

				} else if (isDateType(scpm.grailsProperty.referencedPropertyType)) {
					propType = "date"

				} else if (GrailsClassUtils.isJdk5Enum(scpm.grailsProperty.referencedPropertyType)) {
					propType = "string"

				} else {
					propType = "object"

				}
			}

			propOptions["type"] = propType

			if (propType == "date" && !propOptions.containsKey("format")) {
				propOptions["format"] = esConfig.date.formats.join('||')
			}

			// See http://www.elasticsearch.com/docs/elasticsearch/mapping/all_field/
			if (isAll() && scpm.isIndexed()) {
				propOptions["include_in_all"] = !scpm.shouldExcludeFromAll()
			}

			// todo only enable this through configuration...
			if (propType == "string" && scpm.isAnalyzed() && !propOptions.containsKey("term_vector")) {
				propOptions["term_vector"] = "with_positions_offsets"
			}

			elasticTypeMappingProperties[scpm.jsonPropertyName] = propOptions
		}

		def mapping = [:]
		def type = elasticTypeName

		def i = type.indexOf('.')
		if (i >= 0) {
			type = type.substring(0, i)
		}

		mapping[type] = ["properties": elasticTypeMappingProperties]

		return mapping
	}

	private boolean isDateType(Class type) {
		return java.util.Date.class.isAssignableFrom(type) || (JODA_TIME != null && JODA_TIME.isAssignableFrom(type))
	}
}
