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

import java.lang.reflect.Method
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.svenson.JSONProperty
import org.codehaus.groovy.grails.commons.GrailsClassUtils

/**
 * Custom searchable property mapping.
 */
public class CouchSearchablePropertyMapping {

	public static final Set<String> SEARCHABLE_MAPPING_OPTIONS = new HashSet<String>(Arrays.asList("boost", "index", "type", "term_vector", "analyzer"))
	public static final Set<String> SEARCHABLE_SPECIAL_MAPPING_OPTIONS = new HashSet<String>(Arrays.asList("excludeFromAll"))

	/**
	 * Grails attributes of this property
	 */
	GrailsDomainClassProperty grailsProperty
	String jsonPropertyName

	// Mapping attributes values, will be added in the ElasticSearch JSON mapping request
	private Map searchableAttributes = [:]

	// Special mapping attributes, only used by the plugin itself (eg: 'component', 'reference')
	private specialAttributes = [:]

	private CouchSearchableClassMapping componentPropertyMapping

	public CouchSearchablePropertyMapping(GrailsDomainClassProperty property) {
		this(property, null)
	}

	public CouchSearchablePropertyMapping(GrailsDomainClassProperty property, Map options) {
		this.grailsProperty = property
		this.jsonPropertyName = property.name

		try {
			Method getter = property.domainClass.clazz.getMethod("get" + jsonPropertyName.capitalize())
			JSONProperty annotation = getter.getAnnotation(JSONProperty)

			if (annotation && annotation.value()) {
				jsonPropertyName = annotation.value()
			}
		} catch (Exception e) {

		}

		if (options) {
			addAttributes(options)
		}
	}

	public void addAttributes(Map options) {
		options.each {k, v ->
			if (SEARCHABLE_MAPPING_OPTIONS.contains(k)) {
				searchableAttributes.put(k, v)

			} else if (SEARCHABLE_SPECIAL_MAPPING_OPTIONS.contains(k)) {
				specialAttributes.put(k, v)

			} else {
				throw new IllegalArgumentException("Invalid option ${k} found in searchable mapping.")
			}
		}
	}

	/**
	 * See http://www.elasticsearch.com/docs/elasticsearch/mapping/all_field/
	 *
	 * @return exclude this property from ALL aggregate field?
	 */
	public boolean shouldExcludeFromAll() {
		Object excludeFromAll = specialAttributes.get("excludeFromAll")
		if (excludeFromAll == null) {

			// exclude id, version, and type by default
			def name = grailsProperty.name
			def dc = grailsProperty.domainClass

			return (grailsProperty.isIdentity()
					|| name == dc.version.name
					|| name == dc.typeFieldName
					|| !isIndexed()
					|| GrailsClassUtils.isJdk5Enum(grailsProperty.referencedPropertyType)
			)

		} else if (excludeFromAll instanceof Boolean) {
			return (Boolean) excludeFromAll

		} else {

			// introduce behaviour compatible with Searchable Plugin.
			return excludeFromAll.toString().equalsIgnoreCase("yes")

		}
	}

	public GrailsDomainClassProperty getGrailsProperty() {
		return grailsProperty
	}

	private Class<?> getPropertyType() {
		return grailsProperty.type
	}

	public String getPropertyName() {
		return grailsProperty.name
	}

	public String getJsonPropertyName() {
		return jsonPropertyName
	}

	public Map getAttributes() {
		return Collections.unmodifiableMap(searchableAttributes)
	}

	public CouchSearchableClassMapping getComponentPropertyMapping() {
		return componentPropertyMapping
	}

	void setComponentPropertyMapping(CouchSearchableClassMapping componentPropertyMapping) {
		this.componentPropertyMapping = componentPropertyMapping
	}

	/**
	 * @return true if field is analyzed. NOTE it doesn't have to be stored.
	 */
	public boolean isAnalyzed() {

		def index = searchableAttributes["index"] ?: ''
		if (index == "analyzed") {
			return true
		}

		if (index) {
			return false
		}

		// don't analyze special grails-couchdb fields
		def name = grailsProperty.name
		def dc = grailsProperty.domainClass

		return !(grailsProperty.isIdentity() || name == dc.version.name || name == dc.typeFieldName || GrailsClassUtils.isJdk5Enum(grailsProperty.referencedPropertyType))
	}

	public boolean isIndexed() {
		!(searchableAttributes["index"] ?: '').toString().equalsIgnoreCase('no')
	}

	/**
	 * @return searchable property mapping information.
	 */
	public String toString() {
		return "CouchSearchablePropertyMapping {" + "propertyName='" + getPropertyName() + '\'' + ", propertyType=" + getPropertyType() + ", attributes=" + attributes + ", specialAttributes=" + specialAttributes + '}'
	}
}
