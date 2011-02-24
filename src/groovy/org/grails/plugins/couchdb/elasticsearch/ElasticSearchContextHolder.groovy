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

import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClass
import org.grails.plugins.couchdb.elasticsearch.mapping.CouchSearchableClassMapping

class ElasticSearchContextHolder {

	ConfigObject config

	private Map<String, CouchSearchableClassMapping> mapping = [:].asSynchronized()

	private Set<String> indicesSet = new HashSet().asSynchronized()
	private String[] indices = null

	public void addMappingContext(CouchSearchableClassMapping scm) {
		if (!indicesSet.contains(scm.indexName)) {
			indicesSet.add(scm.indexName)
			indices = indicesSet as String[]
		}

		mapping[scm.indexName + '.' + scm.elasticTypeName] = scm
	}

	CouchSearchableClassMapping getMappingContext(String index, String type) {
		mapping[index + '.' + type]
	}

	CouchSearchableClassMapping getMappingContext(String index, CouchDomainClass domainClass) {
		mapping[index + '.' + domainClass.documentType]
	}

	CouchSearchableClassMapping getMappingContextByType(Class clazz) {
		mapping.values().find { scm -> scm.domainClass.clazz == clazz }
	}

	def isRootClass(Class clazz) {
		mapping.values().any { scm -> scm.domainClass.clazz == clazz && scm.root }
	}

	Class findMappedClassByElasticType(String index, String elasticTypeName) {
		mapping.values().find { scm -> scm.indexName == index && scm.elasticTypeName == elasticTypeName }?.domainClass?.clazz
	}

	String[] getIndices() {
		indices
	}
}
