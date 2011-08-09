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

    /**
     * The configuration of the ElasticSearch plugin
     */
    ConfigObject config

    /**
     * A map containing the mapping to ElasticSearch
     */
	private Map<String, CouchSearchableClassMapping> mapping = [:].asSynchronized()

	private Set<String> indicesSet = new HashSet().asSynchronized()
	private String[] indices = null

    /**
     * Adds a mapping context to the current mapping holder
     *
     * @param scm The SearchableClassMapping instance to add
     */
    public void addMappingContext(CouchSearchableClassMapping scm) {
		if (!indicesSet.contains(scm.indexName)) {
			indicesSet.add(scm.indexName)
			indices = indicesSet as String[]
		}

		mapping[scm.indexName + '.' + scm.elasticTypeName] = scm
    }

    /**
     * Returns the mapping context for a peculiar type
	 * @param index
     * @param type
     * @return
     */
    CouchSearchableClassMapping getMappingContext(String index, String type) {
		mapping[index + '.' + type]
	}

    /**
     * Returns the mapping context for a peculiar GrailsDomainClass
	 * @param index
     * @param domainClass
     * @return
     */
    CouchSearchableClassMapping getMappingContext(String index, CouchDomainClass domainClass) {
		mapping[index + '.' + domainClass.documentType]
	}

    /**
     * Returns the mapping context for a peculiar Class
     *
     * @param clazz
     * @return
     */
    CouchSearchableClassMapping getMappingContextByType(Class clazz) {
        mapping.values().find { scm -> scm.domainClass.clazz == clazz }
    }

    /**
     * Determines if a Class is root-mapped by the ElasticSearch plugin
     *
     * @param clazz
     * @return A boolean determining if the class is root-mapped or not
     */
    def isRootClass(Class clazz) {
        mapping.values().any { scm -> scm.domainClass.clazz == clazz && scm.root }
    }

    /**
     * Returns the Class that is associated to a specific elasticSearch type
     *
     * @param elasticTypeName
     * @return A Class instance or NULL if the class was not found
     */
	Class findMappedClassByElasticType(String index, String elasticTypeName) {
		mapping.values().find { scm -> scm.indexName == index && scm.elasticTypeName == elasticTypeName }?.domainClass?.clazz
	}

	String[] getIndices() {
		indices
	}
}