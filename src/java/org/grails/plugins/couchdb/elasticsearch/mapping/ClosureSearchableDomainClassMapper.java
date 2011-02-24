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
package org.grails.plugins.couchdb.elasticsearch.mapping;

import org.codehaus.groovy.grails.commons.GrailsApplication;
import org.codehaus.groovy.grails.commons.GrailsClassUtils;
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty;
import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClass;
import org.codehaus.groovy.grails.plugins.couchdb.domain.CouchDomainClassArtefactHandler;

import groovy.lang.Closure;
import groovy.lang.GroovyObjectSupport;
import groovy.util.ConfigObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ClosureSearchableDomainClassMapper extends GroovyObjectSupport {

	/**
	 * Options applied to searchable class itself
	 */
	public static final Set<String> CLASS_MAPPING_OPTIONS = new HashSet<String>(Arrays.asList("all", "root", "only", "except"));

	/**
	 * Searchable property name
	 */
	public static final String SEARCHABLE_PROPERTY_NAME = "searchable";

	private GrailsApplication grailsApplication;
	private ConfigObject esConfig;

	/**
	 * Class mapping properties
	 */
	private CouchDomainClass couchDomainClass;

	private Boolean all = true;
	private Object only;
	private Object except;

	private Set<String> mappableProperties = new HashSet<String>();
	private Map<String, CouchSearchablePropertyMapping> customMappedProperties = new HashMap<String, CouchSearchablePropertyMapping>();

	/**
	 * Create closure-based mapping configurator.
	 *
	 * @param grailsApplication grails app reference
	 * @param domainClass	   Grails domain class to be configured
	 * @param esConfig		  ElasticSearch configuration
	 */
	ClosureSearchableDomainClassMapper(GrailsApplication grailsApplication, ConfigObject esConfig, CouchDomainClass domainClass) {
		this.grailsApplication = grailsApplication;
		this.esConfig = esConfig;
		this.couchDomainClass = domainClass;
	}

	public void setAll(Boolean all) {
		this.all = all;
	}

	public void setOnly(Object only) {
		this.only = only;
	}

	public void setExcept(Object except) {
		this.except = except;
	}

	/**
	 * @return searchable domain class mapping
	 */
	public CouchSearchableClassMapping buildClassMapping() {

		if (!couchDomainClass.hasProperty(SEARCHABLE_PROPERTY_NAME)) {
			return null;
		}

		if (couchDomainClass.getPropertyValue(SEARCHABLE_PROPERTY_NAME).equals(Boolean.FALSE)) {
			return null;
		}

		if (couchDomainClass.getDocumentType() == null) {
			return null;
		}

		// Process inheritance.
		List<CouchDomainClass> superMappings = new ArrayList<CouchDomainClass>();
		Class<?> currentClass = couchDomainClass.getClazz();
		superMappings.add(couchDomainClass);
		while (currentClass != null) {
			currentClass = currentClass.getSuperclass();
			if (currentClass != null && CouchDomainClassArtefactHandler.isCouchDomainClass(currentClass)) {
				CouchDomainClass superDomainClass = (CouchDomainClass) grailsApplication.getArtefact(CouchDomainClassArtefactHandler.TYPE, currentClass.getName());
				if (superDomainClass.hasProperty(SEARCHABLE_PROPERTY_NAME) && superDomainClass.getPropertyValue(SEARCHABLE_PROPERTY_NAME).equals(Boolean.FALSE)) {

					// hierarchy explicitly terminated. Do not browse any more properties.
					break;
				}
				superMappings.add(superDomainClass);
				if (superDomainClass.isRoot()) {
					break;
				}
			}
		}

		Collections.reverse(superMappings);

		// hmm. should we only consider persistent properties?
		for (GrailsDomainClassProperty prop : couchDomainClass.getPersistentProperties()) {
			this.mappableProperties.add(prop.getName());
		}

		// !!!! Allow explicit identifier indexing ONLY when defined with custom attributes.
		mappableProperties.add(couchDomainClass.getIdentifier().getName());
		mappableProperties.add(couchDomainClass.getVersion().getName());

		// Process inherited mappings in reverse order.
		for (CouchDomainClass domainClass : superMappings) {
			if (domainClass.hasProperty(SEARCHABLE_PROPERTY_NAME)) {
				Object searchable = domainClass.getPropertyValue(SEARCHABLE_PROPERTY_NAME);
				if (searchable instanceof Boolean) {
					buildDefaultMapping(domainClass);

				} else if (searchable instanceof Closure) {

					// check which properties belong to this domain class ONLY
					Set<String> inheritedProperties = new HashSet<String>();
					for (GrailsDomainClassProperty prop : domainClass.getPersistentProperties()) {
						if (GrailsClassUtils.isPropertyInherited(domainClass.getClazz(), prop.getName())) {
							inheritedProperties.add(prop.getName());
						}
					}

					buildClosureMapping(domainClass, (Closure) searchable, inheritedProperties);
				} else {
					throw new IllegalArgumentException("'searchable' property has unknown type: " + searchable.getClass());
				}
			}
		}

		// Populate default settings.
		// Clean out any per-property specs not allowed by 'only','except' rules.
		customMappedProperties.keySet().retainAll(mappableProperties);

		for (String propertyName : mappableProperties) {
			CouchSearchablePropertyMapping scpm = customMappedProperties.get(propertyName);
			if (scpm == null) {
				if (propertyName.equals(couchDomainClass.getIdentifier().getName())) {
					scpm = new CouchSearchablePropertyMapping(couchDomainClass.getIdentifier());

				} else if (propertyName.equals(couchDomainClass.getVersion().getName())) {
					scpm = new CouchSearchablePropertyMapping(couchDomainClass.getVersion());

				} else {
					scpm = new CouchSearchablePropertyMapping(couchDomainClass.getPropertyByName(propertyName));
				}

				customMappedProperties.put(propertyName, scpm);
			}
		}

		return new CouchSearchableClassMapping(grailsApplication, esConfig, couchDomainClass, customMappedProperties.values());
	}

	public void buildDefaultMapping(CouchDomainClass couchDomainClass) {

		for (GrailsDomainClassProperty property : couchDomainClass.getPersistentProperties()) {

			@SuppressWarnings ({"unchecked"})
			List<String> defaultExcludedProperties = (List<String>) esConfig.get("defaultExcludedProperties");

			if ((defaultExcludedProperties == null || !defaultExcludedProperties.contains(property.getName())) && !couchDomainClass.getDocumentType().equals(property.getName())) {
				customMappedProperties.put(property.getName(), new CouchSearchablePropertyMapping(property));
			}
		}
	}

	public void buildClosureMapping(CouchDomainClass couchDomainClass, Closure searchable, Set<String> inheritedProperties) {
		assert searchable != null;

		// Build user-defined specific mappings
		Closure closure = (Closure) searchable.clone();
		closure.setDelegate(this);
		closure.call();

		Set<String> propsOnly = convertToSet(only);
		Set<String> propsExcept = convertToSet(except);

		if (!propsOnly.isEmpty() && !propsExcept.isEmpty()) {
			throw new IllegalArgumentException("Both 'only' and 'except' were used in '" + couchDomainClass.getPropertyName() + "#searchable': provide one or neither but not both.");
		}

		Boolean alwaysInheritProperties = (Boolean) esConfig.get("alwaysInheritProperties");
		boolean inherit = alwaysInheritProperties != null && alwaysInheritProperties;
		if (!propsExcept.isEmpty()) {
			mappableProperties.removeAll(propsExcept);
		}
		if (!propsOnly.isEmpty()) {
			if (inherit) {
				mappableProperties.retainAll(inheritedProperties);
			} else {
				mappableProperties.clear();
			}
			mappableProperties.addAll(propsOnly);
		}
	}

	/**
	 * Invoked by 'searchable' closure.
	 *
	 * @param name synthetic method name
	 * @param args method arguments.
	 *
	 * @return <code>null</code>
	 */
	public Object invokeMethod(String name, Object args) {
		// Predefined mapping options
//        if (CLASS_MAPPING_OPTIONS.contains(name)) {
//            if (args == null || ObjectUtils.isEmpty((Object[])args)) {
//                throw new IllegalArgumentException(grailsDomainClass.getPropertyName() + " mapping declares " + name + " : found no argument.");
//            }
//            Field target = ReflectionUtils.findField(this.getClass(), name);
//            ReflectionUtils.makeAccessible(target);
//            ReflectionUtils.setField(target, this, ((Object[])args)[0]);
//            return null;
//        }

		// Custom properties mapping options
		GrailsDomainClassProperty property = couchDomainClass.getPropertyByName(name);
		if (property == null) {
			throw new IllegalArgumentException("Unable to find property [" + name + "] used in [" + couchDomainClass.getPropertyName() + "}#searchable].");
		}
//        if (!mappableProperties.contains(name)) {
//            throw new IllegalArgumentException("Unable to map [" + couchDomainClass.getPropertyName() + "." +
//                    property.getName() + "]");
//        }

		// Check if we already has mapping for this property.
		CouchSearchablePropertyMapping propertyMapping = customMappedProperties.get(name);
		if (propertyMapping == null) {
			propertyMapping = new CouchSearchablePropertyMapping(property);
			customMappedProperties.put(name, propertyMapping);
		}

		//noinspection unchecked
		propertyMapping.addAttributes((Map<String, Object>) ((Object[]) args)[0]);
		return null;
	}

	private Set<String> convertToSet(Object arg) {
		if (arg == null) {
			return Collections.emptySet();
		} else if (arg instanceof String) {
			return Collections.singleton((String) arg);
		} else if (arg instanceof Object[]) {
			return new HashSet<String>(Arrays.asList((String[]) arg));
		} else if (arg instanceof Collection) {
			//noinspection unchecked
			return new HashSet<String>((Collection<String>) arg);
		} else {
			throw new IllegalArgumentException("Unknown argument: " + arg);
		}
	}
}