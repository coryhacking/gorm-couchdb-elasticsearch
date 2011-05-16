/**
 * This {@link groovy.util.ConfigObject} script provides the default plugin configuration.
 *
 * Note : The plugin loaded this file and merges it with the GrailsApplication#Config script.
 * Any properties set in the Config.groovy script of your application override those set here.
 * It is not required to add a reference to this file in your Config.groovy
 */

elasticSearch {

	/**
	 * Date formats
	 */
	date.formats = ["yyyy/MM/dd HH:mm:ss Z"]

	/**
	 * Hosts for remote ElasticSearch instances.
	 * Will only be used with the "transport" client mode.
	 * If the client mode is set to "transport" and no hosts are defined, ["localhost", 9300] will be used by default.
	 */
	client.hosts = [
			[host: 'localhost', port: 9300]
	]

	/**
	 * Default mapping property exclusions
	 *
	 * No properties matching the given names will be mapped by default
	 * ie, when using "searchable = true"
	 *
	 * This does not apply for classes using mapping by closure
	 */
	defaultExcludedProperties = ["password"]

	/**
 	 * define the cluster name using "cluster.name", e.g.
	 *
	 * 	cluster.name = "cluster1"
	 *
	 */
}

environments {
	development {
		/**
		 * Possible values : "local", "node", "transport"
		 * If set to null, "node" mode is used by default.
		 */
		elasticSearch.client.mode = 'local'
	}
	test {
		elasticSearch.client.mode = 'local'
	}
	production {
		elasticSearch.client.mode = 'node'
	}
}