# OktaBulkLoader

Requirements:

* Maven - to build the jar file
* JDK 1.6+ 


Steps:

1. Install Maven - https://maven.apache.org/install.html

2. Download OktaBulkLoad repository from Github - https://github.com/schandra-okta/OktaBulkLoader.git

3. Make appropriate changes to the config.properties file to match up with
   your local environment

3. Change directory to project folder. You need to be in the directory where pom.xml file
   exists.

4. Build project file using the below command. You could also import the project in the IDE of
   your choice assuming that said IDE has a Maven integration built in
	
	$> mvn package

5. Using the jar file that is created in the target directory, start up the application

	$> java -jar target/okta-bulkload.jar <path_to_config_file>
