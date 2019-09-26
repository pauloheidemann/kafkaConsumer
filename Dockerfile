# You can use the official wildfly image if you're planning to run Java 8 web apps
FROM jboss/wildfly

# Expose the default's application port
EXPOSE 8080

# Copy the war file to the deployments folder
COPY target/kafkaConsumerIntegradorGenesisKaffa.war /opt/jboss/wildfly/standalone/deployments/kafkaConsumerIntegradorGenesisKaffa.war