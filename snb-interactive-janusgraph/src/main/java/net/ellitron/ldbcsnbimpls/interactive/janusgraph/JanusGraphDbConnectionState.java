/*
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.ellitron.ldbcsnbimpls.interactive.janusgraph;

import com.ldbc.driver.DbConnectionState;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

public class JanusGraphDbConnectionState extends DbConnectionState {
    private Graph client;

    public JanusGraphDbConnectionState(Map<String, String> props) {
        BaseConfiguration config = new BaseConfiguration();
        config.setDelimiterParsingDisabled(true);

        /*
         * Extract parameters from properties map.
         */
        String cassandraLocator;
        if (props.containsKey("cassandraLocator")) {
            cassandraLocator = props.get("cassandraLocator");
        } else {
            cassandraLocator = "127.0.0.1";
        }

        String graphName;
        if (props.containsKey("graphName")) {
            graphName = props.get("graphName");
        } else {
            graphName = "default";
        }

        config.setProperty("storage.backend", "cassandra");
        config.setProperty("storage.hostname", cassandraLocator);
        config.setProperty("storage.cassandra.keyspace", graphName);

        client = JanusGraphFactory.open(config);
    }

    public Graph getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(JanusGraphDb.class.getName())
                    .log(Level.SEVERE, null, ex);
        }
    }
}
