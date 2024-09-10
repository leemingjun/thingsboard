/**
 * Copyright © 2016-2024 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.install;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
@Profile("install")
@Slf4j
public class TDengineTsDatabaseSchemaService implements TsDatabaseSchemaService {
    protected static final String SQL_DIR = "sql";

    @Value("${tdengine.datasource.jdbcUrl}")
    protected String dbUrl;

    @Value("${tdengine.datasource.username}")
    protected String dbUserName;

    @Value("${tdengine.datasource.password}")
    protected String dbPassword;

    @Autowired
    protected InstallScripts installScripts;

    private final String schemaSql;

    protected TDengineTsDatabaseSchemaService() {
        this.schemaSql = "schema-tdengine.sql";
    }


    @Override
    public void createDatabaseSchema() throws Exception {
        executeQueryFromFile(schemaSql);
    }


    @Override
    public void createDatabaseSchema(boolean createIndexes) throws Exception {
    }

    @Override
    public void createDatabaseIndexes() throws Exception {
    }

    void executeQueryFromFile(String schemaIdxSql) throws SQLException, IOException {
        Path schemaIdxFile = Paths.get(installScripts.getDataDir(), SQL_DIR, schemaIdxSql);
        String sql = Files.readString(schemaIdxFile);
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUserName, dbPassword)) {
            conn.createStatement().execute(sql); //NOSONAR, ignoring because method used to load initial thingsboard database schema
        }
    }
}