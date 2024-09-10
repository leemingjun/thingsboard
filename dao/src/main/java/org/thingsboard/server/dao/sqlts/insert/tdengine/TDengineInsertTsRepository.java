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
package org.thingsboard.server.dao.sqlts.insert.tdengine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.dao.model.sqlts.tdengine.TDengineTsKvEntity;
import org.thingsboard.server.dao.sqlts.insert.AbstractInsertRepository;
import org.thingsboard.server.dao.sqlts.insert.InsertTsRepository;
import org.thingsboard.server.dao.util.TDengineDBTsDao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@TDengineDBTsDao
@Repository
@Transactional
public class TDengineInsertTsRepository implements InsertTsRepository<TDengineTsKvEntity> {

    @Autowired
    @Qualifier("tdengineJdbcTemplate")
    private JdbcTemplate tdengineJdbcTemplate;

    private static final String INSERT_OR_UPDATE =
            "INSERT INTO ? USING ts_kv TAGS (?, ?) (ts, bool_v, str_v, long_v, dbl_v, json_v) VALUES(?, ?, ?, ?, ?, ?) ";

    public void saveOrUpdate(List<TDengineTsKvEntity> entities) {
        tdengineJdbcTemplate.batchUpdate(INSERT_OR_UPDATE, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement pst, int i) throws SQLException {
                TDengineTsKvEntity tsKvEntity = entities.get(i);
                String childTableName = "t_" + tsKvEntity.getEntityId().toString().replace("-","")
                        + "_" + tsKvEntity.getStrKey();

                pst.setString(1, childTableName);
                pst.setString(2, tsKvEntity.getEntityId().toString());
                pst.setString(3, tsKvEntity.getStrKey());

                pst.setLong(4, tsKvEntity.getTs());
                if(null == tsKvEntity.getBooleanValue()){
                    pst.setNull(5, Types.BOOLEAN);
                }
                else {
                    pst.setBoolean(5, tsKvEntity.getBooleanValue());
                }
                pst.setString(6, tsKvEntity.getStrValue());
                if(null == tsKvEntity.getLongValue()){
                   pst.setNull(7, Types.BIGINT);
                }
                else {
                   pst.setLong(7, tsKvEntity.getLongValue());
                }
                if(null == tsKvEntity.getDoubleValue()){
                    pst.setNull(8, Types.DOUBLE);
                }
                else {
                    pst.setDouble(8, tsKvEntity.getDoubleValue());
                }
                pst.setString(9, tsKvEntity.getJsonValue());
            }

            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
    }
}
