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
package org.thingsboard.server.dao.sqlts.tdengine;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.model.ModelConstants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
@Repository
public class TsKvTDengineRepository {

    @Autowired
    @Qualifier("tdengineJdbcTemplate")
    private JdbcTemplate tdengineJdbcTemplate;

    static final String FIND_LASTEST_QUERY = "select * from ts_kv tskv where tskv.entity_id = ? and tskv.str_key = ? " +
            "order by ts desc limit 1";

    static final String FIND_ALL_WITH_LIMIT = "SELECT * FROM ts_kv WHERE entity_id = ? " +
            "AND str_key = ? AND ts >= ? AND ts < ? order by ts desc limit 100";

    static final String FIND_ALL_LASTEST = "SELECT last_row(ts) as ts, last_row(bool_v) as bool_v, last_row(str_v) as str_v, " +
            "last_row(long_v) as long_v, last_row(dbl_v) as dbl_v, last_row(json_v) as json_v, " +
            "entity_id, str_key FROM ts_kv tskv WHERE tskv.entity_id = ? partition by tbname ";

    static final String FIND_ALL_KEYS_BY_ENTITYID = "select distinct str_key from ts_kv tskv "+
            "WHERE tskv.entity_id in (:entityIds) ";

    static final String DELETE = "DELETE FROM ts_kv WHERE entiti_id = ? AND str_key = ? " +
            "and ts >= ? AND ts < ? ";

    public TsKvTDengineRepository() {
    }

    public ListenableFuture<List<TsKvEntry>> findAllLatest(TenantId tenantId, EntityId entityId) {
        List rs = tdengineJdbcTemplate.query(TsKvTDengineRepository.FIND_ALL_LASTEST,
                new TDengineTsKvEntityRowMapper(),
                entityId.getId().toString());
        return  Futures.immediateFuture((List<TsKvEntry>) rs);
    }

    public void remove(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query){
        tdengineJdbcTemplate.update(DELETE, entityId.getId(), query.getStartTs(), query.getEndTs());
    }

    public ReadTsKvQueryResult findAllAsyncWithLimit(EntityId entityId, ReadTsKvQuery query) {
        String strKey = query.getKey();
        List<TsKvEntry> tsKvEntries = tdengineJdbcTemplate.query(FIND_ALL_WITH_LIMIT,
                new TDengineTsKvEntityRowMapper(),
                entityId.getId(), strKey, query.getStartTs(), query.getEndTs());

        long lastTs = query.getStartTs();
        for (int i = 0; i < tsKvEntries.size(); i++) {
            if (lastTs < tsKvEntries.get(i).getTs()) {
                lastTs = tsKvEntries.get(i).getTs();
            }
        }
        return new ReadTsKvQueryResult(query.getId(), tsKvEntries, lastTs);
    }

    public List<String> findAllKeysByEntityId(List<String> entityKeys) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(tdengineJdbcTemplate);
        SqlParameterSource parameters = new MapSqlParameterSource("entityIds", entityKeys);

        List rs = jdbcTemplate.query(FIND_ALL_KEYS_BY_ENTITYID, parameters, (rs1, rowNum) -> rs1.getString("str_key"));
        return rs;
    }

    public static class TDengineTsKvEntityRowMapper implements RowMapper<TsKvEntry> {
        @Override
        public TsKvEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
            return getTsKvEntry(rs, log);
        }

        @NotNull
        static TsKvEntry getTsKvEntry(ResultSet rs, Logger log) throws SQLException {
            String key = rs.getString("str_key");
            long ts = rs.getLong(ModelConstants.TS_COLUMN);

            KvEntry kvEntry = null;
            String strV = rs.getString("str_v");
            if (strV != null) {
                kvEntry = new StringDataEntry(key, strV);
            } else {
                long longV = rs.getLong("long_v");
                if (!rs.wasNull()) {
                    kvEntry = new LongDataEntry(key, longV);
                } else {
                    double doubleV = rs.getDouble("dbl_v");
                    if (!rs.wasNull()) {
                        kvEntry = new DoubleDataEntry(key, doubleV);
                    } else {
                        boolean boolV = rs.getBoolean("bool_v");
                        if (!rs.wasNull()) {
                            kvEntry = new BooleanDataEntry(key, boolV);
                        } else {
                            String jsonV = rs.getString("json_v");
                            if (StringUtils.isNoneEmpty(jsonV)) {
                                kvEntry = new JsonDataEntry(key, jsonV);
                            } else {
                                log.warn("All values in key-value row are nullable ");
                            }
                        }
                    }
                }
            }
            return new BasicTsKvEntry(ts, kvEntry);
        }
    }

}