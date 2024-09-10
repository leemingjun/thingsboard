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

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import org.thingsboard.server.dao.model.sqlts.tdengine.TDengineTsKvEntity;

import java.util.List;
import java.util.UUID;

@Repository
public class TDengineAggregationRepository {

    public static final String FIND_AVG = "findAvg";
    public static final String FIND_MAX = "findMax";
    public static final String FIND_MIN = "findMin";
    public static final String FIND_SUM = "findSum";
    public static final String FIND_COUNT = "findCount";

    public static final String FROM_WHERE_CLAUSE = "FROM ts_kv tskv WHERE " +
            "tskv.entity_id = :entityId" +
            "AND tskv.str_key= :entityKey " +
            "AND tskv.ts >= :startTs AND tskv.ts < :endTs " +
            "INTERVAL(:interval) " +
            "ORDER BY tskv.entity_id, tskv.str_key, tsStart";

    public static final String FIND_AVG_QUERY = "SELECT " +
            "_wstart AS tsStart, :timeBucket AS `interval`, " +
            "SUM(tskv.long_v) AS longValue, " +
            "SUM(tskv.dbl_v) AS doubleValue, " +
            "COUNT(tskv.long_v) AS longCountValue, " +
            "COUNT(tskv.dbl_v) AS doubleCountValue, " +
            "null AS strValue, 'AVG' AS aggType, _wend AS maxAggTs ";

    public static final String FIND_MAX_QUERY = "SELECT " +
            "_wstart AS tsStart, :timeBucket AS `interval`, " +
            "MAX(tskv.long_v) AS longValue, " +
            "MAX(tskv.dbl_v) AS doubleValue, " +
            "COUNT(tskv.long_v) AS longCountValue, " +
            "COUNT(tskv.dbl_v) AS doubleCountValue, " +
            "MAX(tskv.str_v) AS strValue, 'MAX' AS aggType,  _wend AS maxAggTs  ";

    public static final String FIND_MIN_QUERY = "SELECT " +
            "_wstart AS tsStart, :timeBucket AS `interval`, " +
            "MIN(tskv.long_v) AS longValue, " +
            "MIN(tskv.dbl_v) AS doubleValue, " +
            "COUNT(tskv.long_v) AS longCountValue, " +
            "COUNT(tskv.dbl_v) AS doubleCountValue, " +
            "MIN(tskv.str_v) AS strValue, 'MIN' AS aggType, _wend AS maxAggTs  ";

    public static final String FIND_SUM_QUERY = "SELECT " +
            "_wstart AS tsStart, :timeBucket AS `interval`, " +
            "SUM(tskv.long_v) AS longValue, " +
            "SUM(tskv.dbl_v) AS doubleValue, " +
            "COUNT(tskv.long_v) AS longCountValue, " +
            "COUNT(tskv.dbl_v) AS doubleCountValue, " +
            "null AS strValue, null AS jsonValue, 'SUM' AS aggType, MAX(tskv.ts) AS maxAggTs ";

    public static final String FIND_COUNT_QUERY = "SELECT " +
            "_wstart AS tsStart, :timeBucket AS `interval`, " +
            "COUNT(tskv.bool_v) AS booleanValueCount, " +
            "COUNT(tskv.str_v) AS strValueCount, " +
            "COUNT(tskv.long_v) AS longCountValue, " +
            "COUNT(tskv.dbl_v) AS doubleCountValue, " +
            "COUNT(tskv.json_v) AS jsonValueCount, " +
            "_wend AS maxAggTs  ";

    @PersistenceContext
    @Qualifier("tdengineEntityManager")
    private EntityManager entityManager;

    @SuppressWarnings("unchecked")
    public List<TDengineTsKvEntity> findAvg(UUID entityId, String entityKey, long interval, long startTs, long endTs) {
        return getResultList(entityId, entityKey, interval, startTs, endTs, FIND_AVG);
    }

    @SuppressWarnings("unchecked")
    public List<TDengineTsKvEntity> findMax(UUID entityId, String entityKey, long interval, long startTs, long endTs) {
        return getResultList(entityId, entityKey, interval, startTs, endTs, FIND_MAX);
    }

    @SuppressWarnings("unchecked")
    public List<TDengineTsKvEntity> findMin(UUID entityId, String entityKey, long interval, long startTs, long endTs) {
        return getResultList(entityId, entityKey, interval, startTs, endTs, FIND_MIN);
    }

    @SuppressWarnings("unchecked")
    public List<TDengineTsKvEntity> findSum(UUID entityId, String entityKey, long interval, long startTs, long endTs) {
        return getResultList(entityId, entityKey, interval, startTs, endTs, FIND_SUM);
    }

    @SuppressWarnings("unchecked")
    public List<TDengineTsKvEntity> findCount(UUID entityId, String entityKey, long interval, long startTs, long endTs) {
        return getResultList(entityId, entityKey, interval, startTs, endTs, FIND_COUNT);
    }

    private List getResultList(UUID entityId, String entityKey, long interval, long startTs, long endTs, String query) {
        return entityManager.createNamedQuery(query)
                .setParameter("entityId", entityId)
                .setParameter("entityKey", entityKey)
                .setParameter("interval", interval)
                .setParameter("startTs", startTs)
                .setParameter("endTs", endTs)
                .getResultList();
    }

}
