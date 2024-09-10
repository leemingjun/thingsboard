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

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.DaoUtil;
import org.thingsboard.server.dao.model.ModelConstants;
import org.thingsboard.server.dao.model.sql.AbstractTsKvEntity;
import org.thingsboard.server.dao.model.sqlts.tdengine.TDengineTsKvEntity;
import org.thingsboard.server.dao.sql.device.DeviceIdRepository;
import org.thingsboard.server.dao.timeseries.TimeseriesLatestDao;
import org.thingsboard.server.dao.util.TDengineDBTsLatestDao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.thingsboard.server.dao.sqlts.tdengine.TsKvTDengineRepository.TDengineTsKvEntityRowMapper.getTsKvEntry;

@Repository
@Component
@Slf4j
@TDengineDBTsLatestDao
public class TDengineTimeseriesLastestDao implements TimeseriesLatestDao {

    @Autowired
    @Qualifier("tdengineJdbcTemplate")
    private JdbcTemplate tdengineJdbcTemplate;

    @Autowired
    private TDengineAggregationRepository aggregationRepository;

    @Autowired
    private TsKvTDengineRepository tsKvTDengineRepository;

    @Autowired
    private DeviceIdRepository deviceIdRepository;

    protected ListenableFuture<ReadTsKvQueryResult> getReadTsKvQueryResultFuture(ReadTsKvQuery query, ListenableFuture<List<Optional<? extends AbstractTsKvEntity>>> future) {
        return Futures.transform(future, new Function<>() {
            @Nullable
            @Override
            public ReadTsKvQueryResult apply(@Nullable List<Optional<? extends AbstractTsKvEntity>> results) {
                if (results == null || results.isEmpty()) {
                    return null;
                }
                List<? extends AbstractTsKvEntity> data = results.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
                var lastTs = data.stream().map(AbstractTsKvEntity::getAggValuesLastTs).filter(Objects::nonNull).max(Long::compare);
                if (lastTs.isEmpty()) {
                    lastTs = data.stream().map(AbstractTsKvEntity::getTs).filter(Objects::nonNull).max(Long::compare);
                }
                return new ReadTsKvQueryResult(query.getId(), DaoUtil.convertDataList(data), lastTs.orElse(query.getStartTs()));
            }
        }, MoreExecutors.directExecutor());
    }

    private List<TDengineTsKvEntity> switchAggregation(String key, long startTs, long endTs, long timeBucket, Aggregation aggregation, UUID entityId) {
        switch (aggregation) {
            case AVG:
                return aggregationRepository.findAvg(entityId, key, timeBucket, startTs, endTs);
            case MAX:
                return aggregationRepository.findMax(entityId, key, timeBucket, startTs, endTs);
            case MIN:
                return aggregationRepository.findMin(entityId, key, timeBucket, startTs, endTs);
            case SUM:
                return aggregationRepository.findSum(entityId, key, timeBucket, startTs, endTs);
            case COUNT:
                return aggregationRepository.findCount(entityId, key, timeBucket, startTs, endTs);
            default:
                throw new IllegalArgumentException("Not supported aggregation type: " + aggregation);
        }
    }

    private static List<Optional<? extends AbstractTsKvEntity>> toResultList(EntityId entityId, String key, List<TDengineTsKvEntity> tdengineTsKvEntities) {
        if (!CollectionUtils.isEmpty(tdengineTsKvEntities)) {
            List<Optional<? extends AbstractTsKvEntity>> result = new ArrayList<>();
            tdengineTsKvEntities.forEach(entity -> {
                if (entity != null && entity.isNotEmpty()) {
                    entity.setEntityId(entityId.getId());
                    entity.setStrKey(key);
                    result.add(Optional.of(entity));
                } else {
                    result.add(Optional.empty());
                }
            });
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public ListenableFuture<Optional<TsKvEntry>> findLatestOpt(TenantId tenantId, EntityId entityId, String key) {
        List entities = tdengineJdbcTemplate.query(TsKvTDengineRepository.FIND_LASTEST_QUERY,
                new TDengineTsKvEntityRowMapper(),
                entityId.getId(), key);

        Optional<TsKvEntry> result = entities.isEmpty() ? Optional.empty() : Optional.of((TsKvEntry)entities.get(0));
        return Futures.immediateFuture(result);
    }

    @Override
    public ListenableFuture<TsKvEntry> findLatest(TenantId tenantId, EntityId entityId, String key) {
        List entities = tdengineJdbcTemplate.query(TsKvTDengineRepository.FIND_LASTEST_QUERY,
                new TDengineTsKvEntityRowMapper(),
                entityId.getId().toString(), key);

        TsKvEntry result = entities.isEmpty() ? null : (TsKvEntry) entities.get(0);
        if (result == null) {
            result = new BasicTsKvEntry(System.currentTimeMillis(), new StringDataEntry(key, null));
        }
        return Futures.immediateFuture(result);
    }

    @Override
    public ListenableFuture<List<TsKvEntry>> findAllLatest(TenantId tenantId, EntityId entityId) {
        return tsKvTDengineRepository.findAllLatest(tenantId, entityId);
    }

    @Override
    public ListenableFuture<Long> saveLatest(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry) {
//      Lastest time series is cached by TDengine.
        return Futures.immediateFuture(1L);
    }

    @Override
    public ListenableFuture<TsKvLatestRemovingResult> removeLatest(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
//      No need delete! Lastest time series is cached by TDengine.
        return Futures.immediateFuture(new TsKvLatestRemovingResult(query.getKey(), true));
    }

    public ListenableFuture<ReadTsKvQueryResult> findAllAsync(TenantId tenantId, EntityId entityId, ReadTsKvQuery query) {
        var aggParams = query.getAggParameters();
        if (query.getAggregation() == Aggregation.NONE) {
            return Futures.immediateFuture(tsKvTDengineRepository.findAllAsyncWithLimit(entityId, query));
        } else {
            long startTs = query.getStartTs();
            long endTs = query.getEndTs();
            List<TDengineTsKvEntity> tdenginesKvEntities = new ArrayList<TDengineTsKvEntity>();
            tdenginesKvEntities.addAll(switchAggregation(query.getKey(), startTs, endTs, endTs - startTs, query.getAggregation(), entityId.getId()));
            return getReadTsKvQueryResultFuture(query, Futures.immediateFuture(org.thingsboard.server.dao.sqlts.tdengine.TDengineTimeseriesLastestDao.toResultList(entityId, query.getKey(), tdenginesKvEntities)));
        }
    }

    @Override
    public List<String> findAllKeysByDeviceProfileId(TenantId tenantId, DeviceProfileId deviceProfileId) {
        List<UUID> uuids = null;
        if (deviceProfileId != null) {
            uuids = deviceIdRepository.findIdsByDeviceProfileId(deviceProfileId.getId(), tenantId.getId());
        }
        else {
            uuids = deviceIdRepository.findIdsByDeviceProfileId(deviceProfileId.getId(), tenantId.getId());
        }
        if (uuids.isEmpty()){
            return Collections.emptyList();
        }

        return (List<String>) findAllKeysByEntityId(uuids);
    }

    @Override
    public List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds) {
        List<UUID> uuids = entityIds.stream().map(EntityId::getId).collect(Collectors.toList());
        return (List<String>) findAllKeysByEntityId(uuids);
    }

    private List<String> findAllKeysByEntityId(List<UUID> uuids){
        List<String> keys = uuids.stream().map(UUID::toString).toList();
        return tsKvTDengineRepository.findAllKeysByEntityId(keys);
    }

    public class TDengineTsKvEntityRowMapper implements RowMapper<TsKvEntry> {
        @Override
        public TsKvEntry mapRow(ResultSet rs, int rowNum) throws SQLException {
            return getTsKvEntry(rs, log);
        }
    }

}
