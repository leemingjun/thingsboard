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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.DaoUtil;
import org.thingsboard.server.dao.model.sql.AbstractTsKvEntity;
import org.thingsboard.server.dao.model.sqlts.tdengine.TDengineTsKvEntity;
import org.thingsboard.server.dao.sqlts.insert.InsertTsRepository;
import org.thingsboard.server.dao.sqlts.insert.tdengine.TDengineInsertTsRepository;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;
import org.thingsboard.server.dao.util.TDengineDBTsDao;

import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
@TDengineDBTsDao
public class TDengineTimeseriesDao implements TimeseriesDao {

    @Autowired
    private TDengineAggregationRepository aggregationRepository;

    @Autowired
    private TsKvTDengineRepository tsKvTDengineRepository;

    @Autowired
    private TDengineInsertTsRepository insertTsRepository;

    @Override
    public ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries) {
        List<ListenableFuture<ReadTsKvQueryResult>> futures = queries
                .stream()
                .map(query -> findAllAsync(tenantId, entityId, query))
                .collect(Collectors.toList());
        return Futures.transform(Futures.allAsList(futures), new Function<>() {
            @Nullable
            @Override
            public List<ReadTsKvQueryResult> apply(@Nullable List<ReadTsKvQueryResult> results) {
                if (results == null || results.isEmpty()) {
                    return null;
                }
                return results.stream().filter(Objects::nonNull).collect(Collectors.toList());
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Integer> save(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
        String strKey = tsKvEntry.getKey();
        TDengineTsKvEntity entity = new TDengineTsKvEntity();
        entity.setEntityId(entityId.getId());
        entity.setTs(tsKvEntry.getTs());
        entity.setKey(0);   // No Used
        entity.setStrKey(strKey);
        entity.setStrValue(tsKvEntry.getStrValue().orElse(null));
        entity.setDoubleValue(tsKvEntry.getDoubleValue().orElse(null));
        entity.setLongValue(tsKvEntry.getLongValue().orElse(null));
        entity.setBooleanValue(tsKvEntry.getBooleanValue().orElse(null));
        entity.setJsonValue(tsKvEntry.getJsonValue().orElse(null));
        log.trace("Saving entity to TDengine db: {}", entity);
        List<TDengineTsKvEntity> entities = new ArrayList<TDengineTsKvEntity>();
        entities.add(entity);
        insertTsRepository.saveOrUpdate(entities);
        return Futures.immediateFuture(1);
    }

    @Override
    public ListenableFuture<Integer> savePartition(TenantId tenantId, EntityId entityId, long tsKvEntryTs, String key) {
        return Futures.immediateFuture(0);
    }

    @Override
    public ListenableFuture<Void> remove(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
        tsKvTDengineRepository.remove(tenantId, entityId, query);
        return null;
    }

    public ListenableFuture<ReadTsKvQueryResult> findAllAsync(TenantId tenantId, EntityId entityId, ReadTsKvQuery query) {
        var aggParams = query.getAggParameters();
        var intervalType = aggParams.getIntervalType();
        if (query.getAggregation() == Aggregation.NONE) {
            return Futures.immediateFuture(findAllAsyncWithLimit(entityId, query));
        } else {
            long startTs = query.getStartTs();
            long endTs = query.getEndTs();
            List<TDengineTsKvEntity> entities = new ArrayList<>();
                entities.addAll(switchAggregation(query.getKey(), startTs, endTs, endTs - startTs, query.getAggregation(), entityId.getId()));

            return getReadTsKvQueryResultFuture(query, Futures.immediateFuture(toResultList(entityId, query.getKey(), entities)));
        }
    }

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

    @Override
    public void cleanup(long systemTtl) {
//        TTL is a native function in TDengine.
    }

    private ReadTsKvQueryResult findAllAsyncWithLimit(EntityId entityId, ReadTsKvQuery query) {
       return tsKvTDengineRepository.findAllAsyncWithLimit(entityId, query);
    }

    private List<Optional<? extends AbstractTsKvEntity>> findAllAndAggregateAsync(EntityId entityId, String key, long startTs, long endTs, long timeBucket, Aggregation aggregation) {
        long interval = endTs - startTs;
        long remainingPart = interval % timeBucket;
        List<TDengineTsKvEntity> entities;
        if (remainingPart == 0) {
            entities = switchAggregation(key, startTs, endTs, timeBucket, aggregation, entityId.getId());
        } else {
            interval = interval - remainingPart;
            entities = new ArrayList<>();
            entities.addAll(switchAggregation(key, startTs, startTs + interval, timeBucket, aggregation, entityId.getId()));
            entities.addAll(switchAggregation(key, startTs + interval, endTs, remainingPart, aggregation, entityId.getId()));
        }

        return toResultList(entityId, key, entities);
    }

    private static List<Optional<? extends AbstractTsKvEntity>> toResultList(EntityId entityId, String key, List<TDengineTsKvEntity> entities) {
        if (!CollectionUtils.isEmpty(entities)) {
            List<Optional<? extends AbstractTsKvEntity>> result = new ArrayList<>();
            entities.forEach(entity -> {
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

}
