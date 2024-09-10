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
package org.thingsboard.server.dao.model.sqlts.tdengine;

import jakarta.persistence.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.dao.model.sql.AbstractTsKvEntity;

import static org.thingsboard.server.dao.sqlts.tdengine.TDengineAggregationRepository.*;


@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = "ts_kv")
@IdClass(TDengineTsKvCompositeKey.class)
@SqlResultSetMappings({
        @SqlResultSetMapping(
                name = "tdengineAggregationMapping",
                classes = {
                        @ConstructorResult(
                                targetClass = TDengineTsKvEntity.class,
                                columns = {
                                        @ColumnResult(name = "tsStart", type = Long.class),
                                        @ColumnResult(name = "interval", type = Long.class),
                                        @ColumnResult(name = "longValue", type = Long.class),
                                        @ColumnResult(name = "doubleValue", type = Double.class),
                                        @ColumnResult(name = "longCountValue", type = Long.class),
                                        @ColumnResult(name = "doubleCountValue", type = Long.class),
                                        @ColumnResult(name = "strValue", type = String.class),
                                        @ColumnResult(name = "aggType", type = String.class),
                                        @ColumnResult(name = "maxAggTs", type = Long.class),
                                }
                        ),
                }),
        @SqlResultSetMapping(
                name = "tdengineCountMapping",
                classes = {
                        @ConstructorResult(
                                targetClass = TDengineTsKvEntity.class,
                                columns = {
                                        @ColumnResult(name = "tsStart", type = Long.class),
                                        @ColumnResult(name = "interval", type = Long.class),
                                        @ColumnResult(name = "booleanValueCount", type = Long.class),
                                        @ColumnResult(name = "strValueCount", type = Long.class),
                                        @ColumnResult(name = "longValueCount", type = Long.class),
                                        @ColumnResult(name = "doubleValueCount", type = Long.class),
                                        @ColumnResult(name = "jsonValueCount", type = Long.class),
                                        @ColumnResult(name = "maxAggTs", type = Long.class),
                                }
                        )
                }),
})
@NamedNativeQueries({
        @NamedNativeQuery(
                name = FIND_AVG,
                query = FIND_AVG_QUERY + FROM_WHERE_CLAUSE,
                resultSetMapping = "tdengineAggregationMapping"
        ),
        @NamedNativeQuery(
                name = FIND_MAX,
                query = FIND_MAX_QUERY + FROM_WHERE_CLAUSE,
                resultSetMapping = "tdengineAggregationMapping"
        ),
        @NamedNativeQuery(
                name = FIND_MIN,
                query = FIND_MIN_QUERY + FROM_WHERE_CLAUSE,
                resultSetMapping = "tdengineAggregationMapping"
        ),
        @NamedNativeQuery(
                name = FIND_SUM,
                query = FIND_SUM_QUERY + FROM_WHERE_CLAUSE,
                resultSetMapping = "tdengineAggregationMapping"
        ),
        @NamedNativeQuery(
                name = FIND_COUNT,
                query = FIND_COUNT_QUERY + FROM_WHERE_CLAUSE,
                resultSetMapping = "tdengineCountMapping"
        )
})
public final class TDengineTsKvEntity extends AbstractTsKvEntity {

    public TDengineTsKvEntity() {
    }

    public TDengineTsKvEntity(Long tsStart, Long interval, Long longValue, Double doubleValue, Long longCountValue, Long doubleCountValue, String strValue, String aggType, Long aggValuesLastTs) {
        super(aggValuesLastTs);
        if (!StringUtils.isEmpty(strValue)) {
            this.strValue = strValue;
        }
        if (!isAllNull(tsStart, interval, longValue, doubleValue, longCountValue, doubleCountValue)) {
            this.ts = tsStart + interval / 2;
            switch (aggType) {
                case AVG:
                    double sum = 0.0;
                    if (longValue != null) {
                        sum += longValue;
                    }
                    if (doubleValue != null) {
                        sum += doubleValue;
                    }
                    long totalCount = longCountValue + doubleCountValue;
                    if (totalCount > 0) {
                        this.doubleValue = sum / (longCountValue + doubleCountValue);
                    } else {
                        this.doubleValue = 0.0;
                    }
                    this.aggValuesCount = totalCount;
                    break;
                case SUM:
                    if (doubleCountValue > 0) {
                        this.doubleValue = doubleValue + (longValue != null ? longValue.doubleValue() : 0.0);
                    } else {
                        this.longValue = longValue;
                    }
                    break;
                case MIN:
                case MAX:
                    if (longCountValue > 0 && doubleCountValue > 0) {
                        this.doubleValue = MAX.equals(aggType) ? Math.max(doubleValue, longValue.doubleValue()) : Math.min(doubleValue, longValue.doubleValue());
                    } else if (doubleCountValue > 0) {
                        this.doubleValue = doubleValue;
                    } else if (longCountValue > 0) {
                        this.longValue = longValue;
                    }
                    break;
            }
        }
    }

    public TDengineTsKvEntity(Long tsStart, Long interval, Long booleanValueCount, Long strValueCount, Long longValueCount, Long doubleValueCount, Long jsonValueCount, Long aggValuesLastTs) {
        super(aggValuesLastTs);
        if (!isAllNull(tsStart, interval, booleanValueCount, strValueCount, longValueCount, doubleValueCount, jsonValueCount)) {
            this.ts = tsStart + interval / 2;
            if (booleanValueCount != 0) {
                this.longValue = booleanValueCount;
            } else if (strValueCount != 0) {
                this.longValue = strValueCount;
            } else if (jsonValueCount != 0) {
                this.longValue = jsonValueCount;
            } else {
                this.longValue = longValueCount + doubleValueCount;
            }
        }
    }

    @Override
    public boolean isNotEmpty() {
        return ts != null && (strValue != null || longValue != null || doubleValue != null || booleanValue != null || jsonValue != null);
    }
}
