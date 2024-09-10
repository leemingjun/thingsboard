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
package org.thingsboard.server.dao.sql.device;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.thingsboard.server.dao.model.sql.DeviceEntity;

import java.util.List;
import java.util.UUID;

public interface DeviceIdRepository extends  JpaRepository<DeviceEntity, UUID> {

    @Query(value = "SELECT id FROM device WHERE tenant_id = :tenantId limit 100 ", nativeQuery = true)
    List<UUID> findIdsByTenantId(@Param("tenantId") UUID tenantId);


    @Query(value = "SELECT id FROM device WHERE device_profile_id = :deviceProfileId " +
            "AND tenant_id = :tenantId limit 100 ", nativeQuery = true)
    List<UUID> findIdsByDeviceProfileId(@Param("deviceProfileId") UUID deviceProfileId,
                                          @Param("tenantId") UUID tenantId);
}
