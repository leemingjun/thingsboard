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
package org.thingsboard.server.dao.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.BootstrapMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.thingsboard.server.dao.util.TDengineDBTsDao;
import org.thingsboard.server.dao.util.TbAutoConfiguration;

import javax.sql.DataSource;

@Configuration
@TbAutoConfiguration
@ComponentScan({"org.thingsboard.server.dao.sqlts.tdengine"})
@EnableJpaRepositories(value = {"org.thingsboard.server.dao.sqlts.tdengine",
        "org.thingsboard.server.dao.sqlts.insert.tdengine"}, bootstrapMode = BootstrapMode.DEFAULT)
@EnableTransactionManagement
@TDengineDBTsDao
public class TDengineDaoConfig {

    @Bean
    @ConfigurationProperties(prefix = "tdengine.datasource")
    public DataSource tdengineDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate tdengineJdbcTemplate(@Qualifier("tdengineDataSource") DataSource tdengineDataSource) {
        return new JdbcTemplate(tdengineDataSource);
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean tdengineEntityManager(
            EntityManagerFactoryBuilder builder,
            @Qualifier("tdengineDataSource")DataSource dataSource) {
        return builder
                .dataSource(dataSource)
                .packages("org.thingsboard.server.dao.model.sqlts.tdengine")
                .persistenceUnit("tdengine") // 可选：指定持久化单元名称
                .build();
    }

    @Bean
    public PlatformTransactionManager tdengineTransactionManager(
            @Qualifier("tdengineDataSource") DataSource dataSource,
            @Qualifier("tdengineEntityManager") LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory.getObject());
    }
}
