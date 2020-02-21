/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.util.Properties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.google.common.io.Files;


@Configuration
// @Import({ cfgTimeSeriesStorage.class })
public class cfgArchiveTest
{

    @Bean
    public static PropertySourcesPlaceholderConfigurer testProperties() throws Exception {
        final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        Properties properties = new Properties();
        // to do add custom props as necessary
        properties.setProperty( "metrics.store.indexDir", Files.createTempDir().getAbsolutePath() );
        properties.setProperty( "metrics.store.dataDir", Files.createTempDir().getAbsolutePath() );
        properties.setProperty( "metrics.store.stagingDir", Files.createTempDir().getAbsolutePath() );
        properties.setProperty( "metrics.store.enabled", "true" );
        // Files.createTempDir().getAbsolutePath() );
        pspc.setProperties(properties);
        return pspc;
    }

    @Bean(name="dataDir")
    public File dataDir()
    {
        return Files.createTempDir();
    }

    @Bean(name="stagingDir")
    public File stagingDir()
    {
        return Files.createTempDir();
    }
}
