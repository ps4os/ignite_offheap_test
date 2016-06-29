/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package demo;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class RunIgniteTester {
    public static void main(String[] args) {
        System.setProperty("IGNITE_UPDATE_NOTIFIER", "false");

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

        Long offHeapSizeInBytes = 0L;
        for (String paramValue : runtimeMxBean.getInputArguments()) {
            if (paramValue.startsWith("-XX:MaxDirectMemorySize") && paramValue.contains("=")) {
                String offHeapSize = paramValue.split("=")[1].trim().replace(" ", "");

                int conversionFactor = -1;
                if (offHeapSize.endsWith("m"))
                    conversionFactor = 2;
                else if (offHeapSize.endsWith("g"))
                    conversionFactor = 3;

                offHeapSizeInBytes = new Long(offHeapSize.substring(0, offHeapSize.length() - 1));
                offHeapSizeInBytes = offHeapSizeInBytes * new Double(Math.pow(1024, conversionFactor)).longValue();
                break;
            }
        }

        //TODO check the effect of using '0' (in case -XX:MaxDirectMemorySize isn't configured)
        runOffHeapTest(new BigDecimal(offHeapSizeInBytes * 0.8).intValue());
    }

    private static void runOffHeapTest(int maxOffHeap) {
        final String cacheName = "demoCache";

        //Limit communication to avoid the overhead caused by the Grid-support
        TcpDiscoverySpi localNodeDiscovery = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1"));
        localNodeDiscovery.setIpFinder(ipFinder);

        IgniteConfiguration configuration = new IgniteConfiguration()
                .setDiscoverySpi(localNodeDiscovery)
                .setDaemon(false);

        Ignite ignite = Ignition.start(configuration);


        CacheConfiguration<String, CustomEntry> offHeapCacheConfig = new CacheConfiguration<String, CustomEntry>()
                .setName(cacheName)
                .setCacheMode(CacheMode.LOCAL)
                .setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
                .setSwapEnabled(false)
                .setBackups(0);

        offHeapCacheConfig.setOffHeapMaxMemory(maxOffHeap);
        //LruEvictionPolicy evictionPolicy = new LruEvictionPolicy();
        //evictionPolicy.setMaxMemorySize(new BigDecimal(offheapCacheConfig.getOffHeapMaxMemory() * 0.9).longValue());
        IgniteCache<String, CustomEntry> cache = ignite.createCache(offHeapCacheConfig);
        cache = cache.withExpiryPolicy(new AccessedExpiryPolicy(new Duration(TimeUnit.HOURS, 24)));

        insertEntries(ignite, cache, 10000000);


        cache.removeAll();
        ignite.destroyCache(cacheName);


        offHeapCacheConfig.setOffHeapMaxMemory(maxOffHeap); //TODO change to smaller value to test dyn. shrink off-heap memory
        //evictionPolicy = new LruEvictionPolicy();
        //evictionPolicy.setMaxMemorySize(new BigDecimal(offheapCacheConfig.getOffHeapMaxMemory() * 0.9).longValue());
        cache = ignite.createCache(offHeapCacheConfig);

        cache = cache.withExpiryPolicy(new AccessedExpiryPolicy(new Duration(TimeUnit.HOURS, 24)));

        insertEntries(ignite, cache, 20000000);

        cache.removeAll();
        ignite.destroyCache(cacheName);


        offHeapCacheConfig.setOffHeapMaxMemory(maxOffHeap); //TODO increase value again (in case of a decrease)
        //evictionPolicy = new LruEvictionPolicy();
        //evictionPolicy.setMaxMemorySize(new BigDecimal(offheapCacheConfig.getOffHeapMaxMemory() * 0.9).longValue());
        cache = ignite.createCache(offHeapCacheConfig);

        cache = cache.withExpiryPolicy(new AccessedExpiryPolicy(new Duration(TimeUnit.HOURS, 24)));

        insertEntries(ignite, cache, 10000000);

        System.out.println("finished");
        ignite.destroyCache(cacheName);
        ignite.close();
    }

    private static void insertEntries(Ignite ignite, IgniteCache<String, CustomEntry> cache, int numberOfEntries) {
        System.out.println("--------------------------------------------------------------------");
        System.out.println("Used Off-Heap: " + cache.metrics().getOffHeapAllocatedSize() / 1024 / 1024); //should be '0'
        System.out.println("Off-Heap entry-count: " + cache.metrics().getOffHeapEntriesCount()); //should be '0'

        try (IgniteDataStreamer<String, CustomEntry> stream = ignite.dataStreamer(cache.getName())) {
            stream.allowOverwrite(true);

            for (int i = 0; i < numberOfEntries; i++) {
                stream.addData("" + i, new CustomEntry("1_" + i, "2", "3"));
            }
        }

        System.out.println("Used off-heap: " + cache.metrics().getOffHeapAllocatedSize() / 1024 / 1024); //TODO check why Ignite 1.6.0 returns '0' here
        System.out.println("Off-heap entry-count: " + cache.metrics().getOffHeapEntriesCount()); //TODO check why Ignite 1.6.0 returns '0' here
    }
}
