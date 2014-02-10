/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
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
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.neo4j;


import com.intel.hadoop.graphbuilder.util.RuntimeConfig;
import org.neo4j.helpers.collection.MapUtil;

import java.util.HashMap;
import java.util.Map;

public class Neo4jConfig {

    public static final String GB_ID_FOR_NEO4J = "_gb_ID";
    public static final String NEO4J_PREFIX = "NEO4J_";

    private static Map<String, String> defaultConfigMap  = MapUtil.stringMap(
            "NEO4J_store_dir","graph.db",
            "NEO4J_use_memory_mapped_buffers", "true",
            "NEO4J_neostore.nodestore.db.mapped_memory", "2G",
            "NEO4J_neostore.relationshipstore.db.mapped_memory", "0M",
            "NEO4J_neostore.propertystore.db.mapped_memory", "1G",
            "NEO4J_neostore.propertystore.db.strings.mapped_memory", "1G",
            "NEO4J_neostore.propertystore.db.arrays.mapped_memory", "0M",
            "NEO4J_neostore.propertystore.db.index.keys.mapped_memory", "1M",
            "NEO4J_neostore.propertystore.db.index.mapped_memory", "1M",
            "NEO4J_cache_type", "none",
            "NEO4J_dump_config", "true"            
    );

    public static final RuntimeConfig config = RuntimeConfig
            .getInstanceWithDefaultConfig(defaultConfigMap);
}
