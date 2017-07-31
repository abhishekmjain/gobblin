/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gobblin.data.management.conversion.hive.source;

import java.util.List;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * An extension to {@link HiveSource} that is used for Avro to ORC conversion jobs.
 */
public class HiveAvroToOrcSource extends HiveSource {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    if (!state.contains(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY)) {
      state.setProp(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY, ConvertibleHiveDatasetFinder.class.getName());
    }
    if (!state.contains(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY)) {
      state.setProp(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, "hive.conversion.avro");
    }

    return super.getWorkunits(state);
  }

}
