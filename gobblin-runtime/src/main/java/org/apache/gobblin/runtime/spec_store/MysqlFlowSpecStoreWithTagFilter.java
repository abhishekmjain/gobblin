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

package org.apache.gobblin.runtime.spec_store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY;
import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY;


/**
 * Implementation of {@link SpecStore} that stores specs in MySQL both as a serialized BLOB and as a JSON string, and extends
 * {@link MysqlSpecStore} with enhanced capabilities to filter on tag value.  The {@link SpecSerDe}'s serialized output
 * is presumed suitable for a MySql `JSON` column.  As in the base, versions are unsupported and ignored.
 */
@Slf4j
public class MysqlFlowSpecStoreWithTagFilter extends MysqlSpecStore {
  public static final String CONFIG_PREFIX = "mysqlFlowSpecStore";

  private static final String SPECIFIC_GET_STATEMENT_BASE = "SELECT spec_uri, spec, spec_json FROM %s WHERE tag In ('TAG_FILTER_REPLACE_STR') AND ";
  private static final String SPECIFIC_GET_ALL_STATEMENT = "SELECT spec_uri, spec, spec_json, modified_time FROM %s WHERE tag In ('TAG_FILTER_REPLACE_STR')";
  private static final String SPECIFIC_GET_SPECS_BATCH_STATEMENT = "SELECT spec_uri, spec, spec_json, modified_time FROM %s WHERE tag In ('TAG_FILTER_REPLACE_STR') ORDER BY spec_uri ASC LIMIT ? OFFSET ?";
  private static final String SPECIFIC_GET_ALL_URIS_STATEMENT = "SELECT spec_uri FROM %s WHERE tag In ('TAG_FILTER_REPLACE_STR')";
  private static final String SPECIFIC_GET_SIZE_STATEMENT = "SELECT COUNT(*) FROM %s WHERE tag In ('TAG_FILTER_REPLACE_STR')";

  /** Bundle all changes following from schema differences against the base class. */
  protected class TagFilterSpecificSqlStatements extends SpecificSqlStatements {
    private final List<String> tagFilters;
    public TagFilterSpecificSqlStatements (List<String> tagFilters) {
      this.tagFilters = tagFilters;
    }

    @Override
    public void completeInsertPreparedStatement(PreparedStatement statement, Spec spec, String tagValue) throws SQLException {
      FlowSpec flowSpec = (FlowSpec) spec;
      URI specUri = flowSpec.getUri();
      Config flowConfig = flowSpec.getConfig();
      String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
      String templateURI = new Gson().toJson(flowSpec.getTemplateURIs());
      String userToProxy = ConfigUtils.getString(flowSpec.getConfig(), "user.to.proxy", null);
      String sourceIdentifier = flowConfig.getString(FLOW_SOURCE_IDENTIFIER_KEY);
      String destinationIdentifier = flowConfig.getString(FLOW_DESTINATION_IDENTIFIER_KEY);
      String schedule = ConfigUtils.getString(flowConfig, ConfigurationKeys.JOB_SCHEDULE_KEY, null);
      String owningGroup = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_OWNING_GROUP_KEY, null);
      boolean isRunImmediately = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, false);

      int i = 0;
      statement.setString(++i, specUri.toString());
      statement.setString(++i, flowGroup);
      statement.setString(++i, flowName);
      statement.setString(++i, templateURI);
      statement.setString(++i, userToProxy);
      statement.setString(++i, sourceIdentifier);
      statement.setString(++i, destinationIdentifier);
      statement.setString(++i, schedule);
      // use first value from comma separated tag filters as insert tag value
      statement.setString(++i, tagFilters.get(0));
      statement.setBoolean(++i, isRunImmediately);
      statement.setString(++i, owningGroup);
      statement.setBlob(++i, new ByteArrayInputStream(MysqlFlowSpecStoreWithTagFilter.this.specSerDe.serialize(flowSpec)));
      statement.setString(++i, new String(MysqlFlowSpecStoreWithTagFilter.this.specSerDe.serialize(flowSpec), Charsets.UTF_8));
    }

    @Override
    protected String getTablelessGetStatementBase() { return replaceTagFilterValue(MysqlFlowSpecStoreWithTagFilter.SPECIFIC_GET_STATEMENT_BASE); }
    @Override
    protected String getTablelessGetAllStatement() { return replaceTagFilterValue(MysqlFlowSpecStoreWithTagFilter.SPECIFIC_GET_ALL_STATEMENT); }
    @Override
    protected String getTablelessGetBatchStatement() { return replaceTagFilterValue(MysqlFlowSpecStoreWithTagFilter.SPECIFIC_GET_SPECS_BATCH_STATEMENT); }
    @Override
    protected String getTablelessGetAllURIsStatement() { return replaceTagFilterValue(MysqlFlowSpecStoreWithTagFilter.SPECIFIC_GET_ALL_URIS_STATEMENT); }
    @Override
    protected String getTablelessGetSizeStatement() { return replaceTagFilterValue(MysqlFlowSpecStoreWithTagFilter.SPECIFIC_GET_SIZE_STATEMENT); }

    private String replaceTagFilterValue(String statement) {
      String tagFilterValue = String.join("','", tagFilters);
      return statement.replace("TAG_FILTER_REPLACE_STR", tagFilterValue);
    }
  }

  public MysqlFlowSpecStoreWithTagFilter(Config config, SpecSerDe specSerDe) throws IOException {
    super(config, specSerDe);
  }

  @Override
  public void addSpecImpl(Spec spec) throws IOException {
    this.addSpec(spec, DEFAULT_TAG_VALUE);
  }

  @Override
  protected String getConfigPrefix() {
    return MysqlFlowSpecStoreWithTagFilter.CONFIG_PREFIX;
  }

  @Override
  protected SqlStatements createSqlStatements(Config config) {
    String configPrefix = getConfigPrefix();
    if (config.hasPath(configPrefix)) {
      config = config.getConfig(configPrefix).withFallback(config);
    }
    String tagFilterCommaSeparated = config.hasPath(ConfigurationKeys.FLOW_SPEC_TAG_FILTER_KEY) ?
        config.getString(ConfigurationKeys.FLOW_SPEC_TAG_FILTER_KEY) : "";
    List<String> tagFilters = Arrays.asList(tagFilterCommaSeparated.split(","));
    return new TagFilterSpecificSqlStatements(tagFilters);
  }
}
