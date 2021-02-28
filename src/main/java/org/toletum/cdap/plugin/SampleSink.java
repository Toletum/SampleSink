/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.toletum.cdap.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("SampleSink")
@Description("Consume all input records and push to log a Sample")
public class SampleSink extends SparkSink<StructuredRecord> {
  private final SampleSinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(SampleSink.class);


  public SampleSink(SampleSinkConfig config) {
    this.config = config;
  }

  /**
   * @param context of runtime for this plugin.
   */
  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
  }

  /**
   */
  @Override
  public void run(SparkExecutionPluginContext sparkExecutionPluginContext,
                  JavaRDD<StructuredRecord> javaRDD) throws Exception {
    StructuredRecord record;
    Iterator<Schema.Field> fieldIter;

    List data = javaRDD.take(config.getSampleNumber());

    for(int i = 0; i < data.size(); i++) {
      record = (StructuredRecord)data.get(i);

      LOG.info("{} -> {}", config.getSampleID(), StructuredRecordStringConverter.toDelimitedString(record," - "));
    }


  }



  /**
   * Config properties for the plugin.
   */
  public static final class SampleSinkConfig extends PluginConfig {
    @Name("sampleID")
    @Description("ID")
    @Macro
    public String sampleID;

    @Name("sampleNumber")
    @Description("Number of sample records.")
    @Macro
    public Integer sampleNumber;

    public SampleSinkConfig(String sampleID, Integer sampleNumber) {
      this.sampleID = sampleID;
      this.sampleNumber = sampleNumber;
    }

    public String getSampleID() {
      return sampleID;
    }

    public Integer getSampleNumber() {
      return sampleNumber;
    }

  }
}
