/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.util.LegacyHadoopConfigurationSource;

/**
 * Configuration source based on existing Hadoop Configuration.
 */
public class CompatibleOzoneConfiguration extends
    LegacyHadoopConfigurationSource {

  public CompatibleOzoneConfiguration(
      Configuration configuration) {
    super(configuration);
  }

  public CompatibleOzoneConfiguration() {
    super(initConfiguration());
  }

  private static Configuration initConfiguration() {
    activate();
    return loadOzoneConfiguration(new Configuration());
  }

  public static Configuration createConfiguration() {
    return loadOzoneConfiguration(new Configuration());
  }

  private static Configuration loadOzoneConfiguration(
      Configuration configuration) {
    try {
      //there could be multiple ozone-default-generated.xml files on the
      // classpath, which are generated by the annotation processor.
      // Here we add all of them to the list of the available configuration.
      Enumeration<URL> generatedDefaults =
          CompatibleOzoneConfiguration.class.getClassLoader().getResources(
              "ozone-default-generated.xml");
      while (generatedDefaults.hasMoreElements()) {
        configuration.addResource(generatedDefaults.nextElement());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    configuration.addResource("ozone-site.xml");
    return configuration;
  }

  public static CompatibleOzoneConfiguration of(Configuration hadoopConf) {
    return new CompatibleOzoneConfiguration(loadOzoneConfiguration(hadoopConf));
  }

  public static void activate() {
    // adds the default resources
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("ozone-default.xml");
  }
}
