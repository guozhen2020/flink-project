/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.guozhen;

import com.github.guozhen.config.Config;
import com.github.guozhen.config.Parameters;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.guozhen.config.Parameters.BOOL_PARAMS;
import static com.github.guozhen.config.Parameters.INT_PARAMS;
import static com.github.guozhen.config.Parameters.STRING_PARAMS;


public class Main {

  public static void main(String[] args) throws Exception {
    Logger logger = LoggerFactory.getLogger("flink-cdc-iceberg");
//    ParameterTool tool = ParameterTool.fromArgs(args);

    ParameterTool tool = ParameterTool.fromPropertiesFile(Main.class.getResourceAsStream("/app-config.properties"));
    Parameters inputParams = new Parameters(tool);
    Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
    AppExecutor executor = new AppExecutor(config);
    executor.run();
  }


}
