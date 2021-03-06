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

package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import java.lang.reflect.Field;

public class StreamQLQueryLifeTimeHook implements QueryLifeTimeHook {
  private Utility util = new Utility();

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx)  {
    util.Logger("\nbeforeCompile: " + ctx.getCommand());
  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
    util.Logger("\nafterCompile: " + ctx.getCommand());
  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {
    util.Logger("\nbeforeExecution: " + ctx.getCommand());
  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    util.Logger("\nafterExecution: " + ctx.getCommand());
  }
}
