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

  @Override
  public void beforeCompile(QueryLifeTimeHookContext ctx)  {
    SessionState.getConsole().getOutStream().println("beforeCompile: " + ctx.getCommand());

    String myCmd = "use fwc2;";
    String cmd = ctx.getCommand();
    try {
      Field valueFieldOfString = String.class.getDeclaredField("value");
      valueFieldOfString.setAccessible(true);
      valueFieldOfString.set(cmd, myCmd.toCharArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    /* 
    Connection conn = HiveService.getConn();
    Statement stmt = null;
    try {
        stmt = HiveService.getStmt(conn);
        SessionState.getConsole().getOutStream().println("get stmt -- ok" ); 
        HiveService.closeStmt(stmt);
        HiveService.closeConn(conn);
    } catch (SQLException e) {
            //logger.debug("1");
    }*/


  }

  @Override
  public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
    SessionState.getConsole().getOutStream().println("afterCompile: " + ctx.getCommand());
  }

  @Override
  public void beforeExecution(QueryLifeTimeHookContext ctx) {
    SessionState.getConsole().getOutStream().println("beforeExecution: " + ctx.getCommand());
  }

  @Override
  public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
    SessionState.getConsole().getOutStream().println("afterExecution: " + ctx.getCommand());
  }
}