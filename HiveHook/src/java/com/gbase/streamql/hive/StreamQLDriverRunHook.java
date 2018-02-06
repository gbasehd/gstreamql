/**
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

//package org.apache.hadoop.hive.ql.hooks;
package com.gbase.streamql.hive;

import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import java.lang.reflect.Field;

public class StreamQLDriverRunHook implements HiveDriverRunHook {

    //@Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {

        SessionState.getConsole().getOutStream().println("change "+ hookContext.getCommand());

        String cmd = hookContext.getCommand();
        String myCmd = "use fwc";
        Field valueFieldOfString = String.class.getDeclaredField("value");
        valueFieldOfString.setAccessible(true);
        valueFieldOfString.set(cmd,myCmd.toCharArray());

        SessionState.getConsole().getOutStream().println("to " + hookContext.getCommand());
    }

    //@Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
        // do nothing
    }
}