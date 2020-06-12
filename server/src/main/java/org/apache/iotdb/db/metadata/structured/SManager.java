/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.metadata.structured;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.tools.corba.se.idl.InvalidArgument;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class takes responsibility of Managing all known and relevant structured-type information.
 */
public class SManager {

    private static final Logger logger = LoggerFactory.getLogger(SManager.class);

    private static final SManager INSTANCE = new SManager();

    private final Map<String, StructuredType> registeredTypes;

    public static SManager getInstance() {
        return SManager.INSTANCE;
    }

    public SManager() {
        registeredTypes = new HashMap<>();
    }

    public void register(String name, StructuredType type) {
        this.registeredTypes.put(name, type);
    }

    /**
     * Removes all structured type information and
     * transforms the plan in a primitive plan
     * that is handled downstream without any changes.
     */
    public InsertPlan translate(InsertPlan plan) {
        for (TSDataType type : plan.getTypes()) {
            if (type != null) {
                throw new IllegalArgumentException("Plan already has type information");
            }
        }
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> newValues = new ArrayList<>();
        // First, we check if it contains a structured type
        Object[] values = plan.getValues();
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value instanceof String && ((String) value).contains("::")) {
                String str = StringUtils.strip(((String) value), "\"");
                str = StringEscapeUtils.unescapeJava(str);
                logger.info("Contains structured type for measurement '{}', rewriting...", plan.getMeasurements()[i]);
                // Now first parse the thing
                String jsonString = str.substring(0, str.indexOf("::"));
                String structName = str.substring(str.indexOf("::") + 2);

                // Lookup the type
                if (!this.registeredTypes.containsKey(structName)) {
                    throw new IllegalArgumentException("Plan references the Unknown Type '" + structName + "'!");
                }

                StructuredType type = this.registeredTypes.get(structName);

                // Now we have all type information and can use the type to get all elements
                if (type.isMap()) {

                    JSONObject map = JSON.parseObject(jsonString);

                    for (String key : type.getKeySet()) {
                        if (!(type.getChild(key) instanceof PrimitiveType)) {
                            throw new NotImplementedException("No nesting is supported currently!");
                        }
                        // Fetch the value
                        if (!map.containsKey(key)) {
                            throw new IllegalArgumentException("Value String misses the requested field '" + key + "'");
                        }
                        measurements.add(plan.getMeasurements()[i] + "." + key);
                        TSDataType primitiveType = type.getChild(key).getPrimitiveType();
                        types.add(primitiveType);
                        try {
                            newValues.add(CommonUtils.parseValue(primitiveType, map.getString(key)));
                        } catch (QueryProcessException e) {
                            throw new IllegalArgumentException("Error parsing given structured Object", e);
                        }
                    }
                } else {
                    throw new NotImplementedException("Only type Map is supported currently!");
                }
            } else {
                // Do inference here
                measurements.add(plan.getMeasurements()[i]);
                types.add(plan.getTypes()[i]);
                newValues.add(value);
            }
        }
        return new InsertPlan(plan.getDeviceId(), plan.getTime(), measurements.toArray(new String[0]), types.toArray(new TSDataType[0]), newValues.toArray(new Object[0]));
    }


}
