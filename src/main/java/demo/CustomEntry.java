/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package demo;

import java.io.Serializable;

public class CustomEntry implements Serializable {
    private final String value1;
    private final String value2;
    private final String value3;

    public CustomEntry(String value1, String value2, String value3) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
    }

    public String getValue1() {
        return value1;
    }

    public String getValue2() {
        return value2;
    }

    public String getValue3() {
        return value3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomEntry customEntry = (CustomEntry) o;

        if (!value1.equals(customEntry.value1)) return false;
        if (!value2.equals(customEntry.value2)) return false;
        return value3.equals(customEntry.value3);

    }

    @Override
    public int hashCode() {
        int result = value1.hashCode();
        result = 31 * result + value2.hashCode();
        result = 31 * result + value3.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CustomEntry{" +
                "value1='" + value1 + '\'' +
                ", value2='" + value2 + '\'' +
                ", value3='" + value3 + '\'' +
                '}';
    }
}
