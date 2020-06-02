/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.types;

import java.util.Objects;


public final class TimeTZ implements Comparable<TimeTZ> {

    private final long time;
    private final int secondsFromUTC;

    public TimeTZ(long time) {
        this(time, 0);
    }

    public TimeTZ(long time, int secondsFromUTC) {
        this.time = time;
        this.secondsFromUTC = secondsFromUTC;
    }

    public long getTime() {
        return time;
    }

    public int getSecondsFromUTC() {
        return secondsFromUTC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || false == o instanceof TimeTZ) {
            return false;
        }
        TimeTZ that = (TimeTZ) o;
        return time == that.time && secondsFromUTC == that.secondsFromUTC;
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, secondsFromUTC);
    }

    @Override
    public String toString() {
        return TimeTZType.formatTime(this);
    }

    @Override
    public int compareTo(TimeTZ that) {
        return Long.compare(time, that.time);
    }
}