/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.atomic.AtomicReference;

public class MovingPercentage {
    private final AtomicReference<Value> valueRef;
    private final long readingLifeTimeNanos;
    private final double lowToHighThreshold;
    private final double highToLowThreshold;

    public MovingPercentage(
        double initialPercentage,
        long readingLifeTimeNanos,
        double lowToHighThreshold,
        double highToLowThreshold) {
        this.valueRef = new AtomicReference<Value>(new Value(nanoTime(), initialPercentage));
        this.readingLifeTimeNanos = readingLifeTimeNanos;
        this.lowToHighThreshold = lowToHighThreshold;
        this.highToLowThreshold = highToLowThreshold;
    }

    public void recordHit() {
        record(nanoTime(), true);
    }

    public void recordMiss() {
        record(nanoTime(), false);
    }

    public void reset(double percentage) {
        this.valueRef.set(new Value(nanoTime(), percentage));
    }

    private void record(long timestampNanos, boolean conditionWasMet) {
        while (true) {
            Value current = valueRef.get();
            Value next = current.update(timestampNanos, conditionWasMet);
            if (valueRef.compareAndSet(current, next)) {
                break;
            }
        }
    }

    public boolean isHigh() {
        return valueRef.get().isHigh;
    }

    protected long nanoTime() {
        return System.nanoTime();
    }

    private class Value {
        private final long timestampNanos;
        private final double percentage;
        private final boolean isHigh;

        Value(long timestampNanos, double percentage) {
            this(timestampNanos, percentage, percentage > lowToHighThreshold);
        }

        private Value(long timestampNanos, double percentage, boolean isHigh) {
            this.timestampNanos = timestampNanos;
            this.percentage = percentage;
            this.isHigh = isHigh;
        }

        Value update(long newTimestampNanos, boolean conditionWasMet) {
            long delta = newTimestampNanos - timestampNanos;
            if (delta <= 0) {
                // Might happen during a reset, discard the measurement
                return this;
            }
            double alpha = 1 - Math.exp(-delta / readingLifeTimeNanos);
            double newPercentage = (conditionWasMet ? alpha : 0) + (1 - alpha) * percentage;
            boolean newHigh;
            if (newPercentage < highToLowThreshold) {
                newHigh = false;
            } else if (newPercentage > lowToHighThreshold) {
                newHigh = true;
            } else { // keep current trend
                newHigh = isHigh;
            }
            return new Value(newTimestampNanos, newPercentage, newHigh);
        }
    }

}
