/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.spring.data.connection;

import org.redisson.client.protocol.convertor.SingleConvertor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class DistanceConvertor extends SingleConvertor<Distance> {

    private final Metric metric;
    
    public DistanceConvertor(Metric metric) {
        super();
        this.metric = metric;
    }

    @Override
    public Distance convert(Object obj) {
        return new Distance((Double)obj, metric);
    }

}
