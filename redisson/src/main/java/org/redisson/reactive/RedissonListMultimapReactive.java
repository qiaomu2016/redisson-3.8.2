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
package org.redisson.reactive;

import java.util.List;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.redisson.RedissonListMultimap;
import org.redisson.api.RList;
import org.redisson.api.RListMultimap;
import org.redisson.api.RListReactive;
import org.redisson.client.codec.Codec;
import org.redisson.command.CommandReactiveExecutor;

/**
 * 
 * @author Nikita Koksharov
 *
 * @param <K> key type
 * @param <V> value type
 */
public class RedissonListMultimapReactive<K, V> {

    private CommandReactiveExecutor commandExecutor;
    private RedissonListMultimap<K, V> instance;
    
    public RedissonListMultimapReactive(CommandReactiveExecutor commandExecutor, String name) {
        this.instance = new RedissonListMultimap<K, V>(commandExecutor, name);
    }

    public RedissonListMultimapReactive(Codec codec, CommandReactiveExecutor commandExecutor, String name) {
        this.instance = new RedissonListMultimap<K, V>(codec, commandExecutor, name);
    }

    public RListReactive<V> get(K key) {
        RList<V> list = ((RListMultimap<K, V>)instance).get(key);
        return ReactiveProxyBuilder.create(commandExecutor, instance, 
                new RedissonListReactive<V>(instance.getCodec(), commandExecutor, list.getName()), RListReactive.class);
    }

            }
