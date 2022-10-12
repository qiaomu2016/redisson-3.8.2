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
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);
        // eg: RLock lock = redisson.getLock("anyLock");
        // KEYS[1]：表示锁的名称，即 anyLock
        // ARGV[1]：表示锁的过期时间，即 internalLockLeaseTime
        // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID:write
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +    // hget anyLock mode 获取加锁的模式
                            "if (mode == false) then " +                                    // 还未加锁，添加写锁
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +        // hset anyLock mode write  设置anyLock hash结构中key为mode的值为write
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +             // hset anyLock UUID_01:threadId_01:write 1  设置anyLock hash结构中key为UUID_01:threadId_01:write的值为1
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +             // pexpire anyLock 30000    设置锁anyLock的持有时间为30000
                                  "return nil; " +                                          // 返回nil,表示加锁成功
                              "end; " +
                              "if (mode == 'write') then " +                                // 如果已经加了写锁
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +   // 判断是否为同一个线程加的写锁，如果是同一个线程加的写锁，表示为重入锁，写锁计数+1，同时写锁的持有时间+internalLockLeaseTime
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +          // 返回nil,表示加锁成功
                                  "end; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",     // 非同一个线程加的写锁，加锁失败，返回ttl（写锁与写锁互斥）
                        Arrays.<Object>asList(getName()),
                        internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +   // 获取加锁的模式
                "if (mode == false) then " +    // 非读写锁，发布消息通知
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +  // 返回1
                "end;" +
                "if (mode == 'write') then " +  // 写锁
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +  // 判断当前线程是否有加锁，如果没有加锁，返回nil
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    "else " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +   // 当前线程有加锁，锁的加锁次数-1
                        "if (counter > 0) then " +      // 加锁次数不为0，仍需持续持有锁，更新锁的ttl
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +  // 如果加锁次数已变为0，删除名为UUID:线程ID的key
                            "if (redis.call('hlen', KEYS[1]) == 1) then " + // 判断anyLock hash结构中key的数量，如果为1，即只存在mode这一个元素，释放锁
                                "redis.call('del', KEYS[1]); " +
                                "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "else " +   // 如果anyLock hash结构中key的数量不为1，表示还存在未解锁的读锁，把mode置为读锁模式
                                // has unlocked read-locks
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+   // 返回1，释放锁成功
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
        Arrays.<Object>asList(getName(), getChannelName()),
        LockPubSub.readUnlockMessage, internalLockLeaseTime, getLockName(threadId));

        /*
         * 同一个客户端多次可重入加写锁 / 同一个客户端先加写锁再加读锁
         *
         * anyLock: {
         *   “mode”: “write”,
         *   “UUID_01:threadId_01:write”: 2,
         *   “UUID_01:threadId_01”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         *
         * 释放写锁
         *
         * KEYS[1] = anyLock
         * KEYS[2] = redisson_rwlock:{anyLock}
         *
         * ARGV[1] = 0
         * ARGV[2] = 30000
         * ARGV[3] = UUID_01:threadId_01:write
         *
         * mode = write
         *
         * hdel anyLock UUID_01:threadId_01:write
         *
         * anyLock: {
         *   “mode”: “write”,
         *   “UUID_01:threadId_01”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         *
         * hset anyLock mode read，将写锁转换为读锁
         *
         * anyLock: {
         *   “mode”: “read”,
         *   “UUID_01:threadId_01”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         *
         */

    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.readUnlockMessage);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.HGET, getName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
