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
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getName(), getLockName(threadId)) + ":rwlock_timeout"; 
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);
        // eg: RLock lock = redisson.getLock("anyLock");
        // KEYS[1]：表示锁的名称，即 anyLock
        // KEYS[2]：表示记录加锁时的超时记录，即 {anyLock}:UUID:线程ID:rwlock_timeout
        // ARGV[1]：表示锁的过期时间，即 internalLockLeaseTime
        // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID
        // ARGV[3]：表示写锁的名称，即 UUID:线程ID:write
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +  // ① 客户端A（UUID_01:threadId_01）来加读锁， hget anyLock mode，相当于是从anyLock这个hash里面获取一个mode作为key的值。但是此时刚开始你都没有加锁呢，所以这里的mode肯定是空的，此时就是直接加一个读锁
                                "if (mode == false) then " +
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +     // ① hset anyLock mode read
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +     // ① hset anyLock UUID_01:threadId_01 1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +       // ① set {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 1
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +  // ① pexpire {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 30000
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +        // ① pexpire anyLock 30000
                                  "return nil; " +                                     // ① 返回加锁成功
                                "end; " +
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +  // ② 客户端B（UUID_02:threadId_02）来加读锁,mode为read
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +          // ② hincrby anyLock UUID_02:threadId_02 1
                                  "local key = KEYS[2] .. ':' .. ind;" +            // ② key = {anyLock}:UUID_02:threadId_02:rwlock_timeout:1
                                  "redis.call('set', key, 1); " +                   // ② set  {anyLock}:UUID_02:threadId_02:rwlock_timeout:1 1
                                  "redis.call('pexpire', key, ARGV[1]); " +         // ② pexpire {anyLock}:UUID_02:threadId_02:rwlock_timeout:1 30000
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +     // ② pexpire anyLock 30000
                                  "return nil; " +                                  // ② 返回加锁成功
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",              // ③ 客户端C 来加写锁，已经有人加了读锁，但没有加写锁，此时会直接执行 pttl anyLock，返回一个anyLock的剩余生存时间
                        Arrays.<Object>asList(getName(), getReadWriteTimeoutNamePrefix(threadId)), 
                        internalLockLeaseTime, getLockName(threadId), getWriteLockName(threadId));

        /*
         * 客户端A及客户端B加了读锁之后的数据结构如下所示：
         * anyLock: {
         *   “mode”: “read”,
         *   “UUID_01:threadId_01”: 1,
         *   “UUID_02:threadId_02”: 1
         * }
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         * {anyLock}:UUID_02:threadId_02:rwlock_timeout:1		1
         * 多个客户端，同时加读锁，读锁与读锁是不互斥的，只会让你不断的在hash里加入哪个客户端也加了一个读锁
         * 每个客户端都会维持一个watchdog，不断的刷新anyLock的生存时间，同时也会刷新那个客户端自己对应的timeout key的生存时间
         *
         * 1.先读锁后写锁如何互斥
         * 客户端C（UUID_03:threadId_03），来加写锁，由于已经有人加了读锁，但没有加写锁，会直接返回锁的ttl，客户端C加锁失败，
         * 时此客户端C就会不断地尝试加锁，陷入一个死循环，除非原先加读锁的人释放了读锁，客户端C的写锁才有可能加上去
         *
         * 2.先有写锁后读锁如何互斥
         * 假设客户端A先加了一个写锁
         * anyLock: {
         *   "mode": "write",
         *   "UUID_01:threadId_01:write": 1
         * }
         * 假设此时客户端B来加读锁
         * 会调用 hexists anyLock UUID_02:threadId_02:write 指定看是否为threadId_02加的写锁，显示不是的，加写锁的是客户端A，所以这里的条件不成立
         * 返回pttl anyLock，导致加锁失败，不断的陷入死循环不断的重试
         */
    }


    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        // eg: RLock lock = redisson.getLock("anyLock");
        // KEYS[1]：表示锁的名称，即 anyLock
        // KEYS[2]：发布订阅模式下的通道，即 redisson_rwlock:{anyLock}
        // KEYS[3]：表示记录加锁时的超时记录，即 {anyLock}:UUID:线程ID:rwlock_timeout
        // KEYS[4]：表示记录加锁时的超时记录的前缀，即：{anyLock}
        // ARGV[1]：表示释放锁时发布的消息内容，即 0
        // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +  // 获取加锁的模式
                "if (mode == false) then " +        // 非读写锁，发布消息通知
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +      // 返回1
                "end; " +
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +  // 判断当前线程是否有加锁，如果没有加锁，返回nil
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +  // 当前线程有加锁，锁的加锁次数-1
                "if (counter == 0) then " +     // 如果加锁次数已变为0，删除名为UUID:线程ID的key
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +  // 删除名为{anyLock}:UUID:线程ID:rwlock_timeout:counter的key
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +  // 判断anyLock hash结构中key的数量，如果大于1
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " +  // 如果anyLock hash结构中所有的key
                    "for n, key in ipairs(keys) do " +      // 循环所有的key
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " +  // 获取key对应的值
                        "if type(counter) == 'number' then " +  // 如果为数字类型
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" +   // 获取rwlock_timeout最大的剩余时间
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    "if maxRemainTime > 0 then " +   // 如果rwlock_timeout最大的剩余时间大于0
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +  // 设置锁的ttl为rwlock_timeout中的最大剩余时间
                        "return 0; " +  // 返回0
                    "end;" + 
                        
                    "if mode == 'write' then " +  // 如果加的为写锁，读锁是释放不了的，直接返回0
                        "return 0;" +   // 返回0
                    "end; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +    // 删除锁
                "redis.call('publish', KEYS[2], ARGV[1]); " + // 发布通知
                "return 1; ",  // 返回1, 表示锁释放成功
                Arrays.<Object>asList(getName(), getChannelName(), timeoutPrefix, keyPrefix), 
                LockPubSub.unlockMessage, getLockName(threadId));


        /*
         * 1）不同客户端加了读锁 / 同一个客户端/线程多次可重入加了读锁
         *
         * anyLock: {
         *   “mode”: “read”,
         *   “UUID_01:threadId_01”: 2,
         *   “UUID_02:threadId_02”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:2		1
         * {anyLock}:UUID_02:threadId_02:rwlock_timeout:1		1
         *
         *
         * 我们现在来看一下，针对上述情况，读锁是如何释放的呢？
         *
         * KEYS[1] = anyLock
         * KEYS[2] = redisson_rwlock:{anyLock}
         * KEYS[3] = {anyLock}:UUID_01:threadId_01:rwlock_timeout
         * KEYS[4] = {anyLock}
         *
         * ARGV[1] = 0
         * ARGV[2] = UUID_01:threadId_01
         *
         * hget anyLock mode，mode = read
         *
         * hexists anyLock UUID_01:threadId_01，肯定是存在的，因为这个客户端A加过读锁
         * hincrby anyLock UUID_01:threadId_01 -1，将这个客户端对应的加锁次数递减1，现在就是变成1，counter = 1
         * del {anyLock}:UUID_01:threadId_01:rwlock_timeout:2，删除了一个timeout key
         *
         *
         * anyLock: {
         *   “mode”: “read”,
         *   “UUID_01:threadId_01”: 1,
         *   “UUID_02:threadId_02”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         * {anyLock}:UUID_02:threadId_02:rwlock_timeout:1		1
         *
         * hlen anyLock > 1，就是hash里面的元素超过1个
         *
         * pttl {anyLock}:UUID_01:threadId_01:rwlock_timeout:1，此时获取那个timeout key的剩余生存时间还有多少毫秒，比如说此时这个key的剩余生存时间是20000毫秒
         *
         * 其实是获取到了所有的timeout key的最大的一个剩余生存时间，假设最大的剩余生存时间是25000毫秒
         *
         * pexpire anyLock 25000
         *
         * 客户端A再来释放一次读锁
         *
         * hincrby anyLock UUID_01:threadId_01 -1，将这个客户端对应的加锁次数递减1，现在就是变成1，counter = 0
         * hdel anyLock UUID_01:threadId_01，此时就是从hash数据结构中删除客户端A这个加锁的记录
         * del {anyLock}:UUID_01:threadId_01:rwlock_timeout:1，删除了一个timeout key
         *
         * anyLock: {
         *   “mode”: “read”,
         *   “UUID_02:threadId_02”: 1
         * }
         *
         * {anyLock}:UUID_02:threadId_02:rwlock_timeout:1		1
         *
         * 会用timeout key的剩余生存时间刷新一下anyLock的生存时间，pexpire
         *
         * 客户端B再来释放一次读锁
         *
         * hincrby anyLock UUID_02:threadId_02 -1，将这个客户端对应的加锁次数递减1，现在就是变成1，counter = 0
         * hdel anyLock UUID_02:threadId_02，此时就是从hash数据结构中删除客户端A这个加锁的记录
         * del {anyLock}:UUID_02:threadId_02:rwlock_timeout:1，删除了一个timeout key
         *
         * anyLock: {
         *   “mode”: “read”
         * }
         *
         * hlen anyLock = 1
         *
         * del anyLock，当没有人再持有这个锁的读锁的时候，此时会识别出来，就会彻底删除这个读锁，整个读锁释放的全过程就是这样的
         *
         * （2）同一个客户端/线程先加写锁再加读锁
         *
         * anyLock: {
         *   “mode”: “write”,
         *   “UUID_01:threadId_01:write”: 1,
         *   “UUID_01:threadId_01”: 1
         * }
         *
         * {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
         *
         * 针对第二种情况，同一个客户端先加写锁，然后加读锁的情况
         *
         * hincrby anyLock UUID_01:threadId_01 -1，将这个客户端对应的加锁次数递减1，现在就是变成1，counter = 0
         * hdel anyLock UUID_01:threadId_01，此时就是从hash数据结构中删除客户端A这个加锁的记录
         * del {anyLock}:UUID_01:threadId_01:rwlock_timeout:1，删除了一个timeout key
         *
         * anyLock: {
         *   “mode”: “write”,
         *   “UUID_01:threadId_01:write”: 1
         * }
         *
         * mode是write的话，返回一个值是0就可以了
         */
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +   // 更新锁key的ttl
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " +  // 更新每一个读锁timeout的ttl
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getName(), keyPrefix), 
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.HGET, getName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
