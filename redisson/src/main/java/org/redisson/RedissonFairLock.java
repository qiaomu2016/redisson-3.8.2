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
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * 可重入锁，公平锁
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 * 通过公平锁，可以保证说，客户端获取锁的顺序，就跟他们请求获取锁的顺序，是一样的，公平锁，排队，谁先申请获取这把锁，谁就可以先获取到这把锁，这个是按照顺序来的，不是胡乱争抢的
 * 因此会把各个客户端对加锁的请求进行排队处理，保证说先申请获取锁的，就先可以得到这把锁，实现所谓的公平性
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime = 5000;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;

    protected RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected RedissonLockEntry getEntry(long threadId) {
        return PUBSUB.getEntry(getEntryName() + ":" + threadId);
    }

    @Override
    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return PUBSUB.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId), commandExecutor.getConnectionManager().getSubscribeService());
    }

    @Override
    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        PUBSUB.unsubscribe(future.getNow(), getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId), commandExecutor.getConnectionManager().getSubscribeService());
    }

    @Override
    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                    "local firstThreadId = redis.call('lindex', KEYS[1], 0); " +
                    "if firstThreadId == ARGV[1] then " +
                        "local keys = redis.call('zrange', KEYS[2], 0, -1); " +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), keys[i]);" +
                        "end;" +
                    "end;" +
                    "redis.call('zrem', KEYS[2], ARGV[1]); " +
                    "redis.call('lrem', KEYS[1], 0, ARGV[1]); ",
                    Arrays.<Object>asList(threadsQueueName, timeoutSetName),
                    getLockName(threadId), threadWaitTime);
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            // eg: RLock lock = redisson.getLock("pms:product:1000001");
            // KEYS[1]：表示锁的名称，即 pms:product:1000001
            // KEYS[2]：基于redis的数据结构实现的一个队列，即 redisson_lock_queue:{pms:product:1000001}
            // KEYS[3]：基于redis的数据结构实现的一个Set数据集合（有序集合），可以自动按照你给每个数据指定的一个分数（score）来进行排序，
            //          即 redisson_lock_timeout:{pms:product:1000001}
            // ARGV[1]：表示锁的过期时间，即 internalLockLeaseTime
            // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID
            // ARGV[3]：表示当前时间戳
            return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do "
                    + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                    + "if firstThreadId2 == false then "
                        + "break;"
                    + "end; "
                    + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                    + "if timeout <= tonumber(ARGV[3]) then "
                        + "redis.call('zrem', KEYS[3], firstThreadId2); "
                        + "redis.call('lpop', KEYS[2]); "
                    + "else "
                        + "break;"
                    + "end; "
                  + "end;"
                    +

                    "if (redis.call('exists', KEYS[1]) == 0) and ((redis.call('exists', KEYS[2]) == 0) "
                            + "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            "redis.call('lpop', KEYS[2]); " +
                            "redis.call('zrem', KEYS[3], ARGV[2]); " +
                            "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "return 1;",
                    Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                    internalLockLeaseTime, getLockName(threadId), currentTime);
        }

        if (command == RedisCommands.EVAL_LONG) {
            // eg: RLock lock = redisson.getLock("pms:product:1000001");
            // KEYS[1]：表示锁的名称，即 pms:product:1000001
            // KEYS[2]：基于redis的数据结构实现的一个队列，即 redisson_lock_queue:{pms:product:1000001}
            // KEYS[3]：基于redis的数据结构实现的一个Set数据集合（有序集合），可以自动按照你给每个数据指定的一个分数（score）来进行排序，
            //          即 redisson_lock_timeout:{pms:product:1000001}
            // ARGV[1]：表示锁的过期时间，即 internalLockLeaseTime
            // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID
            // ARGV[3]：表示当前时间戳+等待时间
            // ARGV[4]：表示当前时间戳
            return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do "
                    + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"    // ①② 获取队列中第一个元素  ③ 第三个线程进来，获取队列中的第一个元素，UUID_02:threadId_02  ④ 第一个线程重入
                    + "if firstThreadId2 == false then "    // ① 刚开始，队列是空的，所以什么都获取不到   ② 队列仍然是空的
                        + "break;"                          // ①② 直接退出while true死循环
                    + "end; "
                    + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"    // ③④ 从有序集合中获取UUID_02:threadId_02对应的分数
                    + "if timeout <= tonumber(ARGV[4]) then "    // 如果当前线程加锁的时间点 大于等于 上一个线程的超时等待时间
                        + "redis.call('zrem', KEYS[3], firstThreadId2); "   // 从set集合中删除上一个线程
                        + "redis.call('lpop', KEYS[2]); "                   // 同时从队列中弹出
                    + "else "           // ③④ 如果第三个线程UUID_03:threadId_03线程加锁的时间点 小于 第二个线程加锁的超时时间
                        + "break;"      // ③④ 直接退出while true死循环
                    + "end; "
                  + "end;"

                      + "if (redis.call('exists', KEYS[1]) == 0) and ((redis.call('exists', KEYS[2]) == 0) "    // ① 如果当前锁key不存在（还没有其他端加锁），并且
                            + "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +   // 队列不存在 或者 队列中的第一个元素是当前线程 （第一个线程进来满足条件）
                            "redis.call('lpop', KEYS[2]); " +                               // 弹出队列的第一个元素
                            "redis.call('zrem', KEYS[3], ARGV[2]); " +                      // 从set集合中删除threadId对应的元素
                            "redis.call('hset', KEYS[1], ARGV[2], 1); " +                   // 加锁 hset pms:product:1000001 UUID:thread_01 1
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +                   // 设置 锁的持有时间
                            "return nil; " +                                                // ① 返回 nil，表示加锁成功
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +         // ② 第二个线程进来，锁key肯定存在，接下来 判断 UUID_02:threadId_02是否存在，当然不存在  ④ 判断 UUID:threadId是否存在，当然存在
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +                // ④ UUID:threadId锁的次数+1
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +                   // ④ 更新锁的持有时间
                            "return nil; " +                                                // ④ 返回nil，表示可重入加锁成功
                        "end; " +

                        "local firstThreadId = redis.call('lindex', KEYS[2], 0); " +        // ② 从队列中获取第一个元素，此时队列是空的  ③ 从队列中获取第一个元素 UUID_02:threadId_02
                        "local ttl; " +
                        "if firstThreadId ~= false and firstThreadId ~= ARGV[2] then " +    // ③ UUID_02:threadId_02 不等于 UUID_03:threadId_03
                            "ttl = tonumber(redis.call('zscore', KEYS[3], firstThreadId)) - tonumber(ARGV[4]);" +   // ③ ttl 设置为 set集合元素UUID_02:threadId_02的分数 - currentTime
                        "else "
                          + "ttl = redis.call('pttl', KEYS[1]);" +                         // ② 获取锁key的ttl
                        "end; " +

                        "local timeout = ttl + tonumber(ARGV[3]);" +                       // ②③ 超时时间设置为 ttl + currentTime + threadWaitTime
                        "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +    // ② 往set集合中插入一个元素，元素的值是UUID_02:threadId_02，他对应的分数是timeout（时间越靠后，时间戳就越大），sorted set会自动根据你插入的元素的分数从小到大来进行排序  ③ 往set集合中插入一个元素，元素的值是UUID_03:threadId_03
                            "redis.call('rpush', KEYS[2], ARGV[2]);" +                     // ② 将UUID_02:threadId_02，插入到队列的头部   ③ 将UUID_03:threadId_03 插入 UUID_02:threadId_02的后面
                        "end; " +
                        "return ttl;",                                                     // ② 返回锁key的剩余有效时间
                        Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                                    internalLockLeaseTime, getLockName(threadId), currentTime + threadWaitTime, currentTime);
        }

        /*
         * 【假设】
         * 当前时间 10:00:00， internalLockLeaseTime 为 30000毫秒
         * 客户端A加锁成功：
         * 客户端B同时过来加锁： timeout = 30000毫秒 + 10:00:00 + 5000毫秒 = 10:00:35
         * 客户端C 10:00:05 过来加锁： timeout = 30000毫秒（10:00:35-10:00:05） + 10:00:05 + 5000毫秒 = 10:00:40
         *
         * 【排队分数刷新】
         * 客户端B和客户端C都是处于排队的状态，会进入while ture循环，每次等待一段时间之后，就会重新来尝试进行加锁
         * 客户端C，10:00:15 再次尝试进行加锁，但是客户端A此时还是持有着这把锁的，此时会出现什么情况呢？
         * while true，获取队列第一个元素，同时获取这个元素的timeout时间，10:00:35 <= 10:00:15？不成立，while死循环退出
         * 判断一下是否没人加锁，以及判断是否是客户端C加的锁，都不成立
         * 获取队列第一个元素, 由于客户端B再次尝试加锁，第一个排队是元素是客户端B，因此ttl为锁的剩余时间（其中ttl会自动续期，在10:00:10分时会续期） = 30000毫秒-5000毫秒 = 25000毫秒  ，此时 timeout = 25000毫秒 + 10:00:15 + 5000毫秒 = 10:00:45
         * zadd redisson_lock_timeout:{pms:product:1000001} 10:00:45 UUID_02:threadId_02，刷新一下有序集合中的元素的分数，10:00:45
         *
         * 大致可以理解为，由于某个客户端在不断的重试尝试加锁？每次重试尝试加锁的时候，就判定为是这个客户端一次新的尝试加锁的行为，
         * 此时会将这个客户端对应的timeout时间往后延长，10:00:35，这次他重试加锁之后，timeout时间算出来就变成10:00:45，把这个客户端的timeout时间延长了，有序集合中的分数变大了
         *
         * zadd指令的返回值，如果是第一次往里面怼入一个元素，返回值是什么？如果是第二次刷新这个元素的分数，返回值是什么？
         * 如果是第一次插入一个元素在有序集合中，此时就会返回值是1，同时会入队；
         * 但是后续不断的尝试加锁的时候，其实是会不断的刷新这个有序集合中的元素的分数，越来越大，但是每次刷新分数，返回值是0，所以不会重复的往队列中插入这个元素的。
         *
         * 通过这种机制，队列里面仍然还是维持和之前一样的顺序，客户端B的分数会刷大，客户端C的分数其实在尝试加锁时也会在不断的刷大，所以在有序集合中的分数的大小的顺序，基本上还是按照的是最初他们是如何排队的，此时也会如何排队
         *
         * 【队列重排】
         * 时间到达10:00:50，客户端C重新尝试进行加锁，此时客户端B可能因为网络原因或者是别的什么原因，可能就没有尝试过重新加锁
         * 进入while true，拿到队列第一个元素的timeout时间，10:00:35 <= 10:00:50，条件成立
         * zrem redisson_lock_timeout:{pms:product:1000001} UUID_02:threadId_02，就从有序集合中将客户端B移除了
         * lpop redisson_lock_queue:{pms:product:1000001}，就从队列中将客户端B也移除了
         * 此时队列的第一个元素是客户端C，他的timeout时间是10:00:40 <= 10:00:50，条件成立，从有序集合以及队列中删除掉客户端C
         *
         * 客户端C尝试进行加锁，不成立，因为客户端A还持有着那把锁，客户端C重新排队入队
         * 假设此时，在10:00:55，客户端B再次尝试来进行加锁，while true退出，尝试加锁，不成立，重新尝试入队
         *
         * 从这个里面，我们可以看到，在一个客户端刚刚加锁之后，其他的客户端来争抢这把锁，刚开始在一定时间范围之内，时间不要过长，各个客户端是可以按照公平的节奏，在队列和有序集合里面进行排序，
         * 其实队列里的元素顺序是不会改变的，各个客户端重新尝试加锁，只不过是刷新有序集合中的分数（timeout），各个客户端的timeout不断加长，但是整体顺序大致还是保持一致的
         * 但是如果客户端A持有的锁的时间过长，timeout，这个所谓的排队是有timeout，可能会在while true死循环中将一些等待时间过长的客户端从队列和有序集合中删除，一旦删除过后，
         * 就会发生各个客户端随着自己重新尝试加锁的时间次序，重新进行一个队列中的重排，也就是排队的顺序可能会发生变化。
         * 客户端跟redis通信的网络的一个问题，延迟，各种情况都可能会发生。
         *
         * 【按顺序依次加锁】
         * 客户端释放锁，释放锁之后队列中的排队的客户端是如何依次获取这把锁的，是按照队列里的顺序去获取锁的
         * 客户端A释放锁后，假如客户端C排在队头，客户端B先拉尝试加锁
         * 由于客户端B不在队头，无法加锁，zadd指令，刷新一下客户端B在有序集合中的timeout分数
         * 因此，哪怕是锁释放掉了，其他各个客户端来尝试重新加锁也是不行的，因为此时排在队头的不是这个客户端也不行，此时只会重新计算timeout分数刷新一下有序集合中的timeout分数罢了
         * 此时客户端C来尝试加锁会如何？锁key不存在的；队列是存在的；队列的队头就是客户端C，所以此时加锁的条件成立了，进入加锁的逻辑

         */

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"

              + "if (redis.call('exists', KEYS[1]) == 0) then " +
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +

                "redis.call('del', KEYS[1]); " +
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
                "if nextThreadId ~= false then " +
                    "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(threadId), System.currentTimeMillis());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return commandExecutor.writeAsync(getName(), RedisCommands.DEL_OBJECTS, getName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpire', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpire', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timeUnit.toMillis(timeToLive));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpireat', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpireat', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timestamp);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('persist', KEYS[1]); " +
                        "redis.call('persist', KEYS[2]); " +
                        "return redis.call('persist', KEYS[3]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName));
    }


    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                +

                "if (redis.call('del', KEYS[1]) == 1) then " +
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " +
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end; " +
                "return 0;",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.unlockMessage, System.currentTimeMillis());
    }

}
