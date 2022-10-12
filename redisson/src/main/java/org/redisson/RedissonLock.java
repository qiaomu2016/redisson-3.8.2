/**
 * Copyright 2018 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.internal.PlatformDependent;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 */
public class RedissonLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {

        private long threadId;
        private Timeout timeout;

        public ExpirationEntry(long threadId, Timeout timeout) {
            super();
            this.threadId = threadId;
            this.timeout = timeout;
        }

        public long getThreadId() {
            return threadId;
        }

        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonLock.class);
    // 用于存放锁续期的线程
    private static final ConcurrentMap<String, ExpirationEntry> expirationRenewalMap = PlatformDependent.newConcurrentHashMap();
    protected long internalLockLeaseTime;  // 锁续期时间，默认为30秒（30 * 1000）

    final UUID id;
    final String entryName;

    protected static final LockPubSub PUBSUB = new LockPubSub();

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    String getChannelName() {  // 获取消息通道的名称：redisson_lock__channel:{name}
        return prefixName("redisson_lock__channel", getName());
    }

    protected String getLockName(long threadId) { // 获取锁的名称： UUID:线程ID
        return id + ":" + threadId;
    }

    /**
     * 加锁，默认锁的持有时间为30秒，同时会开启看门狗机制进行锁续期
     * 注：不会自动释放锁，除非手动调用unlock来释放锁，当然当前应用所在服务器宕机或者当前应用被kill掉情况除外
     * 当然如果当前依赖redisson框架的应用已经不存在了，对锁key不断刷新其生存周期的后台定时调度的任务也就没了，redis里的key，自动就会在最多30秒内就过期删除，其他的客户端就可以成功加锁了
     */
    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 指定锁的持有时间加锁，当超过锁的持有时间还未释放锁（调用unlock）时，会自动释放锁
     * 注：不会开启看门狗机制
     *
     * @param leaseTime the maximum time to hold the lock after granting it,
     *                  before automatically releasing it if it hasn't already been released by invoking <code>unlock</code>.
     *                  If leaseTime is -1, hold the lock until explicitly unlocked.
     * @param unit      the time unit of the {@code leaseTime} argument
     */
    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        long threadId = Thread.currentThread().getId();  // 获取当前线程ID
        Long ttl = tryAcquire(leaseTime, unit, threadId);   // 尝试加锁，返回锁剩余的ttl
        // lock acquired
        if (ttl == null) { // 如果ttl为null,表示加锁成功，直接返回
            return;
        }
        // 加锁失败，此时会进行消息订阅，等待其他线程发送消息（此处会阻塞）
        RFuture<RedissonLockEntry> future = subscribe(threadId);
        commandExecutor.syncSubscription(future);
        // 进入此处，表示收到消息通知，再次尝试加锁
        try {
            while (true) {
                ttl = tryAcquire(leaseTime, unit, threadId); // 尝试加锁
                // lock acquired
                if (ttl == null) { // 加锁成功，退出循环
                    break;
                }

                // waiting for message
                if (ttl >= 0) {
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    getEntry(threadId).getLatch().acquire();
                }
            }
        } finally {
            unsubscribe(future, threadId);  // 取消订阅
        }
//        get(lockAsync(leaseTime, unit));
    }

    private Long tryAcquire(long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync(leaseTime, unit, threadId));
    }

    private RFuture<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, final long threadId) {
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }
        RFuture<Boolean> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.addListener(new FutureListener<Boolean>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                if (!future.isSuccess()) {
                    return;
                }

                Boolean ttlRemaining = future.getNow();
                // lock acquired
                if (ttlRemaining) {
                    scheduleExpirationRenewal(threadId);
                }
            }
        });
        return ttlRemainingFuture;
    }

    private <T> RFuture<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, final long threadId) {
        if (leaseTime != -1) { // 如果加锁时指定锁的持有时间leaseTime，则根据leaseTime加锁
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }
        // 没有指定锁的持有时间，默认根据lockWatchdogTimeout加锁（默认30秒）
        RFuture<Long> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    return;
                }

                Long ttlRemaining = future.getNow();
                // lock acquired
                if (ttlRemaining == null) {  // 加锁成功，开启看门狗机制，对锁进行续期
                    scheduleExpirationRenewal(threadId);
                }
            }
        });
        return ttlRemainingFuture;
    }

    /**
     * tryLock 尝试获取锁（一次），不会阻塞线程
     * 如果没有获取到锁，直接返回false
     * 如果有获取到锁，默认锁的持有时间为30秒，同时会开启看门狗机制进行锁续期
     * 注：不会自动释放锁，除非手动调用unlock来释放锁
     *
     * @return boolean
     */
    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    private void scheduleExpirationRenewal(final long threadId) {
        if (expirationRenewalMap.containsKey(getEntryName())) {  // 如果已存在续期的线程，直接返回，保证同一个线程只存在一个续期线程
            return;
        }

        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {

                RFuture<Boolean> future = renewExpirationAsync(threadId);  // 续期

                future.addListener(new FutureListener<Boolean>() {
                    @Override
                    public void operationComplete(Future<Boolean> future) throws Exception {
                        expirationRenewalMap.remove(getEntryName());  // 任务完成，移除续期线程
                        if (!future.isSuccess()) {
                            log.error("Can't update lock " + getName() + " expiration", future.cause());
                            return;
                        }

                        if (future.getNow()) {  // 锁还存在
                            // reschedule itself
                            scheduleExpirationRenewal(threadId);  // 每隔10S续期一次
                        }
                    }
                });
            }

        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);  // 每隔10S续期一次

        // 保存续期的线程，如果已经存在续期的线程，则取消任务（正常应该不会调用cancel，因为最前面已经加了判断）
        if (expirationRenewalMap.putIfAbsent(getEntryName(), new ExpirationEntry(threadId, task)) != null) {
            task.cancel();
        }
    }

    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +    // 判断当前线程加锁的key是否存在
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +   // 如果存在，对锁进行续期，续期的时间为internalLockLeaseTime（30秒或者传递的leaseTime）
                        "return 1; " +      // 返回1
                        "end; " +           // 如果不存在当前线程加锁的key
                        "return 0;",        // 返回0
                Collections.<Object>singletonList(getName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    // 取消锁续期，一是从expirationRenewalMap移除锁续期线程，二是取消任务
    void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = expirationRenewalMap.get(getEntryName());
        if (task != null && (threadId == null || task.getThreadId() == threadId)) {
            expirationRenewalMap.remove(getEntryName());
            task.getTimeout().cancel();
        }
    }

    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);  // 转为毫秒

        // eg: RLock lock = redisson.getLock("pms:product:1000001");
        // KEYS[1]：表示锁的名称，即 pms:product:1000001
        // ARGV[1]：表示锁的过期时间，即 internalLockLeaseTime
        // ARGV[2]：表示当前线程加锁的名称，即 UUID:线程ID
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                "if (redis.call('exists', KEYS[1]) == 0) then " +   // 判断 pms:product:1000001 是否存在，如果不存在，表示加锁成功，返回nil
                        "redis.call('hset', KEYS[1], ARGV[2], 1); " +     // 给 pms:product:1000001 hash结构（类似map）中设置一个key value，key为 UUID:线程ID，值为1
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +     // 设置锁 pms:product:1000001 的过期时间为 internalLockLeaseTime，如果没有指定默认为30秒
                        "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +     // 如果已经存在，则判断是否为当前线程再次加锁，如果是，则把当前线程加锁的次数加1，返回nil
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +            // 给 pms:product:1000001 hash结构中 key为 UUID:线程ID 的值加1
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +                // 同时设置锁 pms:product:1000001 的过期时间为 internalLockLeaseTime
                        "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",  // 如果 pms:product:1000001 存在，并且不是当前线程再次加锁，返回当前锁  pms:product:1000001 的剩余有效时间
                Collections.<Object>singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
    }

    private void acquireFailed(long threadId) {
        get(acquireFailedAsync(threadId));
    }

    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    /**
     * 指定超时时间获取锁
     *
     * @param waitTime  the maximum time to aquire the lock 获取锁的最大超时等待时间
     * @param leaseTime lease time  锁的持有时间
     * @param unit      time unit    单位
     * @return boolean
     * @throws InterruptedException 中断异常
     */
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        long time = unit.toMillis(waitTime);  // 超时时间（毫秒）
        long current = System.currentTimeMillis();  // 当前时间（毫秒）
        final long threadId = Thread.currentThread().getId();   // 当前线程ID
        Long ttl = tryAcquire(leaseTime, unit, threadId);  // 先尝试获取锁
        // lock acquired
        if (ttl == null) {  // 如果成功获取到锁，直接返回
            return true;
        }
        // 没有获取到锁，重新计算锁的超时时间
        time -= (System.currentTimeMillis() - current);
        if (time <= 0) {  // 如果已经超过加锁的最大超时时间，返回加锁失败
            acquireFailed(threadId);
            return false;
        }

        current = System.currentTimeMillis();  // 重新设置当前时间（毫秒）
        final RFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId); // 订阅消息，此时会阻塞线程
        // 阻塞等待锁释放，await()返回false，说明等待超时了
        if (!await(subscribeFuture, time, TimeUnit.MILLISECONDS)) {  // 等待剩余的超时时间
            if (!subscribeFuture.cancel(false)) {
                subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {  // 添加监听
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (subscribeFuture.isSuccess()) {
                            unsubscribe(subscribeFuture, threadId);  // 等待都超时了，直接取消订阅
                        }
                    }
                });
            }
            acquireFailed(threadId);
            return false;
        }

        try {
            time -= (System.currentTimeMillis() - current);  // 重新计算锁的超时时间
            if (time <= 0) {  // 如果已经超过加锁的最大超时时间，返回加锁失败
                acquireFailed(threadId);
                return false;
            }

            while (true) {
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(leaseTime, unit, threadId); // 尝试加锁
                // lock acquired
                if (ttl == null) {  // 加锁成功，直接返回
                    return true;
                }
                // 没有获取到锁，重新计算锁的超时时间
                time -= (System.currentTimeMillis() - currentTime);
                if (time <= 0) {  // 如果已经超过加锁的最大超时时间，返回加锁失败
                    acquireFailed(threadId);
                    return false;
                }

                // waiting for message
                // 这两行代码的细节，不用过多的关注，里面涉及到了其他的同步组件，Semaphore，在这里的话呢，其实大家只要知道一点，
                // 如果获取锁不成功，此时就会等待一段时间，再次投入到while(true)死循环的逻辑内，尝试去获取锁，以此循环往复
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {  // 如果锁的剩余有效时间 大于 0 ，并且 少于 剩余的超时时间，通过信号量Semaphore等待ttl时间获取一个许可
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    getEntry(threadId).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);  // 通过信号量Semaphore等待剩余的超时时间（time）获取一个许可
                }

                time -= (System.currentTimeMillis() - currentTime);  // 重新计算锁的超时时间
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }
            }
        } finally {
            unsubscribe(subscribeFuture, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected RedissonLockEntry getEntry(long threadId) {
        return PUBSUB.getEntry(getEntryName());
    }

    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return PUBSUB.subscribe(getEntryName(), getChannelName(), commandExecutor.getConnectionManager().getSubscribeService());
    }

    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        PUBSUB.unsubscribe(future.getNow(), getEntryName(), getChannelName(), commandExecutor.getConnectionManager().getSubscribeService());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getName(), codec, RedisCommands.EXISTS, getName());
    }

    @Override
    public boolean isHeldByCurrentThread() {
        RFuture<Boolean> future = commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(Thread.currentThread().getId()));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", ValueType.MAP_VALUE, new IntegerReplayConvertor(0));

    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, HGET, getName(), getLockName(Thread.currentThread().getId()));
    }

    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        // eg: RLock lock = redisson.getLock("pms:product:1000001");
        // KEYS[1]：表示锁的名称，即 pms:product:1000001
        // KEYS[2]：表示消息通道，即 redisson_lock__channel:{pms:product:1000001}
        // ARGV[1]：表示发布消息的内容，即 0
        // ARGV[2]：表示锁的过期时间，即 internalLockLeaseTime
        // ARGV[3]：表示当前线程加锁的名称，即 UUID:线程ID
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('exists', KEYS[1]) == 0) then " +    // 如果锁不存在
                        "redis.call('publish', KEYS[2], ARGV[1]); " +      // 往消息通道 redisson_lock__channel:{pms:product:1000001} 推送一条消息，内容为0，当其他监听该通道的线程（阻塞）收到消息后，就会退出阻塞状态，再次尝试加锁
                        "return 1; " +                                     // 返回1
                        "end;" +
                        "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +  // 如果锁存在，调用 hexists pms:product:1000001 UUID:线程ID 判断当前这个锁key对应的hash数据结构中，是否存在当前线程加的这个锁
                        "return nil;" +                                              // 不存在当前线程加的这个锁，直接返回nil
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +   // 存在当前线程加的这个锁，加锁次数减1
                        "if (counter > 0) then " +                                          // 如果大于0，表示为可重入锁，还持有当前锁，不能删除
                        "redis.call('pexpire', KEYS[1], ARGV[2]); " +                       // 重新设置锁的持有时间
                        "return 0; " +                                                      // 返回0
                        "else " +
                        "redis.call('del', KEYS[1]); " +                                    // 如果锁的记数为0，表示可以释放锁，直接通过del删除key
                        "redis.call('publish', KEYS[2], ARGV[1]); " +                       // 同时 往消息通道 redisson_lock__channel:{pms:product:1000001} 推送一条消息，内容为0
                        "return 1; " +                                                      // 返回1
                        "end; " +
                        "return nil;",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(threadId));

    }

    @Override
    public RFuture<Void> unlockAsync(final long threadId) {
        final RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Boolean> future = unlockInnerAsync(threadId);  // 释放锁

        future.addListener(new FutureListener<Boolean>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                if (!future.isSuccess()) { // 如果锁释放执行失败，取消锁自动续期任务，防止锁一直没有释放导致死锁的问题
                    cancelExpirationRenewal(threadId);
                    result.tryFailure(future.cause());
                    return;
                }

                Boolean opStatus = future.getNow();
                if (opStatus == null) { // 不存在当前线程加的锁
                    IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                            + id + " thread-id: " + threadId);
                    result.tryFailure(cause);
                    return;
                }
                // 如果锁释放成功
                if (opStatus) {
                    cancelExpirationRenewal(null);  // 取消锁自动续期任务
                }
                result.trySuccess(null);
            }
        });

        return result;
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        final long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(final long leaseTime, final TimeUnit unit, final long currentThreadId) {
        final RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    result.tryFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();

                // lock acquired
                if (ttl == null) {
                    if (!result.trySuccess(null)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }

                final RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
                subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (!future.isSuccess()) {
                            result.tryFailure(future.cause());
                            return;
                        }

                        lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                    }

                });
            }
        });

        return result;
    }

    private void lockAsync(final long leaseTime, final TimeUnit unit,
                           final RFuture<RedissonLockEntry> subscribeFuture, final RPromise<Void> result, final long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();
                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(null)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }

                final RedissonLockEntry entry = getEntry(currentThreadId);
                if (entry.getLatch().tryAcquire()) {
                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    // waiting for message
                    final AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    final Runnable listener = new Runnable() {
                        @Override
                        public void run() {
                            if (futureRef.get() != null) {
                                futureRef.get().cancel();
                            }
                            lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                        }
                    };

                    entry.addListener(listener);

                    if (ttl >= 0) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, ttl, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(final long waitTime, final long leaseTime, final TimeUnit unit,
                                         final long currentThreadId) {
        final RPromise<Boolean> result = new RedissonPromise<Boolean>();

        final AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        final long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    result.tryFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();

                // lock acquired
                if (ttl == null) {
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }

                long elapsed = System.currentTimeMillis() - currentTime;
                time.addAndGet(-elapsed);

                if (time.get() <= 0) {
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                final long current = System.currentTimeMillis();
                final AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                final RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
                subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (!future.isSuccess()) {
                            result.tryFailure(future.cause());
                            return;
                        }

                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);

                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    }
                });
                if (!subscribeFuture.isDone()) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            if (!subscribeFuture.isDone()) {
                                subscribeFuture.cancel(false);
                                trySuccessFalse(currentThreadId, result);
                            }
                        }
                    }, time.get(), TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }

        });


        return result;
    }

    private void trySuccessFalse(final long currentThreadId, final RPromise<Boolean> result) {
        acquireFailedAsync(currentThreadId).addListener(new FutureListener<Void>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (future.isSuccess()) {
                    result.trySuccess(false);
                } else {
                    result.tryFailure(future.cause());
                }
            }
        });
    }

    private void tryLockAsync(final AtomicLong time, final long leaseTime, final TimeUnit unit,
                              final RFuture<RedissonLockEntry> subscribeFuture, final RPromise<Boolean> result, final long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture, currentThreadId);
            return;
        }

        if (time.get() <= 0) {
            unsubscribe(subscribeFuture, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }

        final long current = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();
                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                if (time.get() <= 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                final long current = System.currentTimeMillis();
                final RedissonLockEntry entry = getEntry(currentThreadId);
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    final AtomicBoolean executed = new AtomicBoolean();
                    final AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    final Runnable listener = new Runnable() {
                        @Override
                        public void run() {
                            executed.set(true);
                            if (futureRef.get() != null) {
                                futureRef.get().cancel();
                            }

                            long elapsed = System.currentTimeMillis() - current;
                            time.addAndGet(-elapsed);

                            tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                        }
                    };
                    entry.addListener(listener);

                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) {
                        t = ttl;
                    }
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);

                                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
            }
        });
    }


}
;