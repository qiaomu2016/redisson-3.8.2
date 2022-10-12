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

import java.util.List;

import org.redisson.api.RLock;

/**
 * RedissonRedLock锁的实现，非常的简单，他是RedissonMultiLock的一个子类，RedLock算法的实现是依赖于MultiLock的一个机制来实现的
 * RedLock locking algorithm implementation for multiple locks. 
 * It manages all locks as one.
 * ----------------------------------------------------- http://zhangtielei.com/posts/blog-redlock-reasoning.html
 * 运行Redlock算法的客户端依次执行下面各个步骤，来完成获取锁的操作：
 * 1.获取当前时间（毫秒数）。
 * 2.按顺序依次向N个Redis节点执行获取锁的操作。这个获取操作跟前面基于单Redis节点的获取锁的过程相同，包含随机字符串my_random_value，也包含过期时间(比如PX 30000，即锁的有效时间)。为了保证在某个Redis节点不可用的时候算法能够继续运行，这个获取锁的操作还有一个超时时间(time out)，它要远小于锁的有效时间（几十毫秒量级）。客户端在向某个Redis节点获取锁失败以后，应该立即尝试下一个Redis节点。这里的失败，应该包含任何类型的失败，比如该Redis节点不可用，或者该Redis节点上的锁已经被其它客户端持有（注：Redlock原文中这里只提到了Redis节点不可用的情况，但也应该包含其它的失败情况）。
 * 3.计算整个获取锁的过程总共消耗了多长时间，计算方法是用当前时间减去第1步记录的时间。如果客户端从大多数Redis节点（>= N/2+1）成功获取到了锁，并且获取锁总共消耗的时间没有超过锁的有效时间(lock validity time)，那么这时客户端才认为最终获取锁成功；否则，认为最终获取锁失败。
 * 4.如果最终获取锁成功了，那么这个锁的有效时间应该重新计算，它等于最初的锁的有效时间减去第3步计算出来的获取锁消耗的时间。
 * 5.如果最终获取锁失败了（可能由于获取到锁的Redis节点个数少于N/2+1，或者整个获取锁的过程消耗的时间超过了锁的最初有效时间），那么客户端应该立即向所有Redis节点发起释放锁的操作（Redis Lua脚本）。
 * 当然，上面描述的只是获取锁的过程，而释放锁的过程比较简单：客户端向所有Redis节点发起释放锁的操作，不管这些节点当时在获取锁的时候成功与否。
 * 由于N个Redis节点中的大多数能正常工作就能保证Redlock正常工作，因此理论上它的可用性更高。
 * -----------------------------------------------------
 *
 * 假设有一个redis cluster，有3个redis master实例
 * 然后执行如下步骤获取一把分布式锁：
 * 1）获取当前时间戳，单位是毫秒
 * 2）轮流尝试在每个master节点上创建锁，过期时间较短，一般就几十毫秒，在每个节点上创建锁的过程中，需要加一个超时时间，一般来说比如几十毫秒，如果没有获取到锁就超时了，标识为获取锁失败
 * 3）尝试在大多数节点上建立一个锁，比如3个节点就要求是2个节点（n / 2 +1）
 * 4）客户端计算建立好锁的时间，如果建立锁的时间小于超时时间，就算建立成功了
 * 5）要是锁建立失败了，那么就依次删除已经创建的锁
 * 6）只要别人创建了一把分布式锁，你就得不断轮询去尝试获取锁
 * 他这里最最核心的一个点，普通的redis分布式锁，其实是在redis集群中根据hash算法选择一台redis实例创建一个锁就可以了
 * RedLock算法思想，不能只在一个redis实例上创建锁，应该是在多个（n / 2 + 1）redis master实例上都成功创建锁，才能算这个整体的RedLock加锁成功，避免说仅仅在一个redis实例上加锁
 *
 * @see <a href="http://redis.io/topics/distlock">http://redis.io/topics/distlock</a>
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonRedLock extends RedissonMultiLock {

    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonRedLock(RLock... locks) {
        super(locks);
    }

    @Override
    protected int failedLocksLimit() {
        // 表示能够容忍加锁失败的一个数量（eg: 3个集群的节点能够容忍加锁失败的数量为1）
        return locks.size() - minLocksAmount(locks);
    }

    protected int minLocksAmount(final List<RLock> locks) {
        return locks.size()/2 + 1;
    }

    @Override
    protected long calcLockWaitTime(long remainTime) {
        // 在对每个lock进行加锁的时候，有一个尝试获取锁超时的时间，原来默认的就是remainTime是 locks.size() * 1500毫秒，每个小lock获取锁超时的时间改成了1500毫秒
        return Math.max(remainTime / locks.size(), 1);
    }

    @Override
    public void unlock() {
        unlockInner(locks);
    }

}
