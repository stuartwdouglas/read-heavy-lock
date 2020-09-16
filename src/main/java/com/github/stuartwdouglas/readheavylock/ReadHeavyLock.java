package com.github.stuartwdouglas.readheavylock;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A lock that is optimised for infrequent writes.
 * <p>
 * This is a basic PoC, Atomic classes should be replaced with volatiles + AtomicFieldUpdaters
 * <p>
 * Completely untested, as you can see from the lack of tests
 */
public class ReadHeavyLock {

    private Thread writeOwner;
    private final AtomicBoolean globalLock = new AtomicBoolean();
    private final CopyOnWriteArrayList<ReadLockData> readData = new CopyOnWriteArrayList<ReadLockData>();
    private final ThreadLocal<ReadLockData> readThreadLocal = new ThreadLocal<ReadLockData>() {
        @Override
        protected ReadLockData initialValue() {
            synchronized (ReadHeavyLock.this) {
                while (globalLock.get()) {
                    try {
                        ReadHeavyLock.this.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                ReadLockData data = new ReadLockData(Thread.currentThread());
                readData.add(data);
                return data;
            }
        }
    };

    public void readLock() throws InterruptedException {
        ReadLockData data = readThreadLocal.get();
        //optimistically lock
        data.locked.set(true);
        if (globalLock.get()) {
            //the global lock is held, we revert the lock
            synchronized (data) {
                data.locked.set(false);
                if (data.writerWaiting) {
                    //notify if our optimistic lock actually caught the writer
                    data.notify();
                }
            }
            synchronized (this) {
                //wait for the global lock to be released
                while (globalLock.get()) {
                    wait();
                }
                //now lock for real
                data.locked.set(true);
            }
        }
    }

    public void readUnlock() {
        ReadLockData data = readThreadLocal.get();
        //unlock
        data.locked.set(false);
        if (globalLock.get()) {
            synchronized (data) {
                if (data.writerWaiting) {
                    //notify if our lock actually caught the writer
                    data.notify();
                }
            }
        }
    }

    public void writeLock() throws InterruptedException {
        synchronized (this) {
            while (globalLock.get()) {
                wait();
            }
            globalLock.set(true);
            writeOwner = Thread.currentThread();
        }
        Iterator<ReadLockData> it = readData.iterator();
        while (it.hasNext()) {
            ReadLockData i = it.next();
            if (i.locked.get()) {
                synchronized (i) {
                    while (i.locked.get()) {
                        try {
                            i.writerWaiting = true;
                            i.wait();
                        } finally {
                            i.writerWaiting = false;
                        }
                    }
                }
            }
            //attempt to clean up stale thread data on write lock
            if (i.owner.get() == null) {
                it.remove();
            }
        }
    }

    public void writeUnlock() {
        synchronized (this) {
            if (writeOwner != Thread.currentThread()) {
                throw new IllegalMonitorStateException();
            }
            globalLock.set(false);
            writeOwner = null;
            notifyAll();
        }
    }


    static class ReadLockData {
        final AtomicBoolean locked = new AtomicBoolean();
        final WeakReference<Thread> owner;
        boolean writerWaiting;

        ReadLockData(Thread owner) {
            this.owner = new WeakReference<Thread>(owner);
        }
    }
}
