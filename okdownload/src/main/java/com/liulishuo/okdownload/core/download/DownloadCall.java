/*
 * Copyright (c) 2017 LingoChamp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liulishuo.okdownload.core.download;

import android.os.SystemClock;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;

import com.liulishuo.okdownload.DownloadTask;
import com.liulishuo.okdownload.OkDownload;
import com.liulishuo.okdownload.core.NamedRunnable;
import com.liulishuo.okdownload.core.Util;
import com.liulishuo.okdownload.core.breakpoint.BlockInfo;
import com.liulishuo.okdownload.core.breakpoint.BreakpointInfo;
import com.liulishuo.okdownload.core.breakpoint.DownloadStore;
import com.liulishuo.okdownload.core.cause.EndCause;
import com.liulishuo.okdownload.core.cause.ResumeFailedCause;
import com.liulishuo.okdownload.core.file.MultiPointOutputStream;
import com.liulishuo.okdownload.core.file.ProcessFileStrategy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DownloadCall extends NamedRunnable implements Comparable<DownloadCall> {
    private static final ExecutorService EXECUTOR = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
            Util.threadFactory("OkDownload Block", false));

    private static final String TAG = "DownloadCall";

    static final int MAX_COUNT_RETRY_FOR_PRECONDITION_FAILED = 1;
    public final DownloadTask task;
    public final boolean asyncExecuted;
    @NonNull private final ArrayList<DownloadChain> blockChainList;

    @Nullable volatile DownloadCache cache;
    volatile boolean canceled;
    volatile boolean finishing;

    private volatile Thread currentThread;
    @NonNull private final DownloadStore store;

    private DownloadCall(DownloadTask task, boolean asyncExecuted, @NonNull DownloadStore store) {
        this(task, asyncExecuted, new ArrayList<DownloadChain>(), store);
    }

    DownloadCall(DownloadTask task, boolean asyncExecuted,
                 @NonNull ArrayList<DownloadChain> runningBlockList,
                 @NonNull DownloadStore store) {
        super("download call: " + task.getId());
        this.task = task;
        this.asyncExecuted = asyncExecuted;
        this.blockChainList = runningBlockList;
        this.store = store;
    }

    public static DownloadCall create(DownloadTask task, boolean asyncExecuted,
                                      @NonNull DownloadStore store) {
        return new DownloadCall(task, asyncExecuted, store);
    }

    public boolean cancel() {
        synchronized (this) {
            if (canceled) return false;
            if (finishing) return false;
            this.canceled = true;
        }

        final long startCancelTime = SystemClock.uptimeMillis();

        OkDownload.with().downloadDispatcher().flyingCanceled(this);

        final DownloadCache cache = this.cache;
        if (cache != null) cache.setUserCanceled();

        final List<DownloadChain> chains = (List<DownloadChain>) blockChainList.clone();
        for (DownloadChain chain : chains) {
            chain.cancel();
        }

        if (chains.isEmpty() && currentThread != null) {
            Util.d(TAG,
                    "interrupt thread with cancel operation because of chains are not running "
                            + task.getId());
            currentThread.interrupt();
        }

        if (cache != null) cache.getOutputStream().cancelAsync();

        Util.d(TAG, "cancel task " + task.getId() + " consume: " + (SystemClock
                .uptimeMillis() - startCancelTime) + "ms");
        return true;
    }

    public boolean isCanceled() { return canceled; }

    public boolean isFinishing() { return finishing; }

    @Override
    public void execute() throws InterruptedException {
        currentThread = Thread.currentThread();

        boolean retry;
        int retryCount = 0;

        // ready param
        final OkDownload okDownload = OkDownload.with();
        final ProcessFileStrategy fileStrategy = okDownload.processFileStrategy();

        // inspect task start
        inspectTaskStart();
        do {
            // 0. check basic param before start
            if (task.getUrl().length() <= 0) {
                this.cache = new DownloadCache.PreError(
                        new IOException("unexpected url: " + task.getUrl()));
                break;
            }

            if (canceled) break;

            // 1. create basic info if not exist
            @NonNull final BreakpointInfo info;
            try {
                //RemitStoreOnSQLite
                BreakpointInfo infoOnStore = store.get(task.getId());
                //若第一次下载，infoOnStore 是空，则 create 一个并插入缓存(数据库)
                if (infoOnStore == null) {
                    info = store.createAndInsert(task);
                } else {
                    info = infoOnStore;
                }
                setInfoToTask(info);
            } catch (IOException e) {
                this.cache = new DownloadCache.PreError(e);
                break;
            }
            if (canceled) break;

            // ready cache.
            @NonNull final DownloadCache cache = createCache(info);
            this.cache = cache;

            // 2. remote check.
            final BreakpointRemoteCheck remoteCheck = createRemoteCheck(info);
            try {
                //TODO: Trial 尝试获取文件的信息，比如文件大小，文件类型等
                remoteCheck.check();
            } catch (IOException e) {
                //处理前置条件的异常，在这里会对错误进行分类
                cache.catchException(e);
                break;
            }
            //设置重定向的地址，如果有重定向的话
            cache.setRedirectLocation(task.getRedirectLocation());

            // 3. waiting for file lock release after file path is confirmed.
            fileStrategy.getFileLock().waitForRelease(task.getFile().getAbsolutePath());

            // 4. reuse another info if another info is idle and available for reuse.
            OkDownload.with().downloadStrategy()
                    .inspectAnotherSameInfo(task, info, remoteCheck.getInstanceLength());

            try {
                if (remoteCheck.isResumable()) {
                    // 5. local check
                    final BreakpointLocalCheck localCheck = createLocalCheck(info,
                            remoteCheck.getInstanceLength());
                    //TODO 检查下载的文件是否已经存在，block 分片信息是否已经存在，OutPutStream信息
                    localCheck.check();
                    if (localCheck.isDirty()) { // 如果下载文件不存在，或者 block 分片信息不存在
                        Util.d(TAG, "breakpoint invalid: download from beginning because of "
                                + "local check is dirty " + task.getId() + " " + localCheck);
                        // 6. assemble block data
                        fileStrategy.discardProcess(task);
                        //TODO: 根据是否允许切片来将文件分片
                        assembleBlockAndCallbackFromBeginning(info, remoteCheck,
                                localCheck.getCauseOrThrow());
                    } else { //已经有历史信息，则直接从断点开始下载
                        okDownload.callbackDispatcher().dispatch()
                                .downloadFromBreakpoint(task, info);
                    }
                } else {
                    Util.d(TAG, "breakpoint invalid: download from beginning because of "
                            + "remote check not resumable " + task.getId() + " " + remoteCheck);
                    // 6. assemble block data
                    fileStrategy.discardProcess(task);
                    //TODO: 根据是否允许切片来将文件分片
                    assembleBlockAndCallbackFromBeginning(info, remoteCheck,
                            remoteCheck.getCauseOrThrow());
                }
            } catch (IOException e) {
                cache.setUnknownError(e);
                break;
            }
            // 7. start with cache and info.
            //TODO: 这里正式开始下载！！！
            start(cache, info);
            if (canceled) break;

            // 8. retry if precondition failed.
            if (cache.isPreconditionFailed() //前置条件失败
                    && retryCount++ < MAX_COUNT_RETRY_FOR_PRECONDITION_FAILED) {
                store.remove(task.getId());
                retry = true;
            } else {
                retry = false;
            }
        } while (retry);

        // finish
        finishing = true;
        blockChainList.clear();

        final DownloadCache cache = this.cache;
        Log.d(TAG, "execute: deal finish canceled="+canceled + ", cache="+cache);
        //疑问1： 这里 cache 为什么会为 null ，从前面的代码分析，只有 canceled = true 的时候会出现代码逻辑从 while 方法体中 break，
        //       但这里已经先判断了 canceled 了，那么 cache == null 这个判断应该不会成立，是这样吗？？
        //疑问2： 执行到这里，如果 canceled == true || cache==null， 那么应该通知上层 taskEnd 吧，直接返回会导致上层卡住
        if (canceled || cache == null) return;

        final EndCause cause;
        Exception realCause = null;
        if (cache.isServerCanceled() || cache.isUnknownError()
                || cache.isPreconditionFailed()) {
            // error
            cause = EndCause.ERROR;
            realCause = cache.getRealCause();
        } else if (cache.isFileBusyAfterRun()) {
            cause = EndCause.FILE_BUSY;
        } else if (cache.isPreAllocateFailed()) {
            cause = EndCause.PRE_ALLOCATE_FAILED;
            realCause = cache.getRealCause();
        } else {
            cause = EndCause.COMPLETED;
        }
        inspectTaskEnd(cache, cause, realCause);
    }

    private void inspectTaskStart() {
        store.onTaskStart(task.getId());
        OkDownload.with().callbackDispatcher().dispatch().taskStart(task);
    }

    private void inspectTaskEnd(DownloadCache cache, @NonNull EndCause cause,
                                @Nullable Exception realCause) {
        // non-cancel handled on here
        if (cause == EndCause.CANCELED) {
            throw new IllegalAccessError("can't recognize cancelled on here");
        }

        synchronized (this) {
            if (canceled) return;
            finishing = true;
        }

        store.onTaskEnd(task.getId(), cause, realCause);
        if (cause == EndCause.COMPLETED) {
            store.markFileClear(task.getId());
            OkDownload.with().processFileStrategy()
                    .completeProcessStream(cache.getOutputStream(), task);
        }

        OkDownload.with().callbackDispatcher().dispatch().taskEnd(task, cause, realCause);
    }

    // this method is convenient for unit-test.
    DownloadCache createCache(@NonNull BreakpointInfo info) {
        //create 可以多线程下载写入的 OutputStream
        final MultiPointOutputStream outputStream = OkDownload.with().processFileStrategy()
                .createProcessStream(task, info, store);
        return new DownloadCache(outputStream);
    }

    // this method is convenient for unit-test.
    int getPriority() {
        return task.getPriority();
    }

    void start(final DownloadCache cache, BreakpointInfo info) throws InterruptedException {
        final int blockCount = info.getBlockCount();
        final List<DownloadChain> blockChainList = new ArrayList<>(info.getBlockCount());
        final List<Integer> blockIndexList = new ArrayList<>();
        for (int i = 0; i < blockCount; i++) {
            final BlockInfo blockInfo = info.getBlock(i);
            if (Util.isCorrectFull(blockInfo.getCurrentOffset(), blockInfo.getContentLength())) {
                continue;
            }

            Util.resetBlockIfDirty(blockInfo);
            final DownloadChain chain = DownloadChain.createChain(i, task, info, cache, store);
            blockChainList.add(chain);
            blockIndexList.add(chain.getBlockIndex());
        }

        if (canceled) {
            return;
        }

        cache.getOutputStream().setRequireStreamBlocks(blockIndexList);

        //开始下载各个 block
        startBlocks(blockChainList);
    }

    @Override
    protected void interrupted(InterruptedException e) {
    }

    @Override
    protected void finished() {
        OkDownload.with().downloadDispatcher().finish(this);
        Util.d(TAG, "call is finished " + task.getId());
    }

    void startBlocks(List<DownloadChain> tasks) throws InterruptedException {
        ArrayList<Future> futures = new ArrayList<>(tasks.size());
        try {
            for (DownloadChain chain : tasks) {
                futures.add(submitChain(chain));
            }

            blockChainList.addAll(tasks);

            for (Future future : futures) {
                if (!future.isDone()) {
                    try {
                        future.get(); //等待下载完成，或者取消、或 exception 中断
                    } catch (CancellationException | ExecutionException ignore) { }
                }
            }
        } catch (Throwable t) {
            for (Future future : futures) {
                future.cancel(true);
            }
            throw t;
        } finally {
            blockChainList.removeAll(tasks);
        }
    }

    // convenient for unit-test
    @NonNull BreakpointLocalCheck createLocalCheck(@NonNull BreakpointInfo info,
                                                   long responseInstanceLength) {
        return new BreakpointLocalCheck(task, info, responseInstanceLength);
    }

    // convenient for unit-test
    @NonNull BreakpointRemoteCheck createRemoteCheck(@NonNull BreakpointInfo info) {
        return new BreakpointRemoteCheck(task, info);
    }

    // convenient for unit-test
    void setInfoToTask(@NonNull BreakpointInfo info) {
        DownloadTask.TaskHideWrapper.setBreakpointInfo(task, info);
    }

    void assembleBlockAndCallbackFromBeginning(@NonNull BreakpointInfo info,
                                               @NonNull BreakpointRemoteCheck remoteCheck,
                                               @NonNull ResumeFailedCause failedCause) {
        Util.assembleBlock(task, info, remoteCheck.getInstanceLength(),
                remoteCheck.isAcceptRange());
        //通知 infoReady
        OkDownload.with().callbackDispatcher().dispatch()
                .downloadFromBeginning(task, info, failedCause);
    }

    Future<?> submitChain(DownloadChain chain) {
        //TODO submit 返回的是 Future 的实现类 FutureTask
        return EXECUTOR.submit(chain);
    }

    public boolean equalsTask(@NonNull DownloadTask task) {
        return this.task.equals(task);
    }

    @Nullable public File getFile() {
        return this.task.getFile();
    }

    @SuppressFBWarnings(value = "Eq", justification = "This special case is just for task priority")
    @Override
    public int compareTo(@NonNull DownloadCall o) {
        return o.getPriority() - getPriority();
    }
}
