/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka.internal;

import java.util.concurrent.CountDownLatch;
import zipkin2.Callback;

/**
 * Blocks until {@link Callback#onSuccess(Object)} or {@link Callback#onError(Throwable)}.
 */
// copy/pasted from zipkin2.reporter.AwaitableCallback to avoid dependency complexity
public final class AwaitableCallback implements Callback<Void> {
  final CountDownLatch countDown = new CountDownLatch(1);
  Throwable throwable; // thread visibility guaranteed by the countdown latch

  /**
   * Blocks until {@link Callback#onSuccess(Object)} or {@link Callback#onError(Throwable)}.
   *
   * <p>Returns unexceptionally if {@link Callback#onSuccess(Object)} was called. <p>Throws if
   * {@link Callback#onError(Throwable)} was called.
   */
  public void await() {
    try {
      countDown.await();
      Throwable result = throwable;
      if (result == null) return; // void return
      if (result instanceof Error error) throw error;
      if (result instanceof RuntimeException exception) throw exception;
      // Don't set interrupted status when the callback received InterruptedException
      throw new RuntimeException(result);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override public void onSuccess(Void ignored) {
    countDown.countDown();
  }

  @Override public void onError(Throwable t) {
    throwable = t;
    countDown.countDown();
  }
}
