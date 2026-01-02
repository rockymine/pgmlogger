package com.github.rockymine.pgmlogger.tracking;

import blue.strategic.parquet.ParquetWriter;
import com.github.rockymine.pgmlogger.model.MatchEvent;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.bukkit.Bukkit;

final class MatchEventWriter {

  private final ParquetWriter<MatchEvent> writer;
  private final BlockingQueue<MatchEvent> writeQueue = new LinkedBlockingQueue<>();
  private final ExecutorService writerExecutor;
  private final AtomicBoolean closing = new AtomicBoolean(false);

  MatchEventWriter(File file) throws IOException {
    this.writer = ParquetWriter.writeFile(MatchEvent.SCHEMA, file, MatchEvent.Serializer.INSTANCE);
    this.writerExecutor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "pgmlogger-parquet-writer");
      thread.setDaemon(true);
      return thread;
    });
    this.writerExecutor.submit(this::drainWrites);
  }

  void write(MatchEvent event) {
    if (!closing.get()) {
      writeQueue.offer(event);
    }
  }

  private void drainWrites() {
    try {
      do {
        MatchEvent event = writeQueue.poll(250, TimeUnit.MILLISECONDS);
        if (event != null) {
          writer.write(event);
        }
      } while (!closing.get() || !writeQueue.isEmpty());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Bukkit.getLogger().log(Level.WARNING, "Parquet writer interrupted", e);
    } catch (IOException e) {
      Bukkit.getLogger().log(Level.WARNING, "Failed to write match event", e);
    }
  }

  void close() {
    closing.set(true);
    writerExecutor.shutdown();
    try {
      if (!writerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        writerExecutor.shutdownNow();
      }
      writer.close();
    } catch (IOException e) {
      Bukkit.getLogger().log(Level.WARNING, "Failed to close parquet writer", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Bukkit.getLogger().log(Level.WARNING, "Interrupted while closing parquet writer", e);
    }
  }
}
