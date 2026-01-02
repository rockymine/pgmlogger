package com.github.rockymine.pgmlogger.logging;

import com.github.rockymine.pgmlogger.PGMLogger;
import com.github.rockymine.pgmlogger.privacy.PermittedPlayers;
import com.github.rockymine.pgmlogger.tracking.PositionTracker;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitTask;

public class MatchLoggingService {

  private static final DateTimeFormatter FILENAME_FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd_HH-mm-ss");

  private final PGMLogger plugin;
  private final Logger logger;
  private final File dataFolder;
  private final PermittedPlayers permittedPlayers;

  private PositionTracker positionTracker;
  private BukkitTask positionSamplerTask;

  private int sampleIntervalTicks = 100;
  private boolean logPositions = true;
  private boolean logDeaths = true;
  private boolean logSpawns = true;
  private boolean logWool = true;

  public MatchLoggingService(PGMLogger plugin, File dataFolder, PermittedPlayers permittedPlayers) {
    this.plugin = plugin;
    this.logger = plugin.getLogger();
    this.dataFolder = dataFolder;
    this.permittedPlayers = permittedPlayers;

    if (!dataFolder.exists()) {
      dataFolder.mkdirs();
    }
  }

  public void applySettings(
      int sampleIntervalTicks,
      boolean logPositions,
      boolean logDeaths,
      boolean logSpawns,
      boolean logWool) {
    this.sampleIntervalTicks = sampleIntervalTicks;
    this.logPositions = logPositions;
    this.logDeaths = logDeaths;
    this.logSpawns = logSpawns;
    this.logWool = logWool;
  }

  public void onMatchStart(String mapName, String matchId) {
    String mapSlug = mapName.toLowerCase().replace(" ", "_").replaceAll("[^a-z0-9_]", "");

    File mapFolder = new File(dataFolder, mapSlug);
    if (!mapFolder.exists()) {
      mapFolder.mkdirs();
    }

    String filename = FILENAME_FORMAT.format(LocalDateTime.now()) + ".parquet";
    File parquetFile = new File(mapFolder, filename);

    try {
      positionTracker = new PositionTracker(parquetFile, permittedPlayers);
      startPositionTracking();
    } catch (IOException e) {
      logger.severe("Failed to create parquet file: " + e.getMessage());
    }
  }

  public void onMatchEnd() {
    stopPositionTracking();

    if (positionTracker != null) {
      logger.info("Data saved to: " + positionTracker.getFileName());
      positionTracker = null;
    }
  }

  public void shutdown() {
    stopPositionTracking();
  }

  private void startPositionTracking() {
    positionSamplerTask = Bukkit.getScheduler()
        .runTaskTimer(
            plugin,
            () -> {
              if (positionTracker != null && logPositions) {
                positionTracker.sampleAllPlayers();
              }
            },
            sampleIntervalTicks,
            sampleIntervalTicks);

    double seconds = sampleIntervalTicks / 20.0;
    logger.info("Started position tracking (every " + seconds + " seconds)");
  }

  private void stopPositionTracking() {
    if (positionSamplerTask != null) {
      positionSamplerTask.cancel();
      positionSamplerTask = null;
    }

    if (positionTracker != null) {
      positionTracker.close();
    }
  }

  public void logDeath(Player player, int x, int y, int z) {
    if (positionTracker != null && logDeaths) {
      positionTracker.logDeath(player, x, y, z);
    }
  }

  public void logSpawn(Player player, int x, int y, int z) {
    if (positionTracker != null && logSpawns) {
      positionTracker.logSpawn(player, x, y, z);
    }
  }

  public void logWoolTouch(Player player, int x, int y, int z, String woolId) {
    if (positionTracker != null && logWool) {
      positionTracker.logWoolTouch(player, x, y, z, woolId);
    }
  }

  public void logWoolCapture(Player player, int x, int y, int z, String woolId) {
    if (positionTracker != null && logWool) {
      positionTracker.logWoolCapture(player, x, y, z, woolId);
    }
  }

  public boolean isLogPositionsEnabled() {
    return logPositions;
  }

  public boolean isLogDeathsEnabled() {
    return logDeaths;
  }

  public boolean isLogSpawnsEnabled() {
    return logSpawns;
  }

  public boolean isLogWoolEnabled() {
    return logWool;
  }

  public boolean togglePositions() {
    logPositions = !logPositions;
    return logPositions;
  }

  public boolean toggleDeaths() {
    logDeaths = !logDeaths;
    return logDeaths;
  }

  public boolean toggleSpawns() {
    logSpawns = !logSpawns;
    return logSpawns;
  }

  public boolean toggleWool() {
    logWool = !logWool;
    return logWool;
  }

  public void setAllLogging(boolean enabled) {
    logPositions = enabled;
    logDeaths = enabled;
    logSpawns = enabled;
    logWool = enabled;
  }

  public String getActiveFileName() {
    return positionTracker != null ? positionTracker.getFileName() : null;
  }
}
