package com.github.rockymine.pgmlogger.logging;

import com.github.rockymine.pgmlogger.PGMLogger;
import com.github.rockymine.pgmlogger.privacy.PermittedPlayers;
import com.github.rockymine.pgmlogger.tracking.PositionTracker;
import com.github.rockymine.pgmlogger.webhook.DiscordWebhookClient;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
  private File dataFolder;
  private final PermittedPlayers permittedPlayers;
  private final DiscordWebhookClient webhookClient;

  private PositionTracker positionTracker;
  private BukkitTask positionSamplerTask;

  private String webhookUrl;
  private String activeMatchName;
  private String activeMatchId;
  private Instant activeMatchStartTime;

  private int sampleIntervalTicks = 100;
  private boolean logPositions = true;
  private boolean logDeaths = true;
  private boolean logSpawns = true;
  private boolean logWool = true;

  public MatchLoggingService(PGMLogger plugin, File dataFolder, PermittedPlayers permittedPlayers) {
    this.plugin = plugin;
    this.logger = plugin.getLogger();
    this.permittedPlayers = permittedPlayers;
    this.webhookClient = new DiscordWebhookClient(logger);

    updateDataFolder(dataFolder);
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
    this.activeMatchName = mapName;
    this.activeMatchId = matchId;
    this.activeMatchStartTime = Instant.now();
    String mapSlug = toSlug(mapName);
    String matchSlug = toSlug(matchId);
    if (matchSlug.isEmpty()) {
      matchSlug = "match";
    }

    File mapFolder = new File(dataFolder, mapSlug);
    if (!mapFolder.exists()) {
      mapFolder.mkdirs();
    }

    String filename = FILENAME_FORMAT.format(LocalDateTime.now()) + "_" + matchSlug + ".parquet";
    File parquetFile = new File(mapFolder, filename);

    try {
      positionTracker = new PositionTracker(parquetFile, permittedPlayers);
      startPositionTracking();
    } catch (IOException e) {
      logger.severe("Failed to create parquet file: " + e.getMessage());
    }
  }

  public void onMatchEnd() {
    Instant matchEndTime = Instant.now();
    stopPositionTracking();

    if (positionTracker != null) {
      String matchName = activeMatchName;
      String matchId = activeMatchId;
      Instant matchStartTime = activeMatchStartTime;
      File savedFile = positionTracker.getFile();
      String fileSize = formatFileSize(positionTracker.getFileSizeBytes());
      logger.info("Data saved to: " + savedFile.getAbsolutePath() + " (" + fileSize + ")");
      String duration = formatDuration(matchStartTime, matchEndTime);
      sendWebhook(matchName, matchId, duration, savedFile, fileSize);
      positionTracker = null;
    }
    activeMatchName = null;
    activeMatchId = null;
    activeMatchStartTime = null;
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

  public void logDeath(
      Player victim, int vx, int vy, int vz, Player killer, Integer kx, Integer ky, Integer kz) {
    if (positionTracker != null && logDeaths) {
      positionTracker.logDeath(victim, vx, vy, vz, killer, kx, ky, kz);
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

  public void updateDataFolder(File dataFolder) {
    this.dataFolder = dataFolder;
    if (!dataFolder.exists()) {
      dataFolder.mkdirs();
    }
  }

  public void updateWebhookUrl(String webhookUrl) {
    this.webhookUrl = webhookUrl;
  }

  private void sendWebhook(
      String matchName, String matchId, String duration, File savedFile, String fileSize) {
    if (webhookUrl == null || webhookUrl.trim().isEmpty()) {
      return;
    }
    Bukkit.getScheduler()
        .runTaskAsynchronously(
            plugin,
            () -> webhookClient.sendMatchSaved(
                webhookUrl, matchName, matchId, duration, savedFile, fileSize));
  }

  private String toSlug(String value) {
    if (value == null) {
      return "";
    }
    return value.toLowerCase().replace(" ", "_").replaceAll("[^a-z0-9_]", "");
  }

  private String formatFileSize(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    String[] units = {"KB", "MB", "GB", "TB"};
    double value = bytes;
    int unitIndex = -1;
    do {
      value /= 1024.0;
      unitIndex++;
    } while (value >= 1024 && unitIndex < units.length - 1);
    return String.format("%.2f %s", value, units[unitIndex]);
  }

  private String formatDuration(Instant startTime, Instant endTime) {
    if (startTime == null || endTime == null || endTime.isBefore(startTime)) {
      return "unknown duration";
    }
    Duration duration = Duration.between(startTime, endTime);
    long totalSeconds = duration.getSeconds();
    long hours = totalSeconds / 3600;
    long minutes = (totalSeconds % 3600) / 60;
    long seconds = totalSeconds % 60;
    if (hours > 0) {
      return String.format("%dh %dm %ds", hours, minutes, seconds);
    }
    if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    }
    return String.format("%ds", seconds);
  }
}
