package com.github.rockymine.pgmlogger;

import com.github.rockymine.pgmlogger.listeners.PGMEventListener;
import com.github.rockymine.pgmlogger.logging.MatchLoggingService;
import com.github.rockymine.pgmlogger.privacy.PermittedPlayers;
import java.io.File;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * Main plugin class for logging PGM match events to Parquet files.
 *
 * <p>This plugin tracks Capture the Wool (CTW) matches and records various events:
 *
 * <ul>
 *   <li>Player positions (sampled every 5 seconds)
 *   <li>Player deaths and spawns
 *   <li>Wool objective interactions (touch and capture)
 *   <li>Match lifecycle (start and end)
 * </ul>
 *
 * <p><b>File Structure:</b> Data is organized as {@code data/{map_name}/{timestamp}.parquet}. Each
 * match creates a new Parquet file timestamped at match start.
 *
 * <p><b>Feature Toggles:</b> Individual event types can be enabled/disabled via the
 * {@code /pgmlogger toggle} command without restarting the plugin.
 *
 * <p><b>Privacy:</b> Player names are only logged if they appear in the permitted players list,
 * otherwise anonymous IDs are assigned.
 */
public class PGMLogger extends JavaPlugin {

  private PermittedPlayers permittedPlayers;
  private MatchLoggingService loggingService;

  // Plugin Lifecycle

  /**
   * Initializes the plugin when enabled by the server.
   *
   * <p>Sets up the data folder structure, loads the permitted players list, and registers the event
   * listener for PGM matches.
   */
  @Override
  public void onEnable() {
    saveDefaultConfig();

    File dataFolder = resolveDataFolder();

    permittedPlayers = new PermittedPlayers(this);
    loggingService = new MatchLoggingService(this, dataFolder, permittedPlayers);
    loadSettings();

    getServer().getPluginManager().registerEvents(new PGMEventListener(loggingService), this);

    getLogger().info("PGM Logger enabled! Listening for CTW matches.");
  }

  /**
   * Cleans up resources when the plugin is disabled.
   *
   * <p>Stops position tracking and closes any open Parquet files to ensure match data is properly
   * saved.
   */
  @Override
  public void onDisable() {
    loggingService.shutdown();
  }

  // Commands

  /**
   * Handles the {@code /pgmlogger} command and its subcommands.
   *
   * <p>Requires the {@code pgmlogger.admin} permission.
   *
   * <p>Available subcommands:
   *
   * <ul>
   *   <li>{@code status} - Shows current feature toggles and recording status
   *   <li>{@code toggle <feature>} - Toggles individual features or all at once
   * </ul>
   *
   * @param sender the command sender
   * @param command the command object
   * @param label the command alias used
   * @param args the command arguments
   * @return true if the command was handled
   */
  @Override
  public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
    if (!command.getName().equalsIgnoreCase("pgmlogger")) {
      return false;
    }

    if (!sender.hasPermission("pgmlogger.admin")) {
      sender.sendMessage(ChatColor.RED + "You don't have permission to use this command.");
      return true;
    }

    if (args.length == 0) {
      showHelp(sender);
      return true;
    }

    String subCommand = args[0].toLowerCase();

    switch (subCommand) {
      case "status":
        showStatus(sender);
        break;
      case "toggle":
        if (args.length < 2) {
          sender.sendMessage(ChatColor.RED + "Usage: /pgmlogger toggle <feature>");
          sender.sendMessage(ChatColor.GRAY + "Features: positions, deaths, spawns, wool, all");
        } else {
          toggleFeature(sender, args[1].toLowerCase());
        }
        break;
      case "reload":
        reloadConfig();
        loadSettings();
        permittedPlayers.load();
        sender.sendMessage(ChatColor.GREEN + "PGM Logger config reloaded.");
        break;
      default:
        showHelp(sender);
        break;
    }

    return true;
  }

  /** Displays help information for the /pgmlogger command. */
  private void showHelp(CommandSender sender) {
    sender.sendMessage(ChatColor.GOLD + "=== PGM Logger Commands ===");
    sender.sendMessage(
        ChatColor.YELLOW + "/pgmlogger status" + ChatColor.WHITE + " - Show current settings");
    sender.sendMessage(
        ChatColor.YELLOW + "/pgmlogger toggle <feature>" + ChatColor.WHITE + " - Toggle a feature");
    sender.sendMessage(
        ChatColor.YELLOW + "/pgmlogger reload" + ChatColor.WHITE + " - Reload config files");
    sender.sendMessage(ChatColor.GRAY + "Features: positions, deaths, spawns, wool, all");
  }

  /** Displays the current status of logging features and active recording. */
  private void showStatus(CommandSender sender) {
    sender.sendMessage(ChatColor.GOLD + "=== PGM Logger Status ===");
    sender.sendMessage(formatStatus("Positions", loggingService.isLogPositionsEnabled()));
    sender.sendMessage(formatStatus("Deaths", loggingService.isLogDeathsEnabled()));
    sender.sendMessage(formatStatus("Spawns", loggingService.isLogSpawnsEnabled()));
    sender.sendMessage(formatStatus("Wool", loggingService.isLogWoolEnabled()));

    String activeFileName = loggingService.getActiveFileName();
    if (activeFileName != null) {
      sender.sendMessage(ChatColor.GREEN + "Currently recording: " + activeFileName);
    } else {
      sender.sendMessage(ChatColor.GRAY + "Not recording (no active match).");
    }
  }

  /** Formats a feature's enabled/disabled status for display. */
  private String formatStatus(String feature, boolean enabled) {
    String status = enabled ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF";
    return ChatColor.YELLOW + feature + ": " + status;
  }

  /**
   * Toggles a logging feature on or off.
   *
   * @param sender the command sender to notify of the change
   * @param feature the feature name (positions, deaths, spawns, wool, or all)
   */
  private void toggleFeature(CommandSender sender, String feature) {
    switch (feature) {
      case "positions":
      case "pos":
        sender.sendMessage(ChatColor.YELLOW
            + "Position logging: "
            + formatToggle(loggingService.togglePositions()));
        break;
      case "deaths":
      case "death":
        sender.sendMessage(
            ChatColor.YELLOW + "Death logging: " + formatToggle(loggingService.toggleDeaths()));
        break;
      case "spawns":
      case "spawn":
        sender.sendMessage(
            ChatColor.YELLOW + "Spawn logging: " + formatToggle(loggingService.toggleSpawns()));
        break;
      case "wool":
      case "wools":
        sender.sendMessage(
            ChatColor.YELLOW + "Wool logging: " + formatToggle(loggingService.toggleWool()));
        break;
      case "all":
        boolean newState = !(loggingService.isLogPositionsEnabled()
            && loggingService.isLogDeathsEnabled()
            && loggingService.isLogSpawnsEnabled()
            && loggingService.isLogWoolEnabled());
        loggingService.setAllLogging(newState);
        sender.sendMessage(ChatColor.YELLOW + "All logging: " + formatToggle(newState));
        break;
      default:
        sender.sendMessage(ChatColor.RED + "Unknown feature: " + feature);
        break;
    }
  }

  /** Formats a boolean toggle state for display. */
  private String formatToggle(boolean enabled) {
    return enabled ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF";
  }

  private void loadSettings() {
    loggingService.applySettings(
        Math.max(1, getConfig().getInt("sampling.interval-ticks", 100)),
        getConfig().getBoolean("logging.positions", true),
        getConfig().getBoolean("logging.deaths", true),
        getConfig().getBoolean("logging.spawns", true),
        getConfig().getBoolean("logging.wool", true));
    loggingService.updateDataFolder(resolveDataFolder());
  }

  private File resolveDataFolder() {
    String configured = getConfig().getString("output.path", "data");
    if (configured == null || configured.trim().isEmpty()) {
      configured = "data";
    }
    File folder = new File(configured);
    if (!folder.isAbsolute()) {
      folder = new File(getDataFolder(), configured);
    }
    File validated = validateWritableFolder(folder);
    if (validated != null) {
      return validated;
    }

    File fallback = new File(getDataFolder(), "data");
    File fallbackValidated = validateWritableFolder(fallback);
    if (fallbackValidated != null) {
      getLogger()
          .warning("Invalid output path in config; using default data folder: "
              + fallbackValidated.getAbsolutePath());
      return fallbackValidated;
    }

    getLogger()
        .warning(
            "Failed to validate output path and default data folder; using default path anyway: "
                + fallback.getAbsolutePath());
    return fallback;
  }

  private File validateWritableFolder(File folder) {
    if (folder.exists()) {
      if (!folder.isDirectory()) {
        return null;
      }
    } else {
      if (!folder.mkdirs()) {
        return null;
      }
    }
    if (!folder.canWrite()) {
      return null;
    }
    return folder;
  }
}
