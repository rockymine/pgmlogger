package com.example.pgmlogger;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Main plugin class for logging PGM match events to Parquet files.
 *
 * <p>This plugin tracks Capture the Wool (CTW) matches and records various events:
 * <ul>
 *   <li>Player positions (sampled every 5 seconds)</li>
 *   <li>Player deaths and spawns</li>
 *   <li>Wool objective interactions (touch and capture)</li>
 *   <li>Match lifecycle (start and end)</li>
 * </ul>
 *
 * <p><b>File Structure:</b> Data is organized as {@code data/{map_name}/{timestamp}.parquet}.
 * Each match creates a new Parquet file timestamped at match start.
 *
 * <p><b>Feature Toggles:</b> Individual event types can be enabled/disabled via the
 * {@code /pgmlogger toggle} command without restarting the plugin.
 *
 * <p><b>Privacy:</b> Player names are only logged if they appear in the permitted
 * players list, otherwise anonymous IDs are assigned.
 */
public class Pgmlogger extends JavaPlugin {

    private static final DateTimeFormatter FILENAME_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd_HH-mm-ss");

    private PositionTracker positionTracker;
    private BukkitTask positionSamplerTask;
    private File dataFolder;
    private PermittedPlayers permittedPlayers;

    private boolean logPositions = true;
    private boolean logDeaths = true;
    private boolean logSpawns = true;
    private boolean logWool = true;

    // Plugin Lifecycle

    /**
     * Initializes the plugin when enabled by the server.
     *
     * <p>Sets up the data folder structure, loads the permitted players list,
     * and registers the event listener for PGM matches.
     */
    @Override
    public void onEnable() {
        dataFolder = new File(getDataFolder(), "data");
        if (!dataFolder.exists()) {
            dataFolder.mkdirs();
        }

        permittedPlayers = new PermittedPlayers(this);

        getServer().getPluginManager().registerEvents(
                new PGMEventListener(this),
                this
        );

        getLogger().info("PGM Logger enabled! Listening for CTW matches.");
    }

    /**
     * Cleans up resources when the plugin is disabled.
     *
     * <p>Stops position tracking and closes any open Parquet files to ensure
     * match data is properly saved.
     */
    @Override
    public void onDisable() {
        stopPositionTracking();
        getLogger().info("PGM Logger disabled.");
    }

    // Commands

    /**
     * Handles the {@code /pgmlogger} command and its subcommands.
     *
     * <p>Requires the {@code pgmlogger.admin} permission.
     *
     * <p>Available subcommands:
     * <ul>
     *   <li>{@code status} - Shows current feature toggles and recording status</li>
     *   <li>{@code toggle <feature>} - Toggles individual features or all at once</li>
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
            default:
                showHelp(sender);
                break;
        }

        return true;
    }

    /**
     * Displays help information for the /pgmlogger command.
     */
    private void showHelp(CommandSender sender) {
        sender.sendMessage(ChatColor.GOLD + "=== PGM Logger Commands ===");
        sender.sendMessage(ChatColor.YELLOW + "/pgmlogger status" + ChatColor.WHITE + " - Show current settings");
        sender.sendMessage(ChatColor.YELLOW + "/pgmlogger toggle <feature>" + ChatColor.WHITE + " - Toggle a feature");
        sender.sendMessage(ChatColor.GRAY + "Features: positions, deaths, spawns, wool, all");
    }

    /**
     * Displays the current status of logging features and active recording.
     */
    private void showStatus(CommandSender sender) {
        sender.sendMessage(ChatColor.GOLD + "=== PGM Logger Status ===");
        sender.sendMessage(formatStatus("Positions", logPositions));
        sender.sendMessage(formatStatus("Deaths", logDeaths));
        sender.sendMessage(formatStatus("Spawns", logSpawns));
        sender.sendMessage(formatStatus("Wool", logWool));

        if (positionTracker != null) {
            sender.sendMessage(ChatColor.GREEN + "Currently recording: " + positionTracker.getFileName());
        } else {
            sender.sendMessage(ChatColor.GRAY + "Not recording (no active match).");
        }
    }

    /**
     * Formats a feature's enabled/disabled status for display.
     */
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
                logPositions = !logPositions;
                sender.sendMessage(ChatColor.YELLOW + "Position logging: " + formatToggle(logPositions));
                break;
            case "deaths":
            case "death":
                logDeaths = !logDeaths;
                sender.sendMessage(ChatColor.YELLOW + "Death logging: " + formatToggle(logDeaths));
                break;
            case "spawns":
            case "spawn":
                logSpawns = !logSpawns;
                sender.sendMessage(ChatColor.YELLOW + "Spawn logging: " + formatToggle(logSpawns));
                break;
            case "wool":
            case "wools":
                logWool = !logWool;
                sender.sendMessage(ChatColor.YELLOW + "Wool logging: " + formatToggle(logWool));
                break;
            case "all":
                boolean newState = !(logPositions && logDeaths && logSpawns && logWool);
                logPositions = logDeaths = logSpawns = logWool = newState;
                sender.sendMessage(ChatColor.YELLOW + "All logging: " + formatToggle(newState));
                break;
            default:
                sender.sendMessage(ChatColor.RED + "Unknown feature: " + feature);
                break;
        }
    }

    /**
     * Formats a boolean toggle state for display.
     */
    private String formatToggle(boolean enabled) {
        return enabled ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF";
    }

    // Match Lifecycle

    /**
     * Called when a PGM match starts.
     *
     * <p>Creates a new Parquet file in the map-specific folder with a timestamp filename.
     * The folder structure is: {@code data/{map_slug}/{timestamp}.parquet}
     *
     * <p>Map names are converted to slugs by lowercasing, replacing spaces with
     * underscores, and removing non-alphanumeric characters.
     *
     * @param mapName the name of the map being played
     * @param matchId the unique match identifier (currently unused)
     */
    public void onMatchStart(String mapName, String matchId) {
        getLogger().info("Match started: " + mapName);

        // Create map-specific folder
        String mapSlug = mapName.toLowerCase()
                .replace(" ", "_")
                .replaceAll("[^a-z0-9_]", "");

        File mapFolder = new File(dataFolder, mapSlug);
        if (!mapFolder.exists()) {
            mapFolder.mkdirs();
        }

        // Create parquet file with timestamp
        String filename = FILENAME_FORMAT.format(LocalDateTime.now()) + ".parquet";
        File parquetFile = new File(mapFolder, filename);

        try {
            positionTracker = new PositionTracker(parquetFile, permittedPlayers);
            startPositionTracking();
        } catch (IOException e) {
            getLogger().severe("Failed to create parquet file: " + e.getMessage());
        }
    }

    /**
     * Called when a PGM match ends.
     *
     * <p>Stops position tracking, closes the Parquet file, and logs the output filename.
     */
    public void onMatchEnd() {
        getLogger().info("Match ended.");
        stopPositionTracking();

        if (positionTracker != null) {
            getLogger().info("Data saved to: " + positionTracker.getFileName());
            positionTracker = null;
        }
    }

    // Position tracking

    /**
     * Starts periodic position sampling for all players.
     *
     * <p>Schedules a task that runs every 100 ticks (5 seconds) to sample player
     * positions, held items, and inventory counts.
     */
    private void startPositionTracking() {
        positionSamplerTask = Bukkit.getScheduler().runTaskTimer(this, () -> {
            if (positionTracker != null && logPositions) {
                positionTracker.sampleAllPlayers();
            }
        }, 100L, 100L);

        getLogger().info("Started position tracking (every 5 seconds)");
    }

    /**
     * Stops position sampling and closes the position tracker.
     *
     * <p>Cancels the scheduled sampling task and finalizes the Parquet file
     * with a match end event.
     */
    private void stopPositionTracking() {
        if (positionSamplerTask != null) {
            positionSamplerTask.cancel();
            positionSamplerTask = null;
        }

        if (positionTracker != null) {
            positionTracker.close();
        }
    }

    // Event logging

    /**
     * Logs a player death event if death logging is enabled.
     *
     * @param player the player who died
     * @param x the death x-coordinate
     * @param y the death y-coordinate
     * @param z the death z-coordinate
     */
    public void logDeath(Player player, int x, int y, int z) {
        if (positionTracker != null && logDeaths) {
            positionTracker.logDeath(player, x, y, z);
        }
    }

    /**
     * Logs a player spawn event if spawn logging is enabled.
     *
     * @param player the player who spawned
     * @param x the spawn x-coordinate
     * @param y the spawn y-coordinate
     * @param z the spawn z-coordinate
     */
    public void logSpawn(Player player, int x, int y, int z) {
        if (positionTracker != null && logSpawns) {
            positionTracker.logSpawn(player, x, y, z);
        }
    }

    /**
     * Logs a wool touch event if wool logging is enabled.
     *
     * @param player the player who touched the wool
     * @param x the touch x-coordinate
     * @param y the touch y-coordinate
     * @param z the touch z-coordinate
     * @param woolId the wool color name
     */
    public void logWoolTouch(Player player, int x, int y, int z, String woolId) {
        if (positionTracker != null && logWool) {
            positionTracker.logWoolTouch(player, x, y, z, woolId);
        }
    }

    /**
     * Logs a wool capture event if wool logging is enabled.
     *
     * @param player the player who captured the wool
     * @param x the capture x-coordinate
     * @param y the capture y-coordinate
     * @param z the capture z-coordinate
     * @param woolId the wool color name
     */
    public void logWoolCapture(Player player, int x, int y, int z, String woolId) {
        if (positionTracker != null && logWool) {
            positionTracker.logWoolCapture(player, x, y, z, woolId);
        }
    }
}