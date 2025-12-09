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
 * Main plugin class for PGM Logger.
 * Logs player positions, deaths, and wool events for CTW map analysis.
 *
 * Data is stored in Parquet format with folder structure:
 *   plugins/pgmlogger/data/{map_slug}/{timestamp}.parquet
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

    @Override
    public void onDisable() {
        stopPositionTracking();
        getLogger().info("PGM Logger disabled.");
    }

    // Commands

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

    private void showHelp(CommandSender sender) {
        sender.sendMessage(ChatColor.GOLD + "=== PGM Logger Commands ===");
        sender.sendMessage(ChatColor.YELLOW + "/pgmlogger status" + ChatColor.WHITE + " - Show current settings");
        sender.sendMessage(ChatColor.YELLOW + "/pgmlogger toggle <feature>" + ChatColor.WHITE + " - Toggle a feature");
        sender.sendMessage(ChatColor.GRAY + "Features: positions, deaths, spawns, wool, all");
    }

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

    private String formatStatus(String feature, boolean enabled) {
        String status = enabled ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF";
        return ChatColor.YELLOW + feature + ": " + status;
    }

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

    private String formatToggle(boolean enabled) {
        return enabled ? ChatColor.GREEN + "ON" : ChatColor.RED + "OFF";
    }

    // Match Lifecycle

    /**
     * Called when a PGM match starts.
     * Creates folder structure: data/{map_slug}/{timestamp}.parquet
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
            positionTracker = new PositionTracker(parquetFile, mapName, permittedPlayers);
            startPositionTracking();
        } catch (IOException e) {
            getLogger().severe("Failed to create parquet file: " + e.getMessage());
        }
    }

    /**
     * Called when a PGM match ends.
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

    private void startPositionTracking() {
        positionSamplerTask = Bukkit.getScheduler().runTaskTimer(this, () -> {
            if (positionTracker != null && logPositions) {
                positionTracker.sampleAllPlayers();
            }
        }, 100L, 100L);

        getLogger().info("Started position tracking (every 5 seconds)");
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

    // Event logging

    public void logDeath(Player player, int x, int y, int z, Player killer) {
        if (positionTracker != null && logDeaths) {
            positionTracker.logDeath(player, x, y, z, killer);
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

    // Getters

    public PermittedPlayers getPermittedPlayers() {
        return permittedPlayers;
    }
}