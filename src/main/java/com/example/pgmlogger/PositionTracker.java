package com.example.pgmlogger;

import blue.strategic.parquet.ParquetWriter;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import tc.oc.pgm.api.PGM;
import tc.oc.pgm.api.match.Match;
import tc.oc.pgm.api.player.MatchPlayer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Tracks player positions and writes data to Parquet format.
 *
 * Parquet benefits:
 * - Binary format, much smaller than CSV (~80% reduction)
 * - Built-in compression
 * - Native support in pandas, spark, etc.
 * - Schema embedded in file
 */
public class PositionTracker {

    // =========================================================================
    // FIELDS
    // =========================================================================

    private final File file;
    private final ParquetWriter<MatchEvent> writer;
    private final long matchStartTime;
    private final PermittedPlayers permittedPlayers;

    // Player tracking
    private final Map<UUID, String> playerIdentifiers = new HashMap<>();
    private int nextAnonymousId = 0;

    // Position change detection
    private final Map<UUID, String> lastPositions = new HashMap<>();

    // =========================================================================
    // CONSTRUCTOR
    // =========================================================================

    public PositionTracker(File file, PermittedPlayers permittedPlayers) throws IOException {
        this.file = file;
        this.matchStartTime = System.currentTimeMillis();
        this.permittedPlayers = permittedPlayers;

        // Create parquet writer
        this.writer = ParquetWriter.writeFile(
                MatchEvent.SCHEMA,
                file,
                MatchEvent.Serializer.INSTANCE
        );

        // Write match start event
        write(MatchEvent.matchStart());
    }

    // =========================================================================
    // PLAYER ID MANAGEMENT (Anonymous)
    // =========================================================================

    /**
     * Get player identifier - real name if permitted, anonymous number if not.
     */
    public String getPlayerIdentifier(UUID uuid) {
        return playerIdentifiers.computeIfAbsent(uuid, id -> {
            if (permittedPlayers.isPermitted(uuid)) {
                String name = permittedPlayers.getPermittedName(uuid);
                return name != null ? name : String.valueOf(nextAnonymousId++);
            } else {
                return String.valueOf(nextAnonymousId++);
            }
        });
    }

    public String getPlayerIdentifier(Player player) {
        return getPlayerIdentifier(player.getUniqueId());
    }

    // =========================================================================
    // EVENT LOGGING
    // =========================================================================

    /**
     * Write an event to the parquet file.
     */
    private synchronized void write(MatchEvent event) {
        try {
            writer.write(event);
        } catch (IOException e) {
            Bukkit.getLogger().log(Level.WARNING, "Failed to write match event", e);
        }
    }

    /**
     * Get current timestamp in seconds since match start.
     */
    private int getTimestamp() {
        return (int) ((System.currentTimeMillis() - matchStartTime) / 1000);
    }

    /**
     * Log a spawn event.
     */
    public void logSpawn(Player player, int x, int y, int z) {
        String playerId = getPlayerIdentifier(player.getUniqueId());
        clearLastPosition(player.getUniqueId());
        write(MatchEvent.spawn(getTimestamp(), playerId, x, y, z));
    }

    /**
     * Log a death event.
     */
    public void logDeath(Player player, int x, int y, int z) {
        String playerId = getPlayerIdentifier(player.getUniqueId());
        write(MatchEvent.death(getTimestamp(), playerId, x, y, z));
    }

    /**
     * Log a wool touch event.
     */
    public void logWoolTouch(Player player, int x, int y, int z, String woolColor) {
        String playerId = getPlayerIdentifier(player.getUniqueId());
        int woolId = resolveWoolId(woolColor);
        write(MatchEvent.woolTouch(getTimestamp(), playerId, x, y, z, woolId));
    }

    /**
     * Log a wool capture event.
     */
    public void logWoolCapture(Player player, int x, int y, int z, String woolColor) {
        String playerId = getPlayerIdentifier(player.getUniqueId());
        int woolId = resolveWoolId(woolColor);
        write(MatchEvent.woolCapture(getTimestamp(), playerId, x, y, z, woolId));
    }

    // =========================================================================
    // POSITION SAMPLING
    // =========================================================================

    /**
     * Sample all online players who are participating.
     */
    public void sampleAllPlayers() {
        for (Player player : Bukkit.getOnlinePlayers()) {
            samplePlayer(player);
        }
    }

    /**
     * Sample a single player's position and state.
     * Skips if player hasn't moved since last sample.
     */
    private void samplePlayer(Player player) {
        Match match = PGM.get().getMatchManager().getMatch(player);
        if (match == null) return;

        MatchPlayer matchPlayer = match.getPlayer(player);
        if (matchPlayer == null || !matchPlayer.isParticipating()) {
            return; // Skip observers
        }

        Location loc = player.getLocation();
        int x = (int) loc.getX();
        int y = (int) loc.getY();
        int z = (int) loc.getZ();

        // Check if position changed
        String posKey = x + "," + y + "," + z;
        UUID uuid = player.getUniqueId();
        UUID logUuid = getLoggableUUID(player);

        if (posKey.equals(lastPositions.get(uuid))) {
            return; // Player hasn't moved, skip
        }
        lastPositions.put(uuid, posKey);

        // Get player state
        String playerId = getPlayerIdentifier(player);
        int heldItem = player.getItemInHand().getType().ordinal();
        int invCount = countInventoryItems(player);

        // Write position event
        write(MatchEvent.position(getTimestamp(), playerId, x, y, z, heldItem, invCount));
    }

    /**
     * Clear last position for a player (called on spawn to ensure it's logged).
     */
    public void clearLastPosition(UUID playerUuid) {
        lastPositions.remove(playerUuid);
    }

    // =========================================================================
    // PLAYER STATE HELPERS
    // =========================================================================

    /**
     * Count total items in player's inventory.
     */
    private int countInventoryItems(Player player) {
        int count = 0;
        for (ItemStack item : player.getInventory().getContents()) {
            if (item != null && item.getType() != Material.AIR) {
                count += item.getAmount();
            }
        }
        for (ItemStack item : player.getInventory().getArmorContents()) {
            if (item != null && item.getType() != Material.AIR) {
                count += item.getAmount();
            }
        }
        return count;
    }

    // =========================================================================
    // UTILITY METHODS
    // =========================================================================

    /**
     * @param player player
     * @return real or anonymized UUID of the player
     */
    private UUID getLoggableUUID(Player player) {
        UUID realUuid = player.getUniqueId();
        if (permittedPlayers.isPermitted(realUuid)) {
            return realUuid;
        } else {
            // Create a fake UUID based on the player's real UUID so it's consistent for this match
            // but can't be reversed easily to find who it really is.
            return UUID.nameUUIDFromBytes(("ANON_" + realUuid.toString()).getBytes());
        }
    }

    /**
     * @param colorName color name
     * @return converted color id
     */
    private int resolveWoolId(String colorName) {
        try {
            // limit usage of string by converting to ordinal immediately
            return org.bukkit.DyeColor.valueOf(colorName).ordinal();
        } catch (IllegalArgumentException e) {
            return -1; // Unknown color
        }
    }

    /**
     * Get the output file name.
     */
    public String getFileName() {
        return file.getName();
    }

    /**
     * Close the tracker and finalize the file.
     */
    public void close() {
        // Write match end event
        write(MatchEvent.matchEnd(getTimestamp()));

        try {
            writer.close();
        } catch (IOException e) {
            Bukkit.getLogger().log(Level.WARNING, "Failed to close parquet writer", e);
        }
    }
}