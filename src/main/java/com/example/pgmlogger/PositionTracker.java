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
 * Tracks and logs player positions and match events to a Parquet file.
 *
 * <p>This tracker records various match events including:
 * <ul>
 *   <li>Match lifecycle (start/end)</li>
 *   <li>Player spawns and deaths</li>
 *   <li>Player position samples (with held item and inventory state)</li>
 *   <li>Wool objectives (touch and capture events)</li>
 * </ul>
 *
 * <p><b>Player Privacy:</b> Players are identified by name if they appear in the
 * permitted players list, otherwise they receive an anonymous numeric identifier.
 *
 * <p><b>Position Tracking:</b> Player positions are sampled periodically via
 * {@link #sampleAllPlayers()}. To reduce file size, positions are only logged
 * when a player moves to a new block coordinate.
 *
 * <p><b>Lifecycle:</b> Create an instance at match start, call logging methods
 * throughout the match, and call {@link #close()} at match end to finalize the file.
 *
 * <p><b>Thread Safety:</b> Write operations are synchronized to prevent concurrent
 * modification of the Parquet file.
 */
public class PositionTracker {

    private final File file;
    private final ParquetWriter<MatchEvent> writer;
    private final long matchStartTime;
    private final PermittedPlayers permittedPlayers;
    private final Map<UUID, Integer> playerIds = new HashMap<>();
    private int nextAnonymousId = 0;
    private final Map<UUID, String> lastPositions = new HashMap<>();

    /**
     * Creates a new position tracker and initializes the match data file.
     *
     * <p>This constructor immediately:
     * <ul>
     *   <li>Records the match start time for timestamp calculations</li>
     *   <li>Creates a Parquet writer configured for MatchEvent objects</li>
     *   <li>Writes a MATCH_START event to the file</li>
     * </ul>
     *
     * @param file the output Parquet file where events will be written
     * @param permittedPlayers the list of players whose names may be logged
     * @throws IOException if the Parquet file cannot be created or written to
     */
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

    /**
     * Retrieves or generates a unique identifier for the given player.
     *
     * @param uuid the unique identifier of the player
     * @return the player's name if permitted, otherwise an anonymous number
     */
    public int getPlayerId(UUID uuid) {
        return playerIds.computeIfAbsent(uuid, id -> {
            if (permittedPlayers.isPermitted(uuid)) {
                return permittedPlayers.getPlayerId(uuid);
            } else {
                return nextAnonymousId++;
            }
        });
    }

    public int getPlayerId(Player player) {
        return getPlayerId(player.getUniqueId());
    }

    /**
     * Writes a MatchEvent into a parquet file.
     *
     * @param event the given MatchEvent
     */
    private synchronized void write(MatchEvent event) {
        try {
            writer.write(event);
        } catch (IOException e) {
            Bukkit.getLogger().log(Level.WARNING, "Failed to write match event", e);
        }
    }

    /**
     * Returns the elapsed time since the match started.
     *
     * @return the number of seconds since match start, as an integer.
     */
    private int getTimestamp() {
        return (int) ((System.currentTimeMillis() - matchStartTime) / 1000);
    }

    /**
     * Logs a player spawn event to the match record.
     *
     * @param player the player who spawned.
     * @param x the x coordinate of the spawn location.
     * @param y the y coordinate of the spawn location.
     * @param z the z coordinate of the spawn location.
     */
    public void logSpawn(Player player, int x, int y, int z) {
        int playerId = getPlayerId(player.getUniqueId());
        clearLastPosition(player.getUniqueId());
        write(MatchEvent.spawn(getTimestamp(), playerId, x, y, z));
    }

    /**
     * Logs a player death event to the match record.
     *
     * @param player the player who died.
     * @param x the x coordinate of the death location.
     * @param y the y coordinate of the death location.
     * @param z the z coordinate of the death location.
     */
    public void logDeath(Player player, int x, int y, int z) {
        int playerId = getPlayerId(player.getUniqueId());
        write(MatchEvent.death(getTimestamp(), playerId, x, y, z));
    }

    /**
     * Logs a player wool touch event to the match record.
     *
     * @param player the player who touched the wool.
     * @param x the x coordinate of the wool touch location.
     * @param y the y coordinate of the wool touch location.
     * @param z the z coordinate of the wool touch location.
     * @param woolColor the color of the touched wool.
     */
    public void logWoolTouch(Player player, int x, int y, int z, String woolColor) {
        int playerId = getPlayerId(player.getUniqueId());
        int woolId = resolveWoolId(woolColor);
        write(MatchEvent.woolTouch(getTimestamp(), playerId, x, y, z, woolId));
    }

    /**
     * Logs a player wool capture event to the match record.
     *
     * @param player the player who captured the wool.
     * @param x the x coordinate of the wool capture location.
     * @param y the y coordinate of the wool capture location.
     * @param z the z coordinate of the wool capture location.
     * @param woolColor the color of the captured wool.
     */
    public void logWoolCapture(Player player, int x, int y, int z, String woolColor) {
        int playerId = getPlayerId(player.getUniqueId());
        int woolId = resolveWoolId(woolColor);
        write(MatchEvent.woolCapture(getTimestamp(), playerId, x, y, z, woolId));
    }

    /**
     * Samples all online players who are participating.
     */
    public void sampleAllPlayers() {
        for (Player player : Bukkit.getOnlinePlayers()) {
            samplePlayer(player);
        }
    }

    /**
     * Samples and logs the current state of a participating player.
     *
     * <p>This method is typically called periodically to track player movement and state
     * throughout the match. It performs several checks before logging:
     * <ul>
     *   <li>Skips if the player is not in an active match</li>
     *   <li>Skips observers (non-participating players)</li>
     *   <li>Skips if the player hasn't moved since the last sample</li>
     * </ul>
     *
     * <p>When logged, the event captures the player's position, currently held item,
     * and total inventory item count. The last known position is cached to avoid
     * redundant log entries.
     *
     * @param player the player to sample
     */
    private void samplePlayer(Player player) {
        // Check if a match exists for the given player
        Match match = PGM.get().getMatchManager().getMatch(player);
        if (match == null) return;

        // Check if a player is participating in the match
        MatchPlayer matchPlayer = match.getPlayer(player);
        if (matchPlayer == null || !matchPlayer.isParticipating()) {
            return;
        }

        // Get the player's current location
        Location loc = player.getLocation();
        int x = (int) loc.getX();
        int y = (int) loc.getY();
        int z = (int) loc.getZ();

        // Check if the player's position has changed
        String posKey = x + "," + y + "," + z;
        UUID uuid = player.getUniqueId();

        if (posKey.equals(lastPositions.get(uuid))) {
            return;
        }
        lastPositions.put(uuid, posKey);

        // Get player state
        int playerId = getPlayerId(player);
        int heldItem = player.getItemInHand().getType().ordinal();
        int invCount = countInventoryItems(player);

        // Write position event
        write(MatchEvent.position(getTimestamp(), playerId, x, y, z, heldItem, invCount));
    }

    /**
     * Clear the cached position for a player, forcing their next position to be logged.
     *
     * @param playerUuid the player's unique identifier.
     */
    public void clearLastPosition(UUID playerUuid) {
        lastPositions.remove(playerUuid);
    }

    /**
     * Counts the total items in a player's inventory and armor slots.
     *
     * @param player the player whose inventory to count.
     * @return the total item count across inventory and armor.
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

    /**
     * Converts a wool color name to its numeric identifier.
     *
     * @param colorName the wool color name (e.g., "RED", "BLUE").
     * @return the color's ordinal value, or -1 if the color name is invalid.
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
     * Returns the name of the parquet output file.
     *
     * @return the file name.
     */
    public String getFileName() {
        return file.getName();
    }

    /**
     * Closes the tracker and finalizes the match data file.
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