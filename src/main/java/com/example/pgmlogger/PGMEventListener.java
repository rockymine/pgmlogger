package com.example.pgmlogger;

import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;

import tc.oc.pgm.api.match.event.MatchFinishEvent;
import tc.oc.pgm.api.match.event.MatchStartEvent;
import tc.oc.pgm.api.player.MatchPlayer;
import tc.oc.pgm.api.player.ParticipantState;
import tc.oc.pgm.api.player.event.MatchPlayerDeathEvent;
import tc.oc.pgm.goals.events.GoalTouchEvent;
import tc.oc.pgm.spawns.events.ParticipantSpawnEvent;
import tc.oc.pgm.wool.MonumentWool;
import tc.oc.pgm.wool.PlayerWoolPlaceEvent;

/**
 * Listens to PGM  events and forwards them to the logging system.
 *
 * <p>This listener acts as a bridge between the PGM plugin's event system and
 * the Parquet-based match logger. It:
 * <ul>
 *   <li>Filters for Capture the Wool (CTW) matches only</li>
 *   <li>Extracts relevant data from PGM events</li>
 *   <li>Forwards Player objects (not names) to maintain privacy controls</li>
 *   <li>Uses MONITOR priority to observe events without interfering</li>
 * </ul>
 *
 * <p><b>Privacy Note:</b> Player objects are passed to the tracker rather than
 * names, allowing the PositionTracker to apply its anonymization logic based on
 * the permitted players list.
 */
public class PGMEventListener implements Listener {

    private final Pgmlogger plugin;

    /**
     * Creates a new PGM event listener.
     *
     * @param plugin the main plugin instance to forward events to
     */
    public PGMEventListener(Pgmlogger plugin) {
        this.plugin = plugin;
    }

    // MATCH LIFECYCLE EVENTS

    /**
     * Handles match start events, initializing logging for CTW matches only.
     *
     * <p>This handler checks if the match has a WoolMatchModule (indicating CTW mode).
     * Non-CTW matches are ignored. When a CTW match starts, it creates a new
     * Parquet file and begins tracking.
     *
     * @param event the match start event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onMatchStart(MatchStartEvent event) {
        boolean isCTW = event.getMatch().getModule(tc.oc.pgm.wool.WoolMatchModule.class) != null;
        if (!isCTW) return;

        String mapName = event.getMatch().getMap().getName();
        String matchId = event.getMatch().getId();
        plugin.onMatchStart(mapName, matchId);
    }
    /**
     * Handles match end events, finalizing and closing the match data file.
     *
     * <p>This stops position sampling and writes the match end event to the
     * Parquet file before closing it.
     *
     * @param event the match finish event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onMatchEnd(MatchFinishEvent event) {
        plugin.onMatchEnd();
    }

    // PLAYER EVENTS

    /**
     * Handles player spawn events, logging the spawn location.
     *
     * <p>This event fires when a player spawns or respawns during the match.
     * The spawn location is extracted from the event and logged.
     *
     * @param event the participant spawn event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerSpawn(ParticipantSpawnEvent event) {
        MatchPlayer matchPlayer = event.getPlayer();
        if (matchPlayer == null) return;

        Player bukkitPlayer = matchPlayer.getBukkit();
        if (bukkitPlayer == null) return;

        Location loc = event.getLocation();
        plugin.logSpawn(bukkitPlayer, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ());
    }

    /**
     * Handles player death events, logging the death location.
     *
     * <p>The victim's location at the time of death is recorded. This can be
     * used to analyze dangerous areas, engagement zones, or player behavior patterns.
     *
     * @param event the match player death event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerDeath(MatchPlayerDeathEvent event) {
        MatchPlayer victim = event.getVictim();
        if (victim == null) return;

        Player bukkitVictim = victim.getBukkit();
        if (bukkitVictim == null) return;

        Location loc = bukkitVictim.getLocation();

        plugin.logDeath(bukkitVictim, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ());
    }

    // WOOL EVENTS

    /**
     * Handles wool touch events when a player picks up wool for the first time in their life.
     *
     * <p>This event is filtered to only log the first touch of a wool objective
     * during a player's current life (using {@code isFirstForPlayerLife()}). This
     * prevents duplicate logs if a player drops and re-picks the same wool.
     *
     * <p>Only processes events for MonumentWool goals (CTW objectives).
     *
     * @param event the goal touch event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onWoolTouch(GoalTouchEvent event) {
        if (!event.isFirstForPlayerLife()) return;
        if (!(event.getGoal() instanceof MonumentWool)) return;

        MonumentWool wool = (MonumentWool) event.getGoal();
        String woolId = wool.getDyeColor().toString();

        ParticipantState player = event.getPlayer();
        if (player == null) return;

        MatchPlayer matchPlayer = player.getPlayer().orElse(null);
        if (matchPlayer == null || matchPlayer.getBukkit() == null) return;

        Player bukkitPlayer = matchPlayer.getBukkit();
        Location loc = bukkitPlayer.getLocation();

        plugin.logWoolTouch(bukkitPlayer, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ(), woolId);
    }

    /**
     * Handles wool capture events when a player places wool at the objective.
     *
     * <p>This event fires when a wool block is successfully placed at a monument,
     * completing the objective. The location logged is where the wool block was
     * placed, not the player's position.
     *
     * @param event the player wool place event
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onWoolPlace(PlayerWoolPlaceEvent event) {
        ParticipantState participant = event.getPlayer();
        if (participant == null) return;

        MatchPlayer matchPlayer = participant.getPlayer().orElse(null);
        if (matchPlayer == null || matchPlayer.getBukkit() == null) return;

        Player bukkitPlayer = matchPlayer.getBukkit();
        Location loc = event.getBlock().getLocation();
        String woolId = event.getWool().getDyeColor().toString();

        plugin.logWoolCapture(bukkitPlayer, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ(), woolId);
    }
}