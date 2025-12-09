package com.example.pgmlogger;

import org.bukkit.Location;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import tc.oc.pgm.api.map.Gamemode;
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
 * Listens to PGM events and forwards them to our main plugin.
 *
 * Note: We pass Player objects instead of names for anonymous tracking.
 * The PositionTracker will assign sequential IDs internally.
 */
public class PGMEventListener implements Listener {

    private final Pgmlogger plugin;

    public PGMEventListener(Pgmlogger plugin) {
        this.plugin = plugin;
    }

    // =========================================================================
    // MATCH LIFECYCLE EVENTS
    // =========================================================================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onMatchStart(MatchStartEvent event) {
        boolean isCTW = event.getMatch().getModule(tc.oc.pgm.wool.WoolMatchModule.class) != null;
        if (!isCTW) return;

        plugin.getLogger().info("Map: " + event.getMatch().getMap().getName());
        plugin.getLogger().info("Gamemode: " + event.getMatch().getMap().getGamemode());
        plugin.getLogger().info("Gamemodes: " + event.getMatch().getMap().getGamemodes());

        String mapName = event.getMatch().getMap().getName();
        String matchId = event.getMatch().getId();
        plugin.onMatchStart(mapName, matchId);
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onMatchEnd(MatchFinishEvent event) {
        plugin.onMatchEnd();
    }

    // =========================================================================
    // PLAYER EVENTS
    // =========================================================================

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerSpawn(ParticipantSpawnEvent event) {
        MatchPlayer matchPlayer = event.getPlayer();
        if (matchPlayer == null) return;

        Player bukkitPlayer = matchPlayer.getBukkit();
        if (bukkitPlayer == null) return;

        Location loc = event.getLocation();
        plugin.logSpawn(bukkitPlayer, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerDeath(MatchPlayerDeathEvent event) {
        MatchPlayer victim = event.getVictim();
        if (victim == null) return;

        Player bukkitVictim = victim.getBukkit();
        if (bukkitVictim == null) return;

        Location loc = bukkitVictim.getLocation();

        // Get killer name (can be null)
        String killerName = null;
        ParticipantState killer = event.getKiller();
        if (killer != null) {
            MatchPlayer killerPlayer = killer.getPlayer().orElse(null);
            if (killerPlayer != null && killerPlayer.getBukkit() != null) {
                killerName = killerPlayer.getBukkit().getName();
            }
        }

        plugin.logDeath(bukkitVictim, (int) loc.getX(), (int) loc.getY(), (int) loc.getZ(), killerName);
    }

    // =========================================================================
    // WOOL EVENTS
    // =========================================================================

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