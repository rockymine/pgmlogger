package com.example.pgmlogger;

import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Manages the list of players who have given permission to use their real names in logs.
 *
 * <p>Players not in this list will be assigned anonymous numeric identifiers instead
 * of having their usernames recorded in match data files.
 *
 * <p>The permitted players list is stored in {@code permitted-players.yml} with the format:
 * <pre>
 * permitted:
 *   uuid-string: "PlayerName"
 * </pre>
 */
public class PermittedPlayers {

    private final JavaPlugin plugin;
    private final Map<UUID, Integer> permittedPlayers = new HashMap<>();
    private final File configFile;

    /**
     * Creates a new permitted players manager and loads the initial list.
     *
     * @param plugin the plugin instance, used for accessing the data folder and resources
     */
    public PermittedPlayers(JavaPlugin plugin) {
        this.plugin = plugin;
        this.configFile = new File(plugin.getDataFolder(), "permitted-players.yml");
        load();
    }

    /**
     * Loads or reloads the permitted players list from the configuration file.
     *
     * <p>If the configuration file doesn't exist, a default template is created from
     * the plugin's resources. Invalid UUIDs in the config file are logged and skipped.
     *
     * <p>This method clears the existing list before loading, so it can be called to
     * reload changes made to the config file.
     */
    public void load() {
        permittedPlayers.clear();

        // Create default config if it doesn't exist
        if (!configFile.exists()) {
            plugin.saveResource("permitted-players.yml", false);
        }

        // Load the config
        YamlConfiguration config = YamlConfiguration.loadConfiguration(configFile);

        // Also load defaults from JAR for reference
        InputStream defaultStream = plugin.getResource("permitted-players.yml");
        if (defaultStream != null) {
            YamlConfiguration defaultConfig = YamlConfiguration.loadConfiguration(
                    new InputStreamReader(defaultStream)
            );
            config.setDefaults(defaultConfig);
        }

        // Read permitted players
        if (config.contains("permitted")) {
            for (String uuidStr : config.getConfigurationSection("permitted").getKeys(false)) {
                try {
                    UUID uuid = UUID.fromString(uuidStr);
                    int playerId = config.getInt("permitted." + uuidStr);
                    permittedPlayers.put(uuid, playerId);
                } catch (IllegalArgumentException e) {
                    plugin.getLogger().warning("Invalid UUID in permitted-players.yml: " + uuidStr);
                }
            }
        }

        plugin.getLogger().info("Loaded " + permittedPlayers.size() + " permitted players");
    }

    /**
     * Checks if a player has given permission to use their real name in logs.
     *
     * @param uuid the player's unique identifier
     * @return true if the player is in the permitted list, false otherwise
     */
    public boolean isPermitted(UUID uuid) {
        return permittedPlayers.containsKey(uuid);
    }

    /**
     * Get the fixed player ID for a permitted player.
     * @return negative player ID, or null if not permitted
     */
    public Integer getPlayerId(UUID uuid) {
        return permittedPlayers.get(uuid);
    }
}