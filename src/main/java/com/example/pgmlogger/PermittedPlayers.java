package com.example.pgmlogger;

import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Manages the list of players who have given permission to use their real names.
 * Players not in this list will be anonymized in the data.
 */
public class PermittedPlayers {

    private final JavaPlugin plugin;
    private final Map<UUID, String> permittedPlayers = new HashMap<>();
    private final File configFile;

    public PermittedPlayers(JavaPlugin plugin) {
        this.plugin = plugin;
        this.configFile = new File(plugin.getDataFolder(), "permitted-players.yml");
        load();
    }

    /**
     * Load permitted players from config file.
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
                    String name = config.getString("permitted." + uuidStr);
                    permittedPlayers.put(uuid, name);
                } catch (IllegalArgumentException e) {
                    plugin.getLogger().warning("Invalid UUID in permitted-players.yml: " + uuidStr);
                }
            }
        }

        plugin.getLogger().info("Loaded " + permittedPlayers.size() + " permitted players");
    }

    /**
     * Check if a player has given permission to use their real name.
     */
    public boolean isPermitted(UUID uuid) {
        return permittedPlayers.containsKey(uuid);
    }

    /**
     * Get the player's name if permitted, or null if not.
     */
    public String getPermittedName(UUID uuid) {
        return permittedPlayers.get(uuid);
    }

    /**
     * Get count of permitted players.
     */
    public int getCount() {
        return permittedPlayers.size();
    }
}