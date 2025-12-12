package com.github.rockymine.pgmlogger;

import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.ValueWriter;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Represents a single event in a match, stored in Parquet format.
 *
 * <p>This class uses a sparse schema where not all fields are populated for
 * every event type. The {@code eventType} determines which fields contain data:
 * <ul>
 *   <li>{@code MATCH_START/MATCH_END}: Only timestamp and event type</li>
 *   <li>{@code SPAWN/DEATH}: timestamp, event type, player ID, and position (x,y,z)</li>
 *   <li>{@code POSITION}: All fields except woolId</li>
 *   <li>{@code WOOL_TOUCH/WOOL_CAPTURE}: timestamp, event type, player ID, position, and woolId</li>
 * </ul>
 *
 * <p>Instances are immutable and should be created using the static factory methods.
 */
public class MatchEvent {

    public static final MessageType SCHEMA = new MessageType("match_event",
            // Required fields (present in every event)
            Types.required(PrimitiveTypeName.INT32).named("timestamp"),
            Types.required(PrimitiveTypeName.INT32).named("event_type"),

            // Optional fields (can be null depending on event type)
            Types.optional(PrimitiveTypeName.INT32).named("player_id"),
            Types.optional(PrimitiveTypeName.INT32).named("x"),
            Types.optional(PrimitiveTypeName.INT32).named("y"),
            Types.optional(PrimitiveTypeName.INT32).named("z"),
            Types.optional(PrimitiveTypeName.INT32).named("held_item"),
            Types.optional(PrimitiveTypeName.INT32).named("inventory_count"),
            Types.optional(PrimitiveTypeName.INT32).named("wool_id") // wool id
    );

    private final int timestamp;
    private final EventType eventType;
    private final Integer playerId;
    private final Integer x, y, z;
    private final Integer heldItem;
    private final Integer invCount;
    public final Integer woolId;

    /**
     * Constructs a match event with the specified fields.
     *
     * <p>This constructor should generally not be called directly. Use the static
     * factory methods instead ({@link #spawn}, {@link #death}, etc.) which ensure
     * the correct fields are populated for each event type.
     *
     * @param timestamp seconds since match start
     * @param eventType the type of event
     * @param playerId the player id (negative or anonymous ID), null for match-level events
     * @param x the x-coordinate, null if not applicable
     * @param y the y-coordinate, null if not applicable
     * @param z the z-coordinate, null if not applicable
     * @param heldItem the held item type ordinal, null if not applicable
     * @param invCount the total inventory count, null if not applicable
     * @param woolId the wool color ordinal, null if not applicable
     */
    public MatchEvent(int timestamp, EventType eventType, Integer playerId, Integer x, Integer y, Integer z,
                      Integer heldItem, Integer invCount, Integer woolId) {
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.playerId = playerId;
        this.x = x;
        this.y = y;
        this.z = z;
        this.heldItem = heldItem;
        this.invCount = invCount;
        this.woolId = woolId;
    }

    /**
     * Creates a match start event with timestamp 0.
     *
     * @return a MATCH_START event
     */
    public static MatchEvent matchStart() {
        return new MatchEvent(0, EventType.MATCH_START, null, null, null, null, null, null, null);
    }

    /**
     * Creates a match end event.
     *
     * @param timestamp seconds since match start
     * @return a MATCH_END event
     */
    public static MatchEvent matchEnd(int timestamp) {
        return new MatchEvent(timestamp, EventType.MATCH_END, null, null, null, null, null, null, null);
    }

    /**
     * Creates a player spawn event.
     *
     * @param timestamp seconds since match start
     * @param playerId the player's identifier
     * @param x the spawn x-coordinate
     * @param y the spawn y-coordinate
     * @param z the spawn z-coordinate
     * @return a SPAWN event
     */
    public static MatchEvent spawn(int timestamp, int playerId, int x, int y, int z) {
        return new MatchEvent(timestamp, EventType.SPAWN, playerId, x, y, z, null, null, null);
    }

    /**
     * Creates a player death event.
     *
     * @param timestamp seconds since match start
     * @param playerId the player's identifier
     * @param x the death x-coordinate
     * @param y the death y-coordinate
     * @param z the death z-coordinate
     * @return a DEATH event
     */
    public static MatchEvent death(int timestamp, int playerId, int x, int y, int z) {
        return new MatchEvent(timestamp, EventType.DEATH, playerId, x, y, z, null, null, null);
    }

    /**
     * Creates a player position sample event.
     *
     * @param timestamp seconds since match start
     * @param playerId the player's identifier
     * @param x the player's x-coordinate
     * @param y the player's y-coordinate
     * @param z the player's z-coordinate
     * @param heldItem the held item type ordinal
     * @param invCount the total inventory item count
     * @return a POSITION event
     */
    public static MatchEvent position(int timestamp, int playerId, int x, int y, int z,
                                      Integer heldItem, int invCount) {
        return new MatchEvent(timestamp, EventType.POSITION, playerId, x, y, z, heldItem, invCount, null);
    }

    /**
     * Creates a wool touch event (when a player picks up wool).
     *
     * @param timestamp seconds since match start
     * @param playerId the player's identifier
     * @param x the touch x-coordinate
     * @param y the touch y-coordinate
     * @param z the touch z-coordinate
     * @param woolColor the wool color ordinal
     * @return a WOOL_TOUCH event
     */
    public static MatchEvent woolTouch(int timestamp, int playerId, int x, int y, int z, Integer woolColor) {
        return new MatchEvent(timestamp, EventType.WOOL_TOUCH, playerId, x, y, z, null, null, woolColor);
    }

    /**
     * Creates a wool capture event (when wool is placed at the objective).
     *
     * @param timestamp seconds since match start
     * @param playerId the player's identifier
     * @param x the capture x-coordinate
     * @param y the capture y-coordinate
     * @param z the capture z-coordinate
     * @param woolColor the wool color ordinal
     * @return a WOOL_CAPTURE event
     */
    public static MatchEvent woolCapture(int timestamp, int playerId, int x, int y, int z, Integer woolColor) {
        return new MatchEvent(timestamp, EventType.WOOL_CAPTURE, playerId, x, y, z, null, null, woolColor);
    }

    public static class Serializer implements Dehydrator<MatchEvent> {
        public static final Serializer INSTANCE = new Serializer();

        /**
         * Serializes a MatchEvent to Parquet format.
         *
         * <p>Required fields (timestamp, event_type) are always written.
         * Optional fields are only written if non-null.
         *
         * @param event the event to serialize
         * @param writer the Parquet value writer
         */
        @Override
        public void dehydrate(MatchEvent event, ValueWriter writer) {
            // Required fields
            writer.write("timestamp", event.timestamp);
            int eventTypeOrdinal = event.eventType.ordinal();
            writer.write("event_type", eventTypeOrdinal);

            // Optional fields - only write if not null
            if (event.playerId != null) writer.write("player_id", event.playerId);
            if (event.x != null) writer.write("x", event.x);
            if (event.y != null) writer.write("y", event.y);
            if (event.z != null) writer.write("z", event.z);
            if (event.heldItem != null) writer.write("held_item", event.heldItem);
            if (event.invCount != null) writer.write("inventory_count", event.invCount);
            if (event.woolId != null) writer.write("wool_id", event.woolId);
        }
    }
}