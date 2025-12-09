package com.example.pgmlogger;

import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ValueWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Represents a single event in a match log.
 */
public class MatchEvent {

    // Parquet schema

    public static final MessageType SCHEMA = new MessageType("match_event",
            // Required fields (present in every event)
            Types.required(PrimitiveTypeName.INT32).named("timestamp"),      // timestamp (seconds since match start)
            Types.required(PrimitiveTypeName.INT32).named("event_type"),  // event type

            // Optional fields (may be null depending on event type)
            Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("player_id"),      // player ID
            Types.optional(PrimitiveTypeName.INT32).named("x"),      // x coordinate
            Types.optional(PrimitiveTypeName.INT32).named("y"),      // y coordinate
            Types.optional(PrimitiveTypeName.INT32).named("z"),      // z coordinate
            Types.optional(PrimitiveTypeName.INT32).named("held_item"),  // held item
            Types.optional(PrimitiveTypeName.INT32).named("inventory_count"),      // inventory count

            Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("killer_id"),

            Types.optional(PrimitiveTypeName.INT32)
                    .named("wool_id"), // wool id

            Types.optional(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("map_name") // map name
    );

    // Fields

    private final int timestamp;        // seconds since match start
    private final EventType eventType;     // MS, ME, S, D, P, WT, WC
    private final String playerId;     // null for MS/ME events
    private final Integer x, y, z;      // null for MS/ME events
    private final Integer heldItem;      // only for P events
    private final Integer invCount;     // only for P events
    private final String killerId;
    public final Integer woolId; // DyeColor ordinal
    public final String mapName;

    // Constructors

    /**
     * Full constructor.
     */
    public MatchEvent(int timestamp, EventType eventType, String playerId,
                      Integer x, Integer y, Integer z,
                      Integer heldItem, Integer invCount, String killerId, Integer woolId, String mapName) {
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.playerId = playerId;
        this.x = x;
        this.y = y;
        this.z = z;
        this.heldItem = heldItem;
        this.invCount = invCount;
        this.killerId = killerId;
        this.woolId = woolId;
        this.mapName = mapName;
    }

    // Factory methods for events

    /**
     * Create a match start event.
     */
    public static MatchEvent matchStart(String mapName) {
        return new MatchEvent(0, EventType.MATCH_START, null, null, null, null, null, null, null, null, mapName);
    }

    /**
     * Create a match end event.
     */
    public static MatchEvent matchEnd(int timestamp) {
        return new MatchEvent(timestamp, EventType.MATCH_END, null, null, null, null, null, null, null, null, null);
    }

    /**
     * Create a spawn event.
     */
    public static MatchEvent spawn(int timestamp, String playerId, int x, int y, int z) {
        return new MatchEvent(timestamp, EventType.SPAWN, playerId, x, y, z, null, null, null, null, null);
    }

    /**
     * Create a death event.
     */
    public static MatchEvent death(int timestamp, String playerId, int x, int y, int z, String killerName) {
        return new MatchEvent(timestamp, EventType.DEATH, playerId, x, y, z, null, null, killerName, null, null);
    }

    /**
     * Create a position event.
     */
    public static MatchEvent position(int timestamp, String playerId, int x, int y, int z,
                                      Integer heldItem, int invCount) {
        return new MatchEvent(timestamp, EventType.POSITION, playerId, x, y, z, heldItem, invCount, null, null, null);
    }

    /**
     * Create a wool touch event.
     */
    public static MatchEvent woolTouch(int timestamp, String playerId, int x, int y, int z, Integer woolColor) {
        return new MatchEvent(timestamp, EventType.WOOL_TOUCH, playerId, x, y, z, null, null, null, woolColor, null);
    }

    /**
     * Create a wool capture event.
     */
    public static MatchEvent woolCapture(int timestamp, String playerId, int x, int y, int z, Integer woolColor) {
        return new MatchEvent(timestamp, EventType.WOOL_CAPTURE, playerId, x, y, z, null, null, null, woolColor, null);
    }

    // Serializer

    public static class Serializer implements Dehydrator<MatchEvent> {
        public static final Serializer INSTANCE = new Serializer();

        @Override
        public void dehydrate(MatchEvent event, ValueWriter writer) {
            // Required fields
            writer.write("timestamp", event.timestamp);
            int eventTypeOrdinal = event.eventType.ordinal();
            writer.write("event_type", eventTypeOrdinal);

            // Optional fields - only write if not null
            if (event.playerId != null) {
                writer.write("player_id", event.playerId);  // e.g. "fe3608b7-d105-4029-8800-34b3147065b6"
            }
            if (event.x != null) writer.write("x", event.x);
            if (event.y != null) writer.write("y", event.y);
            if (event.z != null) writer.write("z", event.z);
            if (event.heldItem != null) writer.write("held_item", event.heldItem);
            if (event.invCount != null) writer.write("inventory_count", event.invCount);
            if (event.killerId != null) {
                writer.write("killer_id", event.killerId);
            }
            if (event.woolId != null) writer.write("wool_id", event.woolId);
            if (event.mapName != null) writer.write("map_name", event.mapName);
        }

        private byte[] uuidToBytes(UUID uuid) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.order(ByteOrder.LITTLE_ENDIAN); // Or BIG_ENDIAN depending on your downstream tool preference
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        }
    }
}