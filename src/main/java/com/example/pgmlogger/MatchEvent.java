package com.example.pgmlogger;

import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ValueWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/**
 * Represents a single event in a match log.
 * Event types:
 *   MS = Match Start
 *   ME = Match End
 *   S  = Spawn
 *   D  = Death
 *   P  = Position
 *   WT = Wool Touch
 *   WC = Wool Capture
 */
public class MatchEvent {

    // Parquet schema

    public static final MessageType SCHEMA = new MessageType("match_event",
            // Required fields (present in every event)
            Types.required(INT32).named("t"),      // timestamp (seconds since match start)
            Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("e"),  // event type

            // Optional fields (may be null depending on event type)
            Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("p"),      // player ID
            Types.optional(INT32).named("x"),      // x coordinate
            Types.optional(INT32).named("y"),      // y coordinate
            Types.optional(INT32).named("z"),      // z coordinate
            Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("h"),  // held item
            Types.optional(INT32).named("n"),      // inventory count
            Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("i")   // extra info
    );

    // Fields

    private final int timestamp;        // seconds since match start
    private final String eventType;     // MS, ME, S, D, P, WT, WC
    private final String playerId;     // null for MS/ME events
    private final Integer x, y, z;      // null for MS/ME events
    private final String heldItem;      // only for P events
    private final Integer invCount;     // only for P events
    private final String extraInfo;     // killer name, wool color, map name, etc.

    // Constructors

    /**
     * Full constructor.
     */
    public MatchEvent(int timestamp, String eventType, String playerId,
                      Integer x, Integer y, Integer z,
                      String heldItem, Integer invCount, String extraInfo) {
        this.timestamp = timestamp;
        this.eventType = eventType;
        this.playerId = playerId;
        this.x = x;
        this.y = y;
        this.z = z;
        this.heldItem = heldItem;
        this.invCount = invCount;
        this.extraInfo = extraInfo;
    }

    // Factory methods for events

    /**
     * Create a match start event.
     */
    public static MatchEvent matchStart(String mapName) {
        return new MatchEvent(0, "MS", null, null, null, null, null, null, mapName);
    }

    /**
     * Create a match end event.
     */
    public static MatchEvent matchEnd(int timestamp) {
        return new MatchEvent(timestamp, "ME", null, null, null, null, null, null, null);
    }

    /**
     * Create a spawn event.
     */
    public static MatchEvent spawn(int timestamp, String playerId, int x, int y, int z) {
        return new MatchEvent(timestamp, "S", playerId, x, y, z, null, null, null);
    }

    /**
     * Create a death event.
     */
    public static MatchEvent death(int timestamp, String playerId, int x, int y, int z, String killerName) {
        return new MatchEvent(timestamp, "D", playerId, x, y, z, null, null, killerName);
    }

    /**
     * Create a position event.
     */
    public static MatchEvent position(int timestamp, String playerId, int x, int y, int z,
                                      String heldItem, int invCount) {
        return new MatchEvent(timestamp, "P", playerId, x, y, z, heldItem, invCount, null);
    }

    /**
     * Create a wool touch event.
     */
    public static MatchEvent woolTouch(int timestamp, String playerId, int x, int y, int z, String woolColor) {
        return new MatchEvent(timestamp, "WT", playerId, x, y, z, null, null, woolColor);
    }

    /**
     * Create a wool capture event.
     */
    public static MatchEvent woolCapture(int timestamp, String playerId, int x, int y, int z, String woolColor) {
        return new MatchEvent(timestamp, "WC", playerId, x, y, z, null, null, woolColor);
    }

    // Serializer

    public static class Serializer implements Dehydrator<MatchEvent> {
        public static final Serializer INSTANCE = new Serializer();

        @Override
        public void dehydrate(MatchEvent event, ValueWriter writer) {
            // Required fields
            writer.write("t", event.timestamp);
            writer.write("e", event.eventType);

            // Optional fields - only write if not null
            if (event.playerId != null) writer.write("p", event.playerId);
            if (event.x != null) writer.write("x", event.x);
            if (event.y != null) writer.write("y", event.y);
            if (event.z != null) writer.write("z", event.z);
            if (event.heldItem != null) writer.write("h", event.heldItem);
            if (event.invCount != null) writer.write("n", event.invCount);
            if (event.extraInfo != null) writer.write("i", event.extraInfo);
        }
    }
}