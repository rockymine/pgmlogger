package com.example.pgmlogger;

/**
 * Types of events that can be logged during a PGM match.
 *
 * <p>These event types correspond to the {@code event_type} field in the
 * Parquet schema. Each type determines which optional fields will be populated
 * in a {@link MatchEvent}.
 *
 * <p>The ordinal values of these enum constants are stored in the Parquet file,
 * so the order should not be changed to maintain backwards compatibility with
 * existing data files.
 */
public enum EventType {
    /** Match initialization event, logged at match start with timestamp 0 */
    MATCH_START,

    /** Match conclusion event, logged when the match ends */
    MATCH_END,

    /** Player spawn event, includes player ID and spawn coordinates */
    SPAWN,

    /** Player death event, includes player ID and death coordinates */
    DEATH,

    /** Player position sample, includes coordinates, held item, and inventory count */
    POSITION,

    /** Wool pickup event, logged when a player first touches a wool objective */
    WOOL_TOUCH,

    /** Wool completion event, logged when wool is placed at the monument */
    WOOL_CAPTURE;
}