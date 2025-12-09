package com.example.pgmlogger;

public enum EventType {
    MATCH_START(0),
    MATCH_END(1),
    SPAWN(2),
    DEATH(3),
    POSITION(4),
    WOOL_TOUCH(5),
    WOOL_CAPTURE(6);

    private final int id;

    EventType(int id) { this.id = id; }

    public int getId() { return id; }
}