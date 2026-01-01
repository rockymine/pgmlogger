package com.github.rockymine.pgmlogger;

/** Utility methods for deterministic player ID assignment. */
public final class PlayerIds {

  private PlayerIds() {}

  /**
   * Returns the negative player ID for a permitted player at the given index.
   *
   * @param index zero-based index in the permitted list
   * @return negative player ID
   */
  public static int permittedIdForIndex(int index) {
    return -1 - index;
  }
}
