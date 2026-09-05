package dev.luisfelipe.vigil.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MainTest {

    @Test
    void readsAnIntegerAndIgnoresSurroundingWhitespace() {
        assertEquals(3, Main.parseIntOr("REPLICATION_FACTOR", "3", 1));
        assertEquals(3, Main.parseIntOr("REPLICATION_FACTOR", "  3  ", 1));
        assertEquals(0, Main.parseIntOr("REPLICATION_FACTOR", "0", 1));
        assertEquals(-1, Main.parseIntOr("REPLICATION_FACTOR", "-1", 1));
    }

    @Test
    void fallsBackWhenTheVariableIsUnset() {
        assertEquals(1, Main.parseIntOr("REPLICATION_FACTOR", null, 1));
        assertEquals(1, Main.parseIntOr("REPLICATION_FACTOR", "", 1));
        assertEquals(1, Main.parseIntOr("REPLICATION_FACTOR", "   ", 1));
    }

    @Test
    void fallsBackWhenTheValueIsMalformed() {
        // A typo in a deployment manifest should not hold the topology down
        // over a number that has a perfectly good default.
        assertEquals(2, Main.parseIntOr("REPLICATION_FACTOR", "three", 2));
        assertEquals(2, Main.parseIntOr("REPLICATION_FACTOR", "3.0", 2));
        assertEquals(2, Main.parseIntOr("REPLICATION_FACTOR", "1x", 2));
    }
}
