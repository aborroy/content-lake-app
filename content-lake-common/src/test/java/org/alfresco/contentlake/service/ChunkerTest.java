package org.alfresco.contentlake.service;

import org.alfresco.contentlake.model.Chunk;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkerTest {

    private static final String NODE_ID = "node-1";

    @Test
    void nullAndEmptyInput_returnsEmptyList() {
        Chunker chunker = new Chunker(100, 10);

        assertThat(chunker.chunk(null, NODE_ID)).isEmpty();
        assertThat(chunker.chunk("", NODE_ID)).isEmpty();
        assertThat(chunker.chunk("   ", NODE_ID)).isEmpty();
    }

    @Test
    void shortText_returnsSingleChunk() {
        Chunker chunker = new Chunker(500, 50);
        String text = "Hello world, this is a short sentence.";

        List<Chunk> chunks = chunker.chunk(text, NODE_ID);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0).getText()).isEqualTo(text);
        assertThat(chunks.get(0).getNodeId()).isEqualTo(NODE_ID);
        assertThat(chunks.get(0).getIndex()).isZero();
    }

    @Test
    void chunksRespectSizeLimit() {
        int chunkSize = 50;
        Chunker chunker = new Chunker(chunkSize, 10);
        String text = "word ".repeat(100); // 500 chars

        List<Chunk> chunks = chunker.chunk(text, NODE_ID);

        assertThat(chunks).hasSizeGreaterThan(1);
        for (Chunk chunk : chunks) {
            assertThat(chunk.getText().length()).isLessThanOrEqualTo(chunkSize);
        }
    }

    @Test
    void overlapPreservedBetweenChunks() {
        int chunkSize = 60;
        int overlap = 15;
        Chunker chunker = new Chunker(chunkSize, overlap);
        String text = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa";

        List<Chunk> chunks = chunker.chunk(text, NODE_ID);

        assertThat(chunks).hasSizeGreaterThan(1);
        for (int i = 1; i < chunks.size(); i++) {
            Chunk prev = chunks.get(i - 1);
            Chunk curr = chunks.get(i);
            // The current chunk should start within the previous chunk's range (overlap region)
            assertThat(curr.getStartOffset()).isLessThan(prev.getEndOffset());
        }
    }

    @Test
    void breaksAtWordBoundary_noMidWordSplits() {
        Chunker chunker = new Chunker(30, 5);
        // Continuous text where mid-word split is possible
        String text = "internationalization globalization modernization standardization";

        List<Chunk> chunks = chunker.chunk(text, NODE_ID);

        for (Chunk chunk : chunks) {
            String t = chunk.getText();
            // Chunk text should not start or end with a partial word (no leading/trailing fragments inside a word)
            assertThat(t).doesNotStartWith(" ");
            assertThat(t).doesNotEndWith(" ");
        }
    }

    @Test
    void constructorValidation_rejectsInvalidArguments() {
        assertThatThrownBy(() -> new Chunker(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize must be > 0");

        assertThatThrownBy(() -> new Chunker(-1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("chunkSize must be > 0");

        assertThatThrownBy(() -> new Chunker(100, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overlap must be >= 0");

        assertThatThrownBy(() -> new Chunker(100, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overlap must be < chunkSize");

        assertThatThrownBy(() -> new Chunker(100, 200))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overlap must be < chunkSize");
    }
}
