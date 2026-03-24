package org.alfresco.contentlake.service;

import org.alfresco.contentlake.model.Chunk;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class Chunker {

    private final int chunkSize;
    private final int overlap;

    public Chunker(int chunkSize, int overlap) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        if (overlap < 0) {
            throw new IllegalArgumentException("overlap must be >= 0");
        }
        if (overlap >= chunkSize) {
            throw new IllegalArgumentException(format(
                    "overlap must be < chunkSize to avoid infinite loops. overlap=%d chunkSize=%d",
                    overlap, chunkSize));
        }
        this.chunkSize = chunkSize;
        this.overlap = overlap;
    }

    public List<Chunk> chunk(String text, String nodeId) {
        List<Chunk> chunks = new ArrayList<>();
        
        if (text == null || text.isBlank()) {
            return chunks;
        }

        int start = 0;
        int index = 0;

        while (start < text.length()) {
            int end = Math.min(start + chunkSize, text.length());
            
            // Try to break at word boundary
            if (end < text.length()) {
                int lastSpace = text.lastIndexOf(' ', end);
                if (lastSpace > start) {
                    end = lastSpace;
                }
            }

            String chunkText = text.substring(start, end).trim();
            if (!chunkText.isEmpty()) {
                chunks.add(new Chunk(nodeId, chunkText, index, start, end));
                index++;
            }

            start = end - overlap;
            if (start <= chunks.get(chunks.size() - 1).getStartOffset()) {
                start = end;
            }
        }

        return chunks;
    }
}
