package org.alfresco.contentlake.service;

import lombok.extern.slf4j.Slf4j;
import org.alfresco.contentlake.spi.TextExtractor;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.springframework.core.io.Resource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Plain-text extraction backed by embedded Apache Tika.
 *
 * <p>This adapter intentionally does not call any Nuxeo server-side transform or
 * conversion endpoint. Plain-text documents are read directly by the shared sync
 * pipeline; binary formats are extracted locally through Tika.</p>
 */
@Slf4j
public class NuxeoTextExtractor implements TextExtractor {

    private final AutoDetectParser parser;

    public NuxeoTextExtractor() {
        this(new AutoDetectParser());
    }

    NuxeoTextExtractor(AutoDetectParser parser) {
        this.parser = parser;
    }

    @Override
    public boolean supports(String mimeType) {
        return mimeType != null && !mimeType.isBlank() && !mimeType.startsWith("text/");
    }

    @Override
    public String extractText(Resource content, String mimeType) {
        Metadata metadata = new Metadata();
        if (mimeType != null && !mimeType.isBlank()) {
            metadata.set(Metadata.CONTENT_TYPE, mimeType);
        }

        try (InputStream inputStream = content.getInputStream()) {
            BodyContentHandler handler = new BodyContentHandler(-1);
            parser.parse(inputStream, handler, metadata, new ParseContext());
            String text = handler.toString();
            return text != null && !text.isBlank() ? text : null;
        } catch (TikaException | SAXException e) {
            log.warn("Tika could not parse document (mimeType={}); returning no text: {}", mimeType, e.getMessage());
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("I/O error reading document content for Tika extraction", e);
        }
    }
}
