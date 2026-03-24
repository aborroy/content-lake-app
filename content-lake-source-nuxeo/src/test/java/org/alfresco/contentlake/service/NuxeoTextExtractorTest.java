package org.alfresco.contentlake.service;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextBox;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;

import java.awt.Rectangle;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class NuxeoTextExtractorTest {

    private final NuxeoTextExtractor extractor = new NuxeoTextExtractor();

    @Test
    void extractText_usesEmbeddedTikaForBinaryFormats() {
        String xml = """
                <?xml version="1.0" encoding="UTF-8"?>
                <document>
                  <title>This is a test document.</title>
                </document>
                """;

        String text = extractor.extractText(
                new ByteArrayResource(xml.getBytes(StandardCharsets.UTF_8)),
                "application/octet-stream"
        );

        assertThat(text).contains("This is a test document.");
    }

    @Test
    void supports_rejectsAlreadyTextualMimeTypes() {
        assertThat(extractor.supports("text/plain")).isFalse();
        assertThat(extractor.supports("application/pdf")).isTrue();
        assertThat(extractor.supports(null)).isFalse();
    }

    @Test
    void extractText_handlesDocxDocuments() throws IOException {
        String text = "Quarterly strategy from DOCX";

        assertThat(extract(buildDocx(text),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"))
                .contains(text);
    }

    @Test
    void extractText_handlesPdfDocuments() throws IOException {
        String text = "Quarterly strategy from PDF";

        assertThat(extract(buildPdf(text), "application/pdf"))
                .contains(text);
    }

    @Test
    void extractText_handlesXlsxDocuments() throws IOException {
        String text = "Quarterly strategy from XLSX";

        assertThat(extract(buildXlsx(text),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .contains(text);
    }

    @Test
    void extractText_handlesPptxDocuments() throws IOException {
        String text = "Quarterly strategy from PPTX";

        assertThat(extract(buildPptx(text),
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"))
                .contains(text);
    }

    private String extract(byte[] content, String mimeType) {
        return extractor.extractText(new ByteArrayResource(content), mimeType);
    }

    private byte[] buildDocx(String text) throws IOException {
        try (XWPFDocument document = new XWPFDocument();
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            XWPFParagraph paragraph = document.createParagraph();
            paragraph.createRun().setText(text);
            document.write(output);
            return output.toByteArray();
        }
    }

    private byte[] buildPdf(String text) throws IOException {
        try (PDDocument document = new PDDocument();
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            PDPage page = new PDPage();
            document.addPage(page);

            try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                contentStream.beginText();
                contentStream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                contentStream.newLineAtOffset(72, 720);
                contentStream.showText(text);
                contentStream.endText();
            }

            document.save(output);
            return output.toByteArray();
        }
    }

    private byte[] buildXlsx(String text) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            var sheet = workbook.createSheet("Sheet1");
            var row = sheet.createRow(0);
            row.createCell(0).setCellValue(text);
            workbook.write(output);
            return output.toByteArray();
        }
    }

    private byte[] buildPptx(String text) throws IOException {
        try (XMLSlideShow slideshow = new XMLSlideShow();
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            XSLFSlide slide = slideshow.createSlide();
            XSLFTextBox textBox = slide.createTextBox();
            textBox.setAnchor(new Rectangle(50, 50, 400, 120));
            textBox.addNewTextParagraph().addNewTextRun().setText(text);
            slideshow.write(output);
            return output.toByteArray();
        }
    }
}
