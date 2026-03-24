package org.alfresco.contentlake.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.alfresco.contentlake.hxpr.api.model.ACE;

import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HxprDocument {

    // Core hxpr / Nuxeo fields (API contract)

    @JsonProperty("sys_id")
    private String sysId;

    @JsonProperty("sys_primaryType")
    private String sysPrimaryType;        // "SysFile", "SysFolder"

    @JsonProperty("sys_name")
    private String sysName;

    @JsonProperty("sys_parentPath")
    private String sysParentPath;

    @JsonProperty("sys_mixinTypes")
    private List<String> sysMixinTypes;   // must include "CinRemote" for ingestion

    @JsonProperty("sys_fulltextBinary")
    private String sysFulltextBinary;

    @JsonProperty("sys_acl")
    private List<ACE> sysAcl;

    @JsonProperty("sys_effectiveAcl")
    private List<ACE> sysEffectiveAcl;

    @JsonProperty("sys_effectivePermissions")
    private List<String> sysEffectivePermissions;


    // CinRemote ingestion fields (THIS is where metadata belongs)

    @JsonProperty("cin_id")
    private String cinId;                 // Alfresco nodeId (searchable)

    @JsonProperty("cin_sourceId")
    private String cinSourceId;           // Alfresco repository id

    @JsonProperty("cin_paths")
    private List<String> cinPaths;
    @JsonProperty("cin_ingestProperties")
    private Map<String, Object> cinIngestProperties;

    @JsonProperty("cin_ingestPropertyNames")
    private List<String> cinIngestPropertyNames;

    @JsonProperty("cin_read")
    private List<String> cinRead;

    @JsonProperty("cin_deny")
    private List<String> cinDeny;

    // SysEmbed mixin fields - embeddings stored as inline array

    @JsonProperty("sysembed_embeddings")
    private List<HxprEmbedding> sysembedEmbeddings;


    // Internal sync state (NOT sent to hxpr)

    @JsonIgnore
    private SyncStatus syncStatus;

    @JsonIgnore
    private String syncError;


   // Alfresco metadata (internal only — NEVER serialized)

    @JsonIgnore
    private String alfrescoNodeId;

    @JsonIgnore
    private String alfrescoRepositoryId;

    @JsonIgnore
    private String alfrescoPath;

    @JsonIgnore
    private String alfrescoName;

    @JsonIgnore
    private String alfrescoMimeType;

    @JsonIgnore
    private String alfrescoModifiedAt;

    @JsonIgnore
    private List<String> alfrescoReadAuthorities;


    public enum SyncStatus {
        PENDING,
        PROCESSING,
        INDEXED,
        FAILED
    }

    /**
     * Query result wrapper for HXQL queries returning documents.
     * Kept here because the API's {@code QueryResult} schema
     * wraps the generic {@code Document} type, whereas this
     * wrapper provides typed access to {@link HxprDocument}
     * with all Alfresco-specific CinRemote fields.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QueryResult {
        private List<HxprDocument> documents;
        private long totalCount;
        private long count;
        private long offset;
        private long limit;
    }
}
