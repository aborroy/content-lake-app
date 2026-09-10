package org.hyland.contentlake.rag.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for RAG behaviour.
 *
 * <p>Bound from {@code rag.*} in {@code application.yml}.</p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "rag")
public class RagProperties {

    /** Default number of chunks to retrieve for context. */
    private int defaultTopK = 5;

    /** Default minimum similarity score threshold. 0.0 = no filtering (lets SemanticSearchService apply its own threshold). */
    private double defaultMinScore = 0.0;

    /** When true, uses hybrid (vector + keyword) search for RAG retrieval; false uses vector-only. */
    private boolean useHybridSearch = true;

    /**
     * Maximum character length of the assembled context sent to the LLM.
     *
     * <p>The previous default of 4000 chars (~1000 tokens) was very conservative
     * and often caused useful chunks to be truncated. Even small local models
     * like llama3.2 support 128K token context windows. 12000 chars (~3000 tokens)
     * allows 5-8 substantial chunks while leaving ample room for the system prompt,
     * user question, and generated answer within any reasonable model's limits.</p>
     */
    private int maxContextLength = 12000;

    /**
     * Default system prompt for the LLM.
     *
     * <p>Structured to work well with smaller local models by providing explicit
     * formatting guidance and step-by-step instructions for citation.</p>
     */
    private String defaultSystemPrompt = """
            You are a document assistant that answers questions based strictly on the provided context.

            RULES:
            1. Use ONLY information from the DOCUMENT CONTEXT below. Do not use prior knowledge.
            2. When referencing information, cite the source using its label (e.g. "According to Source 1..."). \
            Cite each source once per answer, not once per sentence.
            3. If multiple sources contain relevant information, synthesize them and cite each.
            4. Extract facts directly from the context. If a name, date, number, code, or identifier \
            appears in any source, state it directly. Do not claim information is missing if it is present \
            in any source.
            5. Partial answers are valuable. If you can answer part of the question from the context, do so, \
            then note what is genuinely missing. Do not refuse the whole question because one detail is unclear.
            6. Be concise and direct. Do not repeat the question or add unnecessary preamble.
            7. You may apply standard world knowledge for unit conversions (temperatures, currencies, UTC offsets) \
            when the document context provides the underlying fact but not the converted value. \
            Do not invent document facts.""";

    /** Cross-encoder reranker settings (disabled when url is blank). */
    private RerankerProperties reranker = new RerankerProperties();

    /** Max Marginal Relevance diversity-selection settings (disabled by default). */
    private MmrProperties mmr = new MmrProperties();

    /** Conversation memory settings. */
    private ConversationProperties conversation = new ConversationProperties();

    /** Source-specific deep-link templates returned in search and RAG responses. */
    private SourceLinkProperties sourceLinks = new SourceLinkProperties();

    /** Shared bounds for the query-expansion stage that multi-query, HyDE and decomposition feed. */
    private QueryExpansionProperties queryExpansion = new QueryExpansionProperties();

    /** Multi-query retrieval settings (disabled by default). */
    private MultiQueryProperties multiQuery = new MultiQueryProperties();

    /** Hypothetical Document Embedding settings (disabled by default). */
    private HydeProperties hyde = new HydeProperties();

    /** Compound-question decomposition settings (disabled by default). */
    private QueryDecompositionProperties queryDecomposition = new QueryDecompositionProperties();

    /** Pre-generation retrieval relevance gate (disabled by default). */
    private RetrievalGradingProperties retrievalGrading = new RetrievalGradingProperties();

    /** Intent-aware filter inference settings (opt-in per request via inferFilters). */
    private FilterInferenceProperties filterInference = new FilterInferenceProperties();

    /** Post-generation citation faithfulness verification (disabled by default). */
    private CitationProperties citation = new CitationProperties();

    /** hxpr named-query exposure. */
    private NamedQueryProperties namedQuery = new NamedQueryProperties();

    /** Retrieval-shaping settings, including small-to-big section expansion. */
    private RetrievalProperties retrieval = new RetrievalProperties();

    /** In-app evaluation smoke endpoint (disabled by default). */
    private EvaluationProperties evaluation = new EvaluationProperties();

    /** Prompt-injection defense on retrieved document content (#71). Disabled by default. */
    private PromptInjectionProperties promptInjection = new PromptInjectionProperties();

    /** Per-principal rate limiting on RAG/search endpoints (#75). Disabled by default. */
    private RateLimitProperties rateLimit = new RateLimitProperties();

    /** Agentic tool-calling during generation (#65). Disabled by default. */
    private AgenticToolsProperties agenticTools = new AgenticToolsProperties();

    /** MCP server exposing rag-service tools to external LLM agents (#61). Enabled by default. */
    private McpProperties mcp = new McpProperties();

    /** Semantic query-result caching (#72). Disabled by default. */
    private CacheProperties cache = new CacheProperties();

    /** User feedback capture on generated answers (#74). Enabled by default. */
    private FeedbackProperties feedback = new FeedbackProperties();

    /** Span payloads and content capture on the RAG pipeline spans (#116). Both off by default. */
    private ObservabilityProperties observability = new ObservabilityProperties();

    /** Multi-embedding-type retrieval and the re-embedding backfill (#121). */
    private EmbeddingProperties embedding = new EmbeddingProperties();

    /**
     * Querying a corpus that holds vectors from more than one embedding model, and moving it onto a
     * new one without search degrading in the meantime (#121).
     *
     * <p>Changing the embedding model invalidates every vector, so an upgrade would otherwise mean a
     * full re-ingest with search degraded throughout, which makes model upgrades indefinitely
     * deferrable.</p>
     */
    @Data
    public static class EmbeddingProperties {

        /**
         * Models, besides the configured one, whose vectors are still present in the corpus.
         *
         * <p>Empty by default, which leaves retrieval on exactly the single-type path it has always
         * used. Naming a model here is what allows its type to be queried <em>in its own vector
         * space</em>: the query is embedded once per model, because a vector from one model has no
         * meaningful similarity to vectors from another. A type present in the corpus but not named
         * here is still queried, using the configured model, which is no worse than the wildcard does
         * today but is not a correct comparison either.</p>
         *
         * <p>Coexisting types must share vector dimensionality, which is a property of the index
         * rather than of this setting.</p>
         */
        private List<String> additionalModels = new ArrayList<>();

        /** Reading back the embedding types actually present in the index. */
        private TypeDiscoveryProperties typeDiscovery = new TypeDiscoveryProperties();

        /** The re-embedding backfill job. */
        private BackfillProperties backfill = new BackfillProperties();

        @Data
        public static class TypeDiscoveryProperties {

            /**
             * When true, the embedding types to query are read from the index rather than assumed from
             * configuration. This is what finds a type written by a retired model, and what tolerates a
             * type recorded in a form the current derivation does not produce. Turning it off pins
             * retrieval to the configured type alone.
             */
            private boolean enabled = true;

            /**
             * How long the discovered set is reused. It is a property of the corpus, not of a request,
             * and it changes only when a model is introduced or retired.
             */
            private long ttlSeconds = 300;
        }

        @Data
        public static class BackfillProperties {

            /**
             * Enables the backfill endpoints. Off by default: the job writes to every document in the
             * corpus and spends embedding throughput, so it is started deliberately.
             */
            private boolean enabled = false;

            /**
             * Documents processed per minute, which is what makes the job something that can run
             * against a live system. Zero or less means no limit.
             */
            private int docsPerMinute = 60;

            /**
             * Accounts allowed to start, pause and resume the job. Empty by default, so the endpoints
             * are closed until a deployment names an operator.
             */
            private List<String> operatorUsers = new ArrayList<>();
        }
    }

    @Data
    public static class RerankerProperties {

        /** TEI cross-encoder endpoint (e.g. http://localhost:8081). Leave blank to disable reranking. */
        private String url = "";

        /** Number of top results to keep after reranking. */
        private int topN = 8;

        /**
         * Enables LLM-based reranking when no TEI {@code url} is set. Escape hatch: when false
         * (default) and no url is set, reranking is a no-op. Ignored when a TEI url is present
         * (the TEI reranker always takes precedence).
         */
        private boolean enabled = false;
    }

    @Data
    public static class MmrProperties {

        /** Enables MMR diversity selection between retrieval and reranking. */
        private boolean enabled = false;

        /**
         * Trade-off between relevance and diversity. 1.0 = pure relevance (original order),
         * 0.0 = pure diversity.
         */
        private double lambda = 0.5;

        /** Size of the over-retrieved candidate pool MMR selects from. */
        private int poolSize = 30;
    }

    @Data
    public static class ConversationProperties {

        /** Enables/disables conversation memory features. */
        private boolean enabled = true;

        /** Number of most recent turns kept in memory per session. */
        private int maxHistoryTurns = 10;

        /** Session expiration timeout based on inactivity. */
        private int sessionTtlMinutes = 30;

        /** Enables/disables conversation-aware query reformulation. */
        private boolean queryReformulation = true;

        /** Persistent running-summary settings (disabled by default). */
        private SummaryProperties summary = new SummaryProperties();
    }

    @Data
    public static class SummaryProperties {

        /**
         * Enables an LLM-maintained running summary persisted in hxpr, preserving key facts and
         * intent beyond the sliding window and across restarts. Off by default until the hxpr
         * sessions folder is provisioned.
         */
        private boolean enabled = false;

        /** hxpr folder under which per-session summary documents are stored. */
        private String basePath = "/_sessions";
    }

    @Data
    public static class SourceLinkProperties {

        /** Share-style deep link for Alfresco documents. */
        private String alfrescoTemplate =
                "${content.service.url}/share/page/document-details?nodeRef=workspace://SpacesStore/{nodeId}";

        /** Default Nuxeo Web UI browse link using the full repository path. */
        private String nuxeoTemplate =
                "${nuxeo.base-url}/ui/#!/browse{nuxeoPath}";
    }

    @Data
    public static class QueryExpansionProperties {

        /**
         * Hard cap on the total number of query variants a single search may run, counting the
         * original. Multi-query, HyDE and decomposition all append to the same list, so without a
         * shared ceiling enabling all three multiplies the per-request embedding and hxpr calls.
         */
        private int maxVariants = 6;

        /**
         * Reciprocal-rank-fusion constant used when merging the per-variant result sets.
         *
         * <p>Distinct from {@code search.hybrid.rrf-k}, which fuses the vector and keyword legs of a
         * single variant. This one fuses across variants, one level up.</p>
         */
        private int rrfK = 60;
    }

    @Data
    public static class MultiQueryProperties {

        /** Enables multi-query retrieval: N LLM-generated query variants, fused by RRF. */
        private boolean enabled = false;

        /** Number of variants to request from the LLM, excluding the original query. */
        private int variants = 3;
    }

    @Data
    public static class HydeProperties {

        /**
         * Enables Hypothetical Document Embedding: the LLM drafts an answer-shaped passage which is
         * embedded document-side (no query instruction prefix) and searched alongside the original
         * query.
         */
        private boolean enabled = false;

        /** Upper bound on the generated passage, truncated before embedding. */
        private int maxChars = 1000;
    }

    @Data
    public static class QueryDecompositionProperties {

        /** Enables decomposition of compound questions into independently-retrievable sub-questions. */
        private boolean enabled = false;

        /** Upper bound on the sub-questions accepted from the LLM. */
        private int maxSubQuestions = 4;
    }

    @Data
    public static class EvaluationProperties {

        /**
         * Enables the in-app {@code POST /api/rag/evaluate} smoke endpoint. Off by default: this is a
         * lightweight CI/smoke check, not the authoritative quality gate (that is the external
         * content-lake-eval harness). Operators enable it deliberately.
         */
        private boolean enabled = false;
    }

    @Data
    public static class RetrievalProperties {

        /** Small-to-big (parent-child) expansion of retrieved chunks to their parent section. */
        private SmallToBigProperties smallToBig = new SmallToBigProperties();

        /** Verbatim matching for identifier-like queries (#122). */
        private VerbatimIdentifierProperties verbatimIdentifier = new VerbatimIdentifierProperties();

        /**
         * An extra retrieval pass restricted to chunks containing an identifier verbatim.
         *
         * <p>A short alphanumeric identifier embeds poorly, and the keyword leg alone does not rescue
         * it. Query expansion does not help either: the problem is that the token is rare, not that the
         * query is ambiguous. Nothing else in the retrieval stack treats an identifier differently from
         * prose.</p>
         */
        @Data
        public static class VerbatimIdentifierProperties {

            /**
             * Enables the verbatim pass. Off by default until the eval confirms the gain, and safe when
             * on: the pass is an additional query variant, so if it matches nothing the ranking is the
             * one the search would have produced anyway. A result is never discarded merely because the
             * query looked token-like.
             */
            private boolean enabled = false;

            /**
             * Shortest token treated as an identifier. Below this a token matches so much of the corpus
             * that the pass costs query time for no selectivity.
             */
            private int minLength = 3;

            /**
             * Most identifiers taken from one query. A query naming several is unusual; the cap stops a
             * pathological one from turning into a long term list.
             */
            private int maxTerms = 2;
        }

        @Data
        public static class SmallToBigProperties {

            /**
             * When true, each retrieved chunk is expanded to its full parent section (from the
             * per-document section map) before the context is assembled for the LLM. Off by default;
             * only chunks whose document carries a section map are expanded, others pass through.
             */
            private boolean enabled = false;

            /** Upper bound on an expanded section's text; longer sections are truncated. */
            private int maxSectionChars = 4000;
        }
    }

    @Data
    public static class CitationProperties {

        /** Post-generation faithfulness verification of the answer against its cited sources. */
        private VerifyProperties verify = new VerifyProperties();

        @Data
        public static class VerifyProperties {

            /**
             * When true, after generation an NLI-style LLM check flags answer claims not supported by
             * the retrieved context, populating {@code verified} and {@code unsupportedClaims} on the
             * response. Off by default (adds an LLM call per answer).
             */
            private boolean enabled = false;
        }
    }

    @Data
    public static class NamedQueryProperties {

        /** Whether clients may discover the named queries registered in hxpr. */
        private DiscoveryProperties discovery = new DiscoveryProperties();

        @Data
        public static class DiscoveryProperties {

            /**
             * When true, {@code GET /api/rag/named-queries} lists the named-query names registered
             * in hxpr so a client can offer them as saved-search filters; the UI selector hides
             * itself on an empty list. Turn it off where those names are hxpr internals
             * ({@code tree_children}, {@code simple_search}, ...) rather than curated saved
             * searches. This governs discovery only: a {@code namedQuery} a client sends explicitly
             * on a search request is still resolved to its filter.
             */
            private boolean enabled = true;
        }
    }

    @Data
    public static class FilterInferenceProperties {

        /**
         * Ingest-property key that holds a document's category, used when the LLM infers a category
         * for a query. Blank (the default) disables category inference: without a known property key
         * the service would be guessing where to file the value, so only date/mime/path filters are
         * inferred until a deployment sets this.
         */
        private String categoryProperty = "";
    }

    @Data
    public static class RetrievalGradingProperties {

        /** Enables the pre-generation relevance gate on the reranked hits. */
        private boolean enabled = false;

        /**
         * Score a hit must reach to count as relevant.
         *
         * <p>This is <strong>not</strong> a cosine value on the default hybrid path: fused scores sit
         * on the fusion's own scale, roughly 0.02-0.03 under {@code rrf} and 0-1 under
         * {@code weighted}/{@code minmax}. The default of 0.0 leaves the gate inert even when
         * enabled, so a threshold has to be chosen against the configured fusion strategy.</p>
         */
        private double minScore = 0.0;

        /** Number of hits that must clear {@code minScore} for the verdict to be relevant. */
        private int minHits = 1;

        /**
         * When true, a weak verdict triggers one broadened re-retrieval pass (threshold dropped,
         * candidate pool widened) before generation is skipped. When false, a weak verdict skips
         * generation immediately.
         */
        private boolean broaden = true;
    }

    @Data
    public static class PromptInjectionProperties {

        /**
         * When true, wraps each retrieved chunk in explicit delimiters and frames it as untrusted
         * document data (not instructions) in the LLM prompt. Off by default so the eval baseline is
         * preserved until the generation delta is characterized.
         */
        private boolean defenseEnabled = false;

        /**
         * When true, runs the heuristic {@code PromptInjectionScanner} over each retrieved chunk and
         * logs matches for audit. Chunks are not dropped (evidence the user needs may match a
         * pattern). Off by default until validated against a real-corpus false-positive rate.
         */
        private boolean scanEnabled = false;
    }

    @Data
    public static class RateLimitProperties {

        /** Enables per-principal rate limiting. Off by default. */
        private boolean enabled = false;

        /**
         * Requests per minute for generation endpoints ({@code /api/rag/prompt},
         * {@code /api/rag/chat/stream}). Tighter than search given the per-request LLM cost.
         */
        private int generateRequestsPerMinute = 20;

        /** Requests per minute for search endpoints ({@code /api/rag/search/**}). */
        private int searchRequestsPerMinute = 60;
    }

    @Data
    public static class AgenticToolsProperties {

        /**
         * Enables agentic tool-calling: the RAG LLM may invoke re-search / get-document / list-sources
         * tools mid-generation. Off by default (increased LLM round-trips and blast radius).
         */
        private boolean enabled = false;

        /** Maximum additional retrieval rounds the model may trigger via {@code researchAgain}. */
        private int maxIterations = 2;
    }

    @Data
    public static class McpProperties {

        /**
         * Enables the MCP server endpoint exposing search/retrieval/listing tools to external LLM
         * agents. Enabled by default; the endpoint sits behind the existing authentication chain
         * (HTTP Basic / Alfresco ticket), so it is never anonymously accessible.
         */
        private boolean enabled = true;
    }

    /**
     * Semantic query-result caching (#72): a short-TTL, bounded in-memory (Caffeine) cache of query
     * embeddings and full retrieval results. Off by default so the eval baseline is preserved until
     * the latency/quality delta is characterized.
     *
     * <p>Cache entries for retrieval results are keyed on the caller's effective permission scope
     * (the authenticated principal) as well as the normalized query and its filters, so a cached
     * result is never served across ACL contexts. Group membership can change within the TTL window;
     * {@code ttl-seconds} bounds that staleness.</p>
     */
    @Data
    public static class CacheProperties {

        /** Master switch for query-embedding and retrieval-result caching. */
        private boolean enabled = false;

        /** Time-to-live for cache entries. Also bounds how stale a principal's ACL scope may be. */
        private long ttlSeconds = 60;

        /** Maximum number of entries per cache (query-embedding cache and result cache each). */
        private long maxSize = 1000;
    }

    /**
     * User feedback capture (#74): a {@code POST /api/rag/feedback} endpoint persists a rating (and
     * optional comment) for a generated answer as an hxpr document, growing the evaluation corpus
     * from real usage. Enabled by default; the endpoint sits behind the existing authentication chain.
     */
    @Data
    public static class FeedbackProperties {

        /** Enables the feedback endpoint and persistence. */
        private boolean enabled = true;

        /** hxpr folder under which feedback documents are stored. */
        private String basePath = "/_feedback";

        /**
         * Accounts allowed to list every submitter's feedback rather than only their own. Empty by
         * default, so the aggregate view is closed until a deployment names an operator.
         */
        private List<String> operatorUsers = new ArrayList<>();
    }

    /**
     * Payloads on the RAG pipeline spans (#116).
     *
     * <p>#73 named the spans; this controls what travels on them. Two independent switches, because
     * they are different decisions: {@code payloadsEnabled} is an operational verbosity choice, while
     * {@code captureContent} is a data-protection one. Both default off, and the second stays off when
     * the first is turned on.</p>
     *
     * <p>Sampling is <strong>not</strong> configured here. {@code management.tracing.sampling.
     * probability} already governs it and is honoured by the Micrometer OpenTelemetry bridge; a second
     * knob would interact multiplicatively with it and would not be the one an operator reaches for.
     * The two are orthogonal: sampling bounds how many spans are exported, these flags bound what each
     * span costs to build and what it carries.</p>
     */
    @Data
    public static class ObservabilityProperties {

        /**
         * Attaches ids, scores, counts and token usage to the RAG spans.
         *
         * <p>Off by default. Note that "off" here does not mean the spans are absent: with actuator on
         * the classpath the {@code ObservationRegistry} is never a no-op, so the spans #73 introduced
         * exist on every request regardless. What this flag governs is whether each one pays an
         * O(hits) payload build, which is wasted work when nothing is exported. Turn it on together
         * with {@code management.otlp.tracing.endpoint}.</p>
         */
        private boolean payloadsEnabled = false;

        /**
         * Also attaches the question, its reformulated and expanded variants, retrieved chunk text,
         * source names and paths, and the answer.
         *
         * <p>This copies ACL-protected user content into the trace backend, whose readers are whoever
         * can reach it rather than the ACL that governed the retrieval. See
         * {@code docs/security-model.md}. Independent of {@code payloadsEnabled}, so a deployment that
         * wants ids and counts does not get content by asking; no effect when that flag is false.</p>
         */
        private boolean captureContent = false;

        /**
         * Maximum hits whose id, rank and score are recorded on a span. Bounds span size on a large
         * {@code topK}.
         */
        private int maxChunksRecorded = 20;

        /**
         * Maximum characters per content attribute. Only read when {@code captureContent} is true, and
         * the reason an enabled deployment cannot ship a full context block on every span.
         */
        private int maxContentChars = 2000;
    }
}
