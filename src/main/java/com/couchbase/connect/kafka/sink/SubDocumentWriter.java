package com.couchbase.connect.kafka.sink;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import com.couchbase.connect.kafka.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Notification;
import rx.Observable;
import rx.functions.Action1;
import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import java.util.NoSuchElementException;
import java.util.Set;

public class SubDocumentWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubDocumentWriter.class);

    private SubDocumentMode mode;

    private String path;

    private boolean createPaths;

    private boolean createDocuments;

    public SubDocumentWriter(SubDocumentMode mode, String path, boolean createPaths,
            boolean createDocuments) {

        this.mode = mode;
        this.path = path;
        this.createPaths = createPaths;
        this.createDocuments = createDocuments;
    }

    public Completable write(final AsyncBucket bucket, final JsonBinaryDocument document,
            PersistTo persistTo, ReplicateTo replicateTo) {
        if (document == null || (document.content() == null
                && (document.id() == null || document.id().isEmpty()))) {

            LOGGER.warn("document or document content is null");
            // skip it
            return Completable.complete();
        }

        JsonObject node = JsonObject.fromJson(document.content().toString(UTF_8));

        SubdocOptionsBuilder options = new SubdocOptionsBuilder().createPath(createPaths);

        AsyncMutateInBuilder mutation = bucket.mutateIn(document.id());

        if (document.content() == null && !document.id().isEmpty()) {
            mutation = mutation.remove(path, options);
        } else {
            switch (mode) {
                case UPSERT: {
                    mutation = mutation.upsert(path, node, options);
                    break;
                }
                case ARRAY_INSERT: {
                    mutation = mutation.arrayInsert(path, node, options);
                    break;
                }
                case ARRAY_APPEND: {
                    mutation = mutation.arrayAppend(path, node, options);

                    break;
                }
                case ARRAY_PREPEND: {
                    mutation = mutation.arrayPrepend(path, node, options);

                    break;
                }
                case ARRAY_INSERT_ALL: {
                    mutation = mutation.arrayInsertAll(path, node, options);

                    break;
                }
                case ARRAY_APPEND_ALL: {
                    mutation = mutation.arrayAppendAll(path, node, options);

                    break;
                }
                case ARRAY_PREPEND_ALL: {
                    mutation = mutation.arrayPrependAll(path, node, options);
                    break;
                }
                case ARRAY_ADD_UNIQUE: {
                    mutation = mutation.arrayAddUnique(path, node, options);
                    break;
                }
                case UPSERT_FIELDS: {
                    // path configuration as a prefix
                    String prefix = path.equals("") ? "" : path + ".";
                    Set<String> paths = node.getNames();
                    // If the message has more than 16 fields, this will fail due to the limit of 16
                    // operations per mutate-in command
                    // (https://developer.couchbase.com/documentation/server/current/developer-guide/sub-doc-api.html#story-h2-5)
                    if (paths.size() <= 16) {
                        for (String p : paths) {
                            Object value = node.get(p);
                            mutation = mutation.upsert(prefix + p, value, options);
                        }
                    } else {
                        LOGGER.warn(
                                "Mutating more than 16 fields. We must retrieve the document, in order to guarantee atomicity.");
                        JsonDocument storedDocument = bucket.get(document.id()).toBlocking().single();
                        if (storedDocument != null) {
                            updateJsonDocumentFields(storedDocument, prefix, paths, node);
                            return bucket.replace(storedDocument, persistTo, replicateTo).toCompletable();
                        } else {
                            if (createDocuments) {
                                JsonDocument newDocument = JsonDocument.create(document.id(), JsonObject.create());
                                updateJsonDocumentFields(JsonDocument.create(document.id()), prefix, paths, node);
                                bucket.insert(newDocument).toBlocking().single();
                            }
                            return Completable.complete();
                        }
                    }
                }
            }
        }

        return mutation.execute(persistTo, replicateTo).doOnError(new Action1<Throwable>() {
            @Override
            public void call(Throwable throwable) {
                if (createDocuments && throwable instanceof DocumentDoesNotExistException) {
                    bucket.insert(JsonDocument.create(document.id())).toBlocking().single();
                }
            }
        }).toCompletable();
    }
    
    private void updateJsonDocumentFields(JsonDocument document, String prefix, Set<String> paths, JsonObject values) {
        JsonObject content = document.content();
        for (String p : paths) {
            Object value = values.get(p);
            content.put(prefix + p, value);
        }
    }
}
