/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import io.crate.common.collections.Tuple;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.repositories.s3.S3RepositorySettings.MAX_FILE_SIZE;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.MAX_FILE_SIZE_USING_MULTIPART;
import static org.elasticsearch.repositories.s3.S3RepositorySettings.MIN_PART_SIZE_USING_MULTIPART;

class S3BlobContainer extends AbstractBlobContainer {

    /**
     * Maximum number of deletes in a {@link DeleteObjectsRequest}.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html">S3 Documentation</a>.
     */
    private static final int MAX_BULK_DELETES = 1000;

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return clientReference.client().doesObjectExist(blobStore.bucket(), buildKey(blobName));
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final S3Object s3Object = clientReference.client().getObject(blobStore.bucket(), buildKey(blobName));
            return s3Object.getObjectContent();
        } catch (final AmazonClientException e) {
            if (e instanceof AmazonS3Exception) {
                if (404 == ((AmazonS3Exception) e).getStatusCode()) {
                    throw new NoSuchFileException("Blob object [" + blobName + "] not found: " + e.getMessage());
                }
            }
            throw e;
        }
    }

    /**
     * This implementation ignores the failIfAlreadyExists flag as the S3 API has no way to enforce this due to its weak consistency model.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        if (blobSize <= blobStore.bufferSizeInBytes()) {
            executeSingleUpload(blobStore, buildKey(blobName), inputStream, blobSize);
        } else {
            executeMultipartUpload(blobStore, buildKey(blobName), inputStream, blobSize);
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        if (blobExists(blobName) == false) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        deleteBlobIgnoringIfNotExists(blobName);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // S3 API only allows 1k blobs per delete so we split up the given blobs into requests of max. 1k deletes
            final List<DeleteObjectsRequest> deleteRequests = new ArrayList<>();
            final List<String> partition = new ArrayList<>();
            for (String blob : blobNames) {
                partition.add(buildKey(blob));
                if (partition.size() == MAX_BULK_DELETES) {
                    deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
                    partition.clear();
                }
            }
            if (partition.isEmpty() == false) {
                deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
            }
            AmazonClientException aex = null;
            for (DeleteObjectsRequest deleteRequest : deleteRequests) {
                try {
                    clientReference.client().deleteObjects(deleteRequest);
                } catch (AmazonClientException e) {
                    if (aex == null) {
                        aex = e;
                    } else {
                        aex.addSuppressed(e);
                    }
                }
            }
            if (aex != null) {
                throw aex;
            }
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when deleting blobs [" + blobNames + "]", e);
        }
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return new DeleteObjectsRequest(bucket).withKeys(blobs.toArray(Strings.EMPTY_ARRAY)).withQuiet(true);
    }

    @Override
    public void deleteBlobIgnoringIfNotExists(String blobName) throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // There is no way to know if an non-versioned object existed before the deletion
            clientReference.client().deleteObject(blobStore.bucket(), buildKey(blobName));
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when deleting blob [" + blobName + "]", e);
        }
    }


    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        final HashMap<String, BlobMetaData> blobsBuilder = new HashMap<>();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            ObjectListing prevListing = null;
            while (true) {
                ObjectListing list;
                if (prevListing != null) {
                    final ObjectListing finalPrevListing = prevListing;
                    list = clientReference.client().listNextBatchOfObjects(finalPrevListing);
                } else {
                    if (blobNamePrefix != null) {
                        list = clientReference.client().listObjects(blobStore.bucket(), buildKey(blobNamePrefix));
                    } else {
                        list = clientReference.client().listObjects(blobStore.bucket(), keyPath);
                    }
                }
                for (final S3ObjectSummary summary : list.getObjectSummaries()) {
                    final String name = summary.getKey().substring(keyPath.length());
                    blobsBuilder.put(name, new PlainBlobMetaData(name, summary.getSize()));
                }
                if (list.isTruncated()) {
                    prevListing = list;
                } else {
                    break;
                }
            }
            return Map.copyOf(blobsBuilder);
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(final S3BlobStore blobStore,
                             final String blobName,
                             final InputStream input,
                             final long blobSize) throws IOException {

        // Extra safety checks
        if (blobSize > MAX_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
        }
        if (blobSize > blobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
        }

        final ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(blobSize);
        if (blobStore.serverSideEncryption()) {
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
        final PutObjectRequest putRequest = new PutObjectRequest(blobStore.bucket(), blobName, input, md);
        putRequest.setStorageClass(blobStore.getStorageClass());
        putRequest.setCannedAcl(blobStore.getCannedACL());

        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            clientReference.client().putObject(putRequest);
        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(final S3BlobStore blobStore,
                                final String blobName,
                                final InputStream input,
                                final long blobSize) throws IOException {

        if (blobSize > MAX_FILE_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException("Multipart upload request size [" + blobSize
                                                + "] can't be larger than " + MAX_FILE_SIZE_USING_MULTIPART);
        }
        if (blobSize < MIN_PART_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException("Multipart upload request size [" + blobSize
                                               + "] can't be smaller than " + MIN_PART_SIZE_USING_MULTIPART);
        }

        final long partSize = blobStore.bufferSizeInBytes();
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger buffer size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = blobStore.bucket();
        boolean success = false;

        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, blobName);
        initRequest.setStorageClass(blobStore.getStorageClass());
        initRequest.setCannedACL(blobStore.getCannedACL());
        if (blobStore.serverSideEncryption()) {
            final ObjectMetadata md = new ObjectMetadata();
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            initRequest.setObjectMetadata(md);
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {

            uploadId.set(clientReference.client().initiateMultipartUpload(initRequest).getUploadId());
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
            }

            final List<PartETag> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final UploadPartRequest uploadRequest = new UploadPartRequest();
                uploadRequest.setBucketName(bucketName);
                uploadRequest.setKey(blobName);
                uploadRequest.setUploadId(uploadId.get());
                uploadRequest.setPartNumber(i);
                uploadRequest.setInputStream(input);

                if (i < nbParts) {
                    uploadRequest.setPartSize(partSize);
                    uploadRequest.setLastPart(false);
                } else {
                    uploadRequest.setPartSize(lastPartSize);
                    uploadRequest.setLastPart(true);
                }
                bytesCount += uploadRequest.getPartSize();

                final UploadPartResult uploadResponse = clientReference.client().uploadPart(uploadRequest);
                parts.add(uploadResponse.getPartETag());
            }

            if (bytesCount != blobSize) {
                throw new IOException("Failed to execute multipart upload for [" + blobName + "], expected " + blobSize
                    + "bytes sent but got " + bytesCount);
            }

            final CompleteMultipartUploadRequest complRequest = new CompleteMultipartUploadRequest(bucketName, blobName, uploadId.get(),
                    parts);
            clientReference.client().completeMultipartUpload(complRequest);
            success = true;

        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucketName, blobName, uploadId.get());
                try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                    clientReference.client().abortMultipartUpload(abortRequest);
                }
            }
        }
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if ((totalSize == 0L) || (totalSize <= partSize)) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }
}
