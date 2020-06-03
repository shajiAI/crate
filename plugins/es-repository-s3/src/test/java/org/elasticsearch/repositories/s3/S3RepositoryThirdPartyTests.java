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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.BlobStoreRepositoryTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;


public class S3RepositoryThirdPartyTests extends BlobStoreRepositoryTestCase {

    @Override
    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetaData> blobs) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertBlobsByPrefix(path, prefix, blobs), 10L, TimeUnit.MINUTES);
    }

    @Override
    protected void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertChildren(path, children), 10L, TimeUnit.SECONDS);
    }

    @Override
    protected S3Repository getRepository() {

        String bucket = "";
        ByteSizeValue bufferSize = new ByteSizeValue(randomIntBetween(5, 100), ByteSizeUnit.MB);
        boolean serverSideEncryption = randomBoolean();

        String cannedACL = null;
        if (randomBoolean()) {
            cannedACL = randomFrom(CannedAccessControlList.values()).toString();
        }

        String storageClass = null;
        if (randomBoolean()) {
            storageClass = randomValueOtherThan(
                StorageClass.Glacier,
                () -> randomFrom(StorageClass.values())).toString();
        }

        final AmazonS3 client = new MockAmazonS3(new ConcurrentHashMap<>(), bucket, false, null, null);
        final S3Service service = new S3Service() {
            @Override
            public synchronized AmazonS3Reference client(RepositoryMetaData metadata) {
                return new AmazonS3Reference(client);
            }
        };

        final RepositoryMetaData metadata = new RepositoryMetaData("dummy-repo", "mock", Settings.builder()
            .put(S3RepositorySettings.BASE_PATH_SETTING.getKey(), "foo/bar").build());
        S3Repository test = new S3Repository(metadata,
                                             Settings.EMPTY,
                                             NamedXContentRegistry.EMPTY,
                                             service,
                                             new TestThreadPool("test"));
        test.start();
        return test;
    }
}

