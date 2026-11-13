/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sis.cloud.aws.s3;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

// Test dependencies
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.sis.test.TestCase;


/**
 * Tests {@link PathIterator}, and {@link FileService#checkAccess(Path, java.nio.file.AccessMode...)} on a bucket root,
 * using a fake S3 client.
 *
 * @author  Quentin Bialota (Geomatys)
 */
@SuppressWarnings("exports")
public final class PathIteratorTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public PathIteratorTest() {
    }

    /**
     * A fake S3 server with the given existing buckets. Each bucket contains
     * a "common/" pseudo-directory and a "data.nc" object at its root.
     */
    private static final class FakeClient implements S3Client {
        private final Set<String> buckets;

        FakeClient(final String... buckets) {
            this.buckets = Set.of(buckets);
        }

        @Override public String serviceName() {return "s3";}
        @Override public void close() {}

        @Override
        public HeadBucketResponse headBucket(final HeadBucketRequest request) {
            if (!buckets.contains(request.bucket())) {
                throw NoSuchBucketException.builder().message(request.bucket()).build();
            }
            return HeadBucketResponse.builder().build();
        }

        @Override
        public ListObjectsV2Response listObjectsV2(final ListObjectsV2Request request) {
            assertNull(request.prefix(), "Listing the root of a bucket should not use a prefix.");
            return ListObjectsV2Response.builder()
                    .commonPrefixes(CommonPrefix.builder().prefix("common/").build())
                    .contents(S3Object.builder().key("data.nc").size(10L).build())
                    .isTruncated(false)
                    .build();
        }
    }

    /**
     * Creates a self-hosted file system backed by a fake client.
     */
    static ClientFileSystem createFileSystem(final String... buckets) throws URISyntaxException, ReflectiveOperationException {
        final ClientFileSystem fs = ClientFileSystemTest.create(true);
        final Field client = ClientFileSystem.class.getDeclaredField("client");
        client.setAccessible(true);
        client.set(fs, new FakeClient(buckets));
        return fs;
    }

    /**
     * Returns the root of the given bucket as built from an URI, i.e. without bucket metadata.
     */
    static KeyPath bucketRoot(final ClientFileSystem fs, final String bucket) {
        return new KeyPath(fs, bucket, new String[0], true);
    }

    /**
     * Lists the root of a bucket. Used to fail with a {@link NullPointerException}
     * because the root has no key. Also checks that directory names are only names.
     *
     * @throws Exception if an error occurred while creating the file system or listing the bucket.
     */
    @Test
    public void testListBucketRoot() throws Exception {
        final ClientFileSystem fs = createFileSystem("the-bucket");
        final List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = fs.provider().newDirectoryStream(bucketRoot(fs, "the-bucket"), (p) -> true)) {
            for (final Path child : stream) {
                names.add(child.getFileName().toString());
            }
        }
        assertEquals(List.of("common", "data.nc"), names);
    }

    /**
     * Tests {@code checkAccess} on the root of a bucket built from an URI (no creation date).
     *
     * @throws Exception if an error occurred while creating the file system.
     */
    @Test
    public void testBucketRootExists() throws Exception {
        final ClientFileSystem fs = createFileSystem("the-bucket");
        final FileService service = (FileService) fs.provider();
        assertDoesNotThrow(() -> service.checkAccess(bucketRoot(fs, "the-bucket")));
        assertThrows(java.nio.file.NoSuchFileException.class,
                () -> service.checkAccess(bucketRoot(fs, "missing-bucket")));
        // A bucket listed by the server has a creation date: no request needed.
        final KeyPath listed = new KeyPath(fs, Bucket.builder().name("other").creationDate(java.time.Instant.EPOCH).build());
        assertDoesNotThrow(() -> service.checkAccess(listed));
    }
}
