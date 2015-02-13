/*
 * Copyright (c) 2015, erukiti at gmail dot com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.erukiti.pasco1.repository;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.erukiti.pasco1.common.Either;
import org.erukiti.pasco1.model.HashID;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3Repository {
    final private AmazonS3Client s3;
    final private ObjectMapper mapper;
    final private MessageDigest digest;

    @Inject
    public S3Repository(AmazonS3Client s3, ObjectMapper mapper, MessageDigest digest) {
        this.s3 = s3;
        this.mapper = mapper;
        this.digest = digest;
    }

    public Either<Throwable, byte[]> read(String bucket, HashID hashID) {
        if (hashID == null) {
            return Either.createLeft(new NullPointerException());
        }
        S3Object obj = s3.getObject(bucket, hashID.getHash());
        byte[] buffer = new byte[(int)obj.getObjectMetadata().getContentLength()];
        try {
            new DataInputStream(obj.getObjectContent()).readFully(buffer);
            return Either.createRight(buffer);
        } catch (IOException e) {
            return Either.createLeft(e);
        }
    }

    public <T> Either<Throwable, T> readObject(String bucket, HashID hashID, TypeReference<T> typeReference) {
        Either<Throwable, T> ret = read(bucket, hashID).<Either<Throwable, T>>match(err -> {
            return Either.createLeft(err);
        }, bytes -> {
            try {
                return Either.createRight(mapper.readValue(bytes, typeReference));
            } catch (JsonMappingException e) {
                return Either.createLeft(e);
            } catch (JsonParseException e) {
                return Either.createLeft(e);
            } catch (IOException e) {
                return Either.createLeft(e);
            }
        });

        return ret;
    }

    private HashID getHashID(byte[] bytes) {
        final byte[] hash = digest.digest(bytes);
        return new HashID(IntStream.range(0, hash.length).mapToObj(i -> String.format("%02x",hash[i])).collect(Collectors.joining()));
    }

    public Either<Throwable, HashID> write(String bucket, byte[] data) {
        HashID hashID = getHashID(data);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        PutObjectResult result = s3.putObject(bucket, hashID.getHash(), new ByteArrayInputStream(data), metadata);
        // Won(*3*) Chu FixMe!: 成否判定
        return Either.createRight(hashID);
    }

    public <T> Either<Throwable, HashID> writeObject(String bucket, T obj) {
        String json;
        try {
            json = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return Either.createLeft(e);
        }
        return write(bucket, json.getBytes());
    }

    public void createBucket(String name) {
        s3.createBucket(name);
    }
}
