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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import rx.Observable;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class S3Observable {
    final private AmazonS3Client s3;
    final private ObjectMapper mapper;
    final private MessageDigest digest;

    @Inject
    public S3Observable(AmazonS3Client s3, ObjectMapper mapper, MessageDigest digest) {
        this.s3 = s3;
        this.mapper = mapper;
        this.digest = digest;
    }

    public <T> Observable<T> read(String bucket, String hashID, TypeReference<T> typeReference) {
        return Observable.create((Observable.OnSubscribe<T>)subscriber -> {
            S3Object obj = s3.getObject(bucket, hashID);
            BufferedReader br = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
            String text = br.lines().collect(Collectors.joining());
            try {
                T data = mapper.readValue(text, typeReference);
                subscriber.onNext(data);
                subscriber.onCompleted();
            } catch (IOException e) {
                subscriber.onError(e);
            }
        });
    }

    private String getHashID(byte[] bytes) {
        final byte[] hash = digest.digest(bytes);
        return IntStream.range(0, hash.length).mapToObj(i -> String.format("%02x",hash[i])).collect(Collectors.joining());
    }

    // Won(*3*) Chu FixMe: writeTextを使うように書き換える
    public <T> Observable<String> writeObject(String bucket, T obj) {
        String json;
        try {
            json = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return Observable.error(e);
        }

        String hashID = getHashID(json.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(json.getBytes().length);
        PutObjectResult result = s3.putObject(bucket, hashID, new ByteArrayInputStream(json.getBytes()), metadata);
//        System.out.println(result);
        return Observable.just(hashID);
    }

    public Observable<String> writeText(String bucket, String text) {
        String hashID = getHashID(text.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(text.getBytes().length);
        PutObjectResult result = s3.putObject(bucket, hashID, new ByteArrayInputStream(text.getBytes()), metadata);
        return Observable.just(hashID);
    }

    public void createBucket(String name) {
        s3.createBucket(name);
    }
}
