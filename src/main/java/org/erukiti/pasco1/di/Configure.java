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

package org.erukiti.pasco1.di;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.stream.Collectors;

public class Configure {
    final private Injector injector;

    public Configure(String confpath) throws IOException, NoSuchAlgorithmException {
        ObjectMapper mapper = new ObjectMapper();

        String configJson = Files.readAllLines(Paths.get(confpath)).stream().collect(Collectors.joining());
        Map<String, String> config = mapper.readValue(configJson, new TypeReference<Map<String, String>>(){});

        String s3Key = config.get("s3_key");
        String s3Secret = config.get("s3_secret");
        String s3Host = config.get("s3_host");
        Integer s3Port = Integer.parseInt(config.get("s3_port"));
        String digestAlgorithm = config.get("digest_algorithm");
        String redisHost = config.get("redis_host");

        BasicAWSCredentials credentials = new BasicAWSCredentials(s3Key, s3Secret);
        ClientConfiguration conf = new ClientConfiguration();
        conf.setProtocol(Protocol.HTTP);
        conf.setProxyHost(s3Host);
        conf.setProxyPort(s3Port);
        AmazonS3Client s3 = new AmazonS3Client(credentials, conf);

        MessageDigest digest;
        digest = MessageDigest.getInstance(digestAlgorithm);

        JedisPool pool = new JedisPool(new JedisPoolConfig(), redisHost);

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(AmazonS3Client.class).toInstance(s3);
                bind(MessageDigest.class).toInstance(digest);
                bind(ObjectMapper.class).toInstance(mapper);
                bind(JedisPool.class).toInstance(pool);
            }
        });

        this.injector = injector;
    }

    public Injector getInjector() {
        return injector;
    }
}
