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

package org.erukiti.pasco1.service;

import com.google.inject.Inject;
import org.erukiti.pasco1.model.Bucket;
import org.erukiti.pasco1.repository.RedisRepository;
import org.erukiti.pasco1.repository.S3Observable;
import org.erukiti.pasco1.repository.S3Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class FixmeCreateTeam {
    final private S3Repository s3Repository;
    final private RedisRepository redisRepository;
    final private JedisPool pool;

    @Inject
    public FixmeCreateTeam(S3Observable s3Observable, S3Repository s3Repository, RedisRepository redisRepository, JedisPool pool) {
        this.s3Repository = s3Repository;
        this.redisRepository = redisRepository;
        this.pool = pool;
    }

    public void createTeam(String id, String name, boolean isPrivate, String[] admin) {
        // Won(*3*)Chu FixMe: id の validation

        try (Jedis jedis = pool.getResource()) {
            s3Repository.createBucket(id);
            Bucket bucket = new Bucket(name, isPrivate, admin);
            s3Repository.writeObject(id, bucket).match(err -> {
                err.printStackTrace();
                return null;
            }, hashID -> {
                redisRepository.writeHashID(jedis, "bucket-" + id, hashID).ifPresent(err -> {
                    err.printStackTrace();
                });
                return null;
            });
        }
        pool.destroy();
    }

}
