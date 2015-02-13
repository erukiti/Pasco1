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

import com.google.inject.Inject;
import org.erukiti.pasco1.common.Either;
import org.erukiti.pasco1.model.HashID;
import redis.clients.jedis.Jedis;

import java.util.Optional;

public class RedisRepository {

    @Inject
    public RedisRepository() {
    }

    public Either<Throwable, HashID> readHashID(Jedis jedis, String key) {
        String hash = jedis.get(key);
        if (hash == null) {
            return Either.createLeft(new Throwable("redis key not found"));
        }
        try {
            return Either.createRight(new HashID(hash));
        } catch (IllegalArgumentException e) {
            return Either.createLeft(e);
        }
    }

    public Optional<Throwable> writeHashID(Jedis jedis, String key, HashID hashID) {
        jedis.set(key, hashID.getHash());
        // Won(*3*) Chu FixMe!: 成否判定
        return Optional.empty();
    }
}
