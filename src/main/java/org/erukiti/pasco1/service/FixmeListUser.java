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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.erukiti.pasco1.model.HashID;
import org.erukiti.pasco1.repository.S3Observable;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.model.User;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;

import java.util.Map;

public class FixmeListUser {
    final private JedisPool pool;
    final private S3Observable s3Observable;
    final private ObjectMapper mapper;

    @Inject
    public FixmeListUser(JedisPool pool, S3Observable s3Observable, ObjectMapper mapper) {
        this.pool = pool;
        this.s3Observable = s3Observable;
        this.mapper = mapper;
    }

    private Observable<HashID> searchUser(HashID hashID) {
        Observable<Map<HashID, TreeNode>> stream = s3Observable.read("user", hashID, new TypeReference<Map<HashID, TreeNode>>(){});
        return stream.flatMap(treeNodeMap -> {
            return Observable.create((Observable.OnSubscribe<HashID>) subscriber -> {
                treeNodeMap.forEach((key, treeNode) -> {
                    if (treeNode.type == TreeNode.Type.File) {
                        subscriber.onNext(treeNode.hashId);
                    } else {
                        searchUser(treeNode.hashId).subscribe(
                                hashID2 -> subscriber.onNext(hashID2),
                                err -> subscriber.onError(err)
                        );
                    }
                });
                subscriber.onCompleted();
            });
        });
    }

    public void list() {
        try (Jedis jedis = pool.getResource()) {
            Observable<HashID> hashIDStream = Observable.create((Observable.OnSubscribe<HashID>)subscriber -> {
                HashID hashID = new HashID(jedis.get("user"));
                if (hashID.getHash() != null) {
                    subscriber.onNext(hashID);
                }
                subscriber.onCompleted();
            });
            Observable<User> userStream = hashIDStream
                    .flatMap(this::searchUser)
                    .flatMap(hashID -> s3Observable.read("user", hashID, new TypeReference<User>(){}));
            userStream.subscribe(user -> {
                if (user.isAdmin) {
                    System.out.printf("* %s(%s)\n", user.id, user.nick);
                } else {
                    System.out.printf("  %s(%s)\n", user.id, user.nick);
                }
            }, err -> {
                err.printStackTrace();
            });
        }
        pool.destroy();
    }
}
