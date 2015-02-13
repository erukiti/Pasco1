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
import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.erukiti.pasco1.model.HashID;
import org.erukiti.pasco1.repository.S3Observable;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.model.User;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;

import java.util.*;
import java.util.function.Function;

public class FixmeCreateUser {
    final private JedisPool pool;
    final private S3Observable s3Observable;

    @Inject
    public FixmeCreateUser(JedisPool pool, S3Observable s3Observable) {
        this.pool = pool;
        this.s3Observable = s3Observable;
    }

    private Observable<Function<HashID, Observable<HashID>>> generator(HashID hashID, String[] pathSplitted) {
        Observable<Map<String, TreeNode>> stream;
        if (hashID == null)  {
            stream = Observable.just(new HashMap<String, TreeNode>(){});
        } else {
            stream = s3Observable.read("user", hashID, new TypeReference<Map<String, TreeNode>>() {});
        }
        return stream.flatMap(treeNodeMap -> {
            TreeNode treeNode = treeNodeMap.get(pathSplitted[0]);
            HashID nextHashID = null;
            if (treeNode != null) {
                if (pathSplitted.length > 1 && treeNode.type == TreeNode.Type.File) {
                    return Observable.error(new IllegalArgumentException("path is wrong"));
                }
                if (pathSplitted.length == 1 && treeNode.type == TreeNode.Type.Dir) {
                    return Observable.error(new IllegalArgumentException("path is wrong"));
                }
                nextHashID = treeNode.hashId;
            }
            TreeNode.Type type;
            Observable<Function<HashID, Observable<HashID>>> ret;
            if (pathSplitted.length > 1) {
                type = TreeNode.Type.Dir;
                ret = generator(nextHashID, Arrays.copyOfRange(pathSplitted, 1, pathSplitted.length));
            } else {
                type = TreeNode.Type.File;
                ret = Observable.empty();
            }

            Function<HashID, Observable<HashID>> function = hashId -> {
                treeNodeMap.put(pathSplitted[0], new TreeNode(hashId, type));
                return s3Observable.writeObject("user", treeNodeMap);
            };
            return ret.mergeWith(Observable.just(function));
        });
    }

    // Won(*3*)Chu FixMe!: CreateDocument と処理の共通化できる部分を共通化する
    public void create(User user) {
        String path = user.id.substring(0, 2) + "/" + user.id;

        try (Jedis jedis = pool.getResource()) {
            Observable<Pair<HashID, String[]>> stream = Observable.just(Pair.of(new HashID(jedis.get("user")), path.split("/")));

            Function<HashID, Observable<HashID>> blobWriteFunction = hashID -> s3Observable.writeObject("user", user);

            Observable<Function<HashID, Observable<HashID>>> writeStream = Observable.just(blobWriteFunction).mergeWith(stream
                    .concatMap(pair -> generator(pair.getLeft(), pair.getRight())));

            writeStream.reduce(new HashID(""), (hashID, x) -> x.apply(hashID).toBlocking().first()).subscribe(hashID -> {
                jedis.set("user", hashID.getHash());
            }, err -> {
                err.printStackTrace();
            });
        }
        pool.destroy();
    }

}
