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
import org.erukiti.pasco1.model.Bucket;
import org.erukiti.pasco1.model.Meta;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.repository.S3Observable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;

import java.util.*;
import java.util.function.Function;

public class FixmeCreateDocument {
    final private S3Observable s3Observable;
    final private JedisPool pool;

    @Inject
    public FixmeCreateDocument(S3Observable s3Observable, JedisPool pool) {
        this.s3Observable = s3Observable;
        this.pool = pool;
    }

    private Observable<Function<String, Observable<String>>> generator(String bucket, String hashID, String[] pathSplitted) {
        Observable<Map<String, TreeNode>> stream;
        if (hashID == null)  {
            stream = Observable.just(new HashMap<String, TreeNode>(){});
        } else {
            stream = s3Observable.read(bucket, hashID, new TypeReference<Map<String, TreeNode>>() {});
        }
        return stream.flatMap(treeNodeMap -> {
            TreeNode treeNode = treeNodeMap.get(pathSplitted[0]);
            String nextHashID = null;
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
            Observable<Function<String, Observable<String>>> ret;
            if (pathSplitted.length > 1) {
                type = TreeNode.Type.Dir;
                ret = generator(bucket, nextHashID, Arrays.copyOfRange(pathSplitted, 1, pathSplitted.length));
            } else {
                type = TreeNode.Type.File;
                ret = Observable.empty();
            }

            Function<String, Observable<String>> function = hashId -> {
                treeNodeMap.put(pathSplitted[0], new TreeNode(hashId, type));
                return s3Observable.writeObject(bucket, treeNodeMap);
            };
            return ret.mergeWith(Observable.just(function));
        });
    }

    // Won(*3*) Chu FixMe!: まじめに書き直す
    public void createDocument(String team, String path, String text) {
        // Won(*3*) Chu FixMe!: path の正規化

        try (Jedis jedis = pool.getResource()) {
            LinkedList<Function<Pair<String, Observable<Function<String, Observable<String>>>>, Pair<String, Observable<Function<String, Observable<String>>>>>> list = new LinkedList();
            list.add(pair -> {
                String hashID = jedis.get("bucket-" + team);
                Observable<Function<String, Observable<String>>> stream = pair.getRight();

                Function<String, Observable<String>> func = hashID2 -> {
                    jedis.set("bucket-" + team, hashID2);
                    return Observable.empty();
                };

                return Pair.of(hashID, Observable.just(func).mergeWith(stream));
            });

            list.add(pair -> {
                String hashID = pair.getLeft();
                Observable<Function<String, Observable<String>>> stream = pair.getRight();

                Bucket bucket = s3Observable.read(team, hashID, new TypeReference<Bucket>() {}).toBlocking().first();

                Function<String, Observable<String>> bucketWriteFunction = hashID2 -> {
                    bucket.previous = bucket.hashID;
                    bucket.hashID = hashID2;
                    return s3Observable.writeObject(team, bucket);
                };

                return Pair.of(bucket.hashID, Observable.just(bucketWriteFunction).mergeWith(stream));
            });

            list.add(pair -> {
                String hashID = pair.getLeft();
                Observable<Function<String, Observable<String>>> stream = pair.getRight();

                return Pair.of("", Observable.just(Pair.of(hashID, path.split("/"))).concatMap(pair2 -> generator(team, pair2.getLeft(), pair2.getRight())).mergeWith(stream));
            });

            list.add(pair -> {
                Observable<Function<String, Observable<String>>> stream = pair.getRight();

                Function<String, Observable<String>> metaWriteFunction = hashID -> {
                    Meta meta = new Meta();
                    meta.hashID = hashID;
                    return s3Observable.writeObject(team, meta);
                };

                return Pair.of("", Observable.just(metaWriteFunction).mergeWith(stream));
            });

            list.add(pair -> {
                Observable<Function<String, Observable<String>>> stream = pair.getRight();

                Function<String, Observable<String>> blobWriteFunction = hashID -> s3Observable.writeText(team, text);

                return Pair.of("", Observable.just(blobWriteFunction).mergeWith(stream));
            });

            Pair<String, Observable<Function<String, Observable<String>>>> init = Pair.of("", Observable.<Function<String, Observable<String>>>empty());
            Observable.from(list).reduce(init, (pair, func) -> func.apply(pair)).subscribe(
                    hoge -> {
                        System.out.println(hoge.getLeft());
                        System.out.println(hoge);
                        hoge.getRight().reduce("", (hashID, x) -> {
                            return x.apply(hashID).toBlocking().first();
                        }).subscribe(hashID -> {
                            System.out.println(hashID);
                        }, err -> {
                            err.printStackTrace();
                        });
                    },
                    err -> {
                        err.printStackTrace();
                    }
            );
        }
        pool.destroy();
    }
}
