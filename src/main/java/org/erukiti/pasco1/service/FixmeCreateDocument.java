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
import org.erukiti.pasco1.model.HashID;
import org.erukiti.pasco1.model.Meta;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.repository.S3Observable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;

public class FixmeCreateDocument {
    final private S3Observable s3Observable;
    final private JedisPool pool;

    @Inject
    public FixmeCreateDocument(S3Observable s3Observable, JedisPool pool) {
        this.s3Observable = s3Observable;
        this.pool = pool;
    }

    private Pair<HashID, PublishSubject<HashIDChainFunction<HashID>>> generator(PublishSubject<HashIDChainFunction<HashID>> subscriber, String bucket, HashID hashID, String[] pathSplited) {
        HashMap<String, TreeNode> treeNodeMap;
        if (hashID == null)  {
            treeNodeMap = new HashMap<>();
        } else {
            treeNodeMap = s3Observable.read(bucket, hashID, new TypeReference<HashMap<String, TreeNode>>() {}).toBlocking().first();
        }
        TreeNode treeNode = treeNodeMap.get(pathSplited[0]);
        HashID nextHashID = null;
        if (treeNode != null) {
            if (pathSplited.length > 1 && treeNode.type == TreeNode.Type.File) {
                subscriber.onError(new IllegalArgumentException("path is wrong"));
                return Pair.of(null, subscriber);
            }
            if (pathSplited.length == 1 && treeNode.type == TreeNode.Type.Dir) {
                subscriber.onError(new IllegalArgumentException("path is wrong"));
                return Pair.of(null, subscriber);
            }
            nextHashID = treeNode.hashId;
        }

        TreeNode.Type type;
        if (pathSplited.length > 1) {
            type = TreeNode.Type.Dir;
        } else {
            type = TreeNode.Type.File;
        }

        subscriber.onNext((prevHashIDRead, obj) -> {
            treeNodeMap.put(pathSplited[0], new TreeNode(prevHashIDRead, type));
            return s3Observable.writeObject(bucket, treeNodeMap).toBlocking().first();
        });

        if (pathSplited.length > 1) {
            return generator(subscriber, bucket, nextHashID, Arrays.copyOfRange(pathSplited, 1, pathSplited.length));
        } else {
            return Pair.of(nextHashID, subscriber);
        }
    }

    // Won(*3*) Chu FixMe!: もうちょっと簡単な仕組みにできないものか
    public void createDocument(String team, String path, String text) {
        // Won(*3*) Chu FixMe!: path の正規化

        try (Jedis jedis = pool.getResource()) {
            LinkedList<HashIDChainFunction<PublishSubject<HashIDChainFunction<HashID>>>> list = new LinkedList<>();

            list.add((prevHashIDRead, subscriber) -> {
                try {
                    HashID hashID = new HashID(jedis.get("bucket-" + team));
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        jedis.set("bucket-" + team, prevHashIDWrite.getHash());
                        return prevHashIDWrite;
                    });
                    return hashID;
                } catch(IllegalArgumentException e) {
                    subscriber.onError(e);
                    return null;
                }
            });

            list.add((prevHashIDRead, subscriber) -> {
                try {
                    Bucket bucket = s3Observable.read(team, prevHashIDRead, new TypeReference<Bucket>() {}).toBlocking().first();
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        bucket.previous = bucket.hashID;
                        bucket.hashID = prevHashIDWrite;
                        return s3Observable.writeObject(team, bucket).toBlocking().first();
                    });

                    return bucket.hashID;
                } catch (Throwable e) {
                    subscriber.onError(e);
                    return null;
                }

            });

            list.add((prevHashIDRead, subscriber) -> generator(subscriber, team, prevHashIDRead, path.split("/")).getLeft());

            list.add((prevHashIDRead, subscriber) -> {
                try {
                    Meta meta = s3Observable.read(team, prevHashIDRead, new TypeReference<Meta>(){}).toBlocking().first();
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        meta.previous = meta.hashID;
                        meta.hashID = prevHashIDWrite;
                        return s3Observable.writeObject(team, meta).toBlocking().first();
                    });
                    return new HashID("Dummy");
                } catch (Throwable e) {
                    Meta meta2 = new Meta();
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        meta2.previous = meta2.hashID;
                        meta2.hashID = prevHashIDWrite;
                        return s3Observable.writeObject(team, meta2).toBlocking().first();
                    });
                    return null;
                }
            });

            list.add((dummyRead, subscriber) -> {
                subscriber.onNext((dummyWrite, obj) -> s3Observable.writeText(team, text).toBlocking().first());
                return null;
            });

            PublishSubject<HashIDChainFunction<HashID>> subject = PublishSubject.create();
            Observable.from(list).reduce((HashID)null, (prev, func) -> func.chain(prev, subject))
                    .subscribe(dummy -> {
                        subject.onCompleted();
                        subject.reduce((HashID)null, (prev, func) -> func.chain(prev, null))
                                .subscribe(dummy2 -> System.out.println("success"), Throwable::printStackTrace);
            }, Throwable::printStackTrace);
        }
        pool.destroy();
    }
}
