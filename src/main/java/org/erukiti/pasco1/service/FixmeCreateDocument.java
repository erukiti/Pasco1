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
import org.erukiti.pasco1.common.Either;
import org.erukiti.pasco1.model.Bucket;
import org.erukiti.pasco1.model.HashID;
import org.erukiti.pasco1.model.Meta;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.repository.RedisRepository;
import org.erukiti.pasco1.repository.S3Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.*;

public class FixmeCreateDocument {
    final private JedisPool pool;
    final private S3Repository s3Repository;
    final private RedisRepository redisRepository;

    @Inject
    public FixmeCreateDocument(S3Repository s3Repository, JedisPool pool, RedisRepository redisRepository) {
        this.pool = pool;
        this.s3Repository = s3Repository;
        this.redisRepository = redisRepository;
    }

    private Pair<HashID, PublishSubject<HashIDChainFunction<HashID>>> dirGenerator(PublishSubject<HashIDChainFunction<HashID>> subscriber, String bucket, HashID hashID, String[] pathSplited) {
        final HashMap<String, TreeNode> treeNodeMap;
        if (hashID == null)  {
            treeNodeMap = new HashMap<>();
        } else {
            Either<Throwable, HashMap<String, TreeNode>> result = s3Repository.readObject(bucket, hashID, new TypeReference<HashMap<String, TreeNode>>() {});
            if (result.getLeft().isPresent()) {
                subscriber.onError(result.getLeft().get());
                return Pair.of(null, subscriber);
            } else {
                treeNodeMap = result.getRight().get();
            }
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
            return s3Repository.writeObject(bucket, treeNodeMap).match(err -> {
                return null;
            }, nextHashIDWrite -> {
                return nextHashIDWrite;
            });
        });

        if (pathSplited.length > 1) {
            return dirGenerator(subscriber, bucket, nextHashID, Arrays.copyOfRange(pathSplited, 1, pathSplited.length));
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
                return redisRepository.readHashID(jedis, "bucket-" + team).match(
                        err -> {
                            subscriber.onError(err);
                            return null;
                        }, hashID -> {
                            subscriber.onNext((prevHashIDWrite, obj) -> {
                                redisRepository.writeHashID(jedis, "bucket-" + team, prevHashIDWrite);
                                return prevHashIDWrite;
                            });
                            return hashID;
                        }
                );
            });

            list.add((prevHashIDRead, subscriber) -> {
                return s3Repository.readObject(team, prevHashIDRead, new TypeReference<Bucket>(){}).match(
                        err -> {
                            subscriber.onError(err);
                            return null;
                        }, bucket -> {
                            subscriber.onNext((prevHashIDWrite, obj) -> {
                                bucket.previous = bucket.hashID;
                                bucket.hashID = prevHashIDWrite;

                                return s3Repository.writeObject(team, bucket).match(err -> {
                                    return null;
                                }, nextHashIDWrite -> {
                                    return nextHashIDWrite;
                                });
                            });
                            return bucket.hashID;
                        }
                );
            });

            list.add((prevHashIDRead, subscriber) -> dirGenerator(subscriber, team, prevHashIDRead, path.split("/")).getLeft());

            list.add((prevHashIDRead, subscriber) -> {
                return s3Repository.readObject(team, prevHashIDRead, new TypeReference<Meta>() {
                }).match(err -> {
                    Meta meta = new Meta();
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        meta.hashID = prevHashIDWrite;
                        return s3Repository.writeObject(team, meta).match(err2 -> {
                            return null;
                        }, nextHashIDWrite -> {
                            return nextHashIDWrite;
                        });
                    });
                    return null;
                }, meta -> {
                    subscriber.onNext((prevHashIDWrite, obj) -> {
                        meta.previous = meta.hashID;
                        meta.hashID = prevHashIDWrite;
                        return s3Repository.writeObject(team, meta).match(err2 -> {
                            return null;
                        }, nextHashIDWrite -> {
                            return nextHashIDWrite;
                        });
                    });
                    return null;
                });
            });

            list.add((dummyRead, subscriber) -> {
                subscriber.onNext((dummyWrite, obj) -> s3Repository.write(team, text.getBytes()).match(err -> {
                    return null;
                }, nextHashWrite -> {
                    return nextHashWrite;
                }));
                return null;
            });

            PublishSubject<HashIDChainFunction<HashID>> subject = PublishSubject.create();
            Observable.from(list).reduce((HashID)null, (prev, func) -> func.chain(prev, subject))
                    .subscribe(dummy -> {
                        subject.onCompleted();
                        subject.reduce((HashID) null, (prev, func) -> func.chain(prev, null))
                                .subscribe(dummy2 -> System.out.println("success"),
                                        Throwable::printStackTrace);
                    }, Throwable::printStackTrace);
        }
        pool.destroy();
    }
}
