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
import rx.Observable;
import rx.subjects.ReplaySubject;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ReadAndWriterGenerator {
    final private S3Repository s3Repository;
    final private RedisRepository redisRepository;

    @Inject
    public ReadAndWriterGenerator(S3Repository s3Repository, RedisRepository redisRepository) {
        this.s3Repository = s3Repository;
        this.redisRepository = redisRepository;
    }

    private Pair<HashID, ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> dirGenerator(ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>> subscriber, String bucket, HashID hashID, String[] pathSplited) {
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

    public HashIDChainFunction<ReplaySubject<HashID>> errorWriter(Throwable err) {
        return (prevHashID, subject) -> {
            subject.onError(err);
            return null;
        };
    }

    public Observable<HashIDChainFunction<ReplaySubject<HashID>>> readAndWriterGenerate(List<HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>>> readerList) {
        ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>> subject = ReplaySubject.create();
        Observable.from(readerList).reduce((HashID) null, (prev, func) -> func.chain(prev, subject)).toBlocking().first();
        subject.onCompleted();

        List<HashIDChainFunction<ReplaySubject<HashID>>> writerList = subject.onErrorReturn(e -> errorWriter(e)).toList().toBlocking().first();
        Collections.reverse(writerList);
        return Observable.from(writerList);
    }

    public Either<Throwable, HashID> write(Observable<HashIDChainFunction<ReplaySubject<HashID>>> writerStream) {
        ReplaySubject<HashID> subject = ReplaySubject.create();
        writerStream.reduce((HashID) null, (prev, func) -> func.chain(prev, subject)).toBlocking().first();
        subject.onCompleted();

        return subject.concatMap(hashID -> Observable.just(Either.<Throwable, HashID>createRight(hashID))).onErrorReturn(Either::<Throwable, HashID>createLeft).toBlocking().first();
    }

    public HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> dirReadAndWriterGenerator(String team, String path) {
        return (prevHashID, subject) -> dirGenerator(subject, team, prevHashID, path.split("/")).getLeft();
    }

    public HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> blobReadAndWriterGenerator(String team, String text) {
        return (prevHashID, subject) -> {
            subject.onNext(blobWriter(team, text));
            return prevHashID;
        };
    }

    public HashIDChainFunction<ReplaySubject<HashID>> blobWriter(String team, String text) {
        return (prevHashID, subject) -> s3Repository.write(team, text.getBytes()).match(err -> {
            subject.onError(err);
            return null;
        }, nextHashWrite -> {
            subject.onNext(nextHashWrite);
            return nextHashWrite;
        });
    }

    public HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> metaReadAndWriterGenerator(String team) {
        return (prevHashIDRead, subject) -> {
            return s3Repository.readObject(team, prevHashIDRead, new TypeReference<Meta>() {
            }).match(err -> {
                Meta meta = new Meta();
                subject.onNext(metaWriter(team, meta));
                return prevHashIDRead;
            }, meta -> {
                meta.previous = meta.hashID;
                subject.onNext(metaWriter(team, meta));
                return prevHashIDRead;
            });
        };
    }

    public HashIDChainFunction<ReplaySubject<HashID>> metaWriter(String team, Meta meta) {
        return (prevHashIDWrite, subject) -> {
            meta.hashID = prevHashIDWrite;
            return s3Repository.writeObject(team, meta).match(err -> {
                subject.onError(err);
                return null;
            }, nextHashIDWrite -> {
                return nextHashIDWrite;
            });
        };
    }

    public HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> bucketReadAndWriterGenerator(String team) {
        return (prevHashIDRead, subject) -> {
            return s3Repository.readObject(team, prevHashIDRead, new TypeReference<Bucket>(){}).match(
                    err -> {
                        subject.onError(err);
                        return null;
                    }, bucket -> {
                        subject.onNext(bucketWriter(team, bucket));
                        return bucket.hashID;
                    }
            );
        };
    }

    public HashIDChainFunction<ReplaySubject<HashID>> bucketWriter(String team, Bucket bucket) {
        return (prevHashIDWrite, obj) -> {
            bucket.previous = bucket.hashID;
            bucket.hashID = prevHashIDWrite;

            return s3Repository.writeObject(team, bucket).match(err -> {
                return null;
            }, nextHashIDWrite -> {
                return nextHashIDWrite;
            });
        };
    }

    public HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>> redisReadAndWriterGenerator(String team, Jedis jedis) {
        return (prevHashIDRead, subject) -> {
            return redisRepository.readHashID(jedis, "bucket-" + team).match(
                    err -> {
                        subject.onError(err);
                        return null;
                    }, hashID -> {
                        subject.onNext(redisWriter(team, jedis));
                        return hashID;
                    }
            );
        };
    }

    public HashIDChainFunction<ReplaySubject<HashID>> redisWriter(String team, Jedis jedis) {
        return (prevHashIDWrite, obj) -> {
            redisRepository.writeHashID(jedis, "bucket-" + team, prevHashIDWrite);
            return prevHashIDWrite;
        };
    }

}
