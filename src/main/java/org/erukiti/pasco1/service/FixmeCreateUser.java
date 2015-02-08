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
import org.apache.commons.lang3.tuple.Pair;
import org.erukiti.pasco1.repository.S3Observable;
import org.erukiti.pasco1.model.TreeNode;
import org.erukiti.pasco1.model.TreeNodes;
import org.erukiti.pasco1.model.User;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;

import java.util.Arrays;
import java.util.function.Function;

public class FixmeCreateUser {
    final private JedisPool pool;
    final private S3Observable s3stream;

    @Inject
    public FixmeCreateUser(JedisPool pool, S3Observable s3stream) {
        this.pool = pool;
        this.s3stream = s3stream;
    }

    private Observable<Function<String, Observable<String>>> generator(String hashID, String[] pathSplited) {
        System.out.println(String.join(",", pathSplited));
        Observable<TreeNodes> stream;
        if (hashID == null)  {
            stream = Observable.just(new TreeNodes());
        } else {
            stream = s3stream.read(hashID, TreeNodes.class);
        }
        return stream.flatMap(treeNodes -> {
            TreeNode treeNode = treeNodes.treeNodeMap.get(pathSplited[0]);
            String nextHashID = null;
            if (treeNode != null) {
                if (pathSplited.length > 1 && treeNode.type == TreeNode.Type.File) {
                    return Observable.error(new IllegalArgumentException("path is wrong"));
                }
                if (pathSplited.length == 1 && treeNode.type == TreeNode.Type.Dir) {
                    return Observable.error(new IllegalArgumentException("path is wrong"));
                }
                nextHashID = treeNode.hashId;
            }
            TreeNode.Type type;
            Observable<Function<String, Observable<String>>> ret;
            if (pathSplited.length > 1) {
                type = TreeNode.Type.Dir;
                ret = generator(nextHashID, Arrays.copyOfRange(pathSplited, 1, pathSplited.length));
            } else {
                type = TreeNode.Type.File;
                ret = Observable.empty();
            }

            Function<String, Observable<String>> function = hashId -> {
                treeNodes.treeNodeMap.put(pathSplited[0], new TreeNode(hashId, type));
                return s3stream.write(treeNodes);
            };
            return ret.mergeWith(Observable.just(function));
        });
    }

    public void create(User user) {
        String path = user.id.substring(0, 2) + "/" + user.id;

        try (Jedis jedis = pool.getResource()) {
            Observable<Pair<String, String[]>> stream = Observable.just(Pair.of(jedis.get("user"), path.split("/")));

            Function<String, Observable<String>> blobWriteFunction = hashID -> s3stream.write(user);

            Observable<Function<String, Observable<String>>> writeStream = Observable.just(blobWriteFunction).mergeWith(stream
                    .concatMap(pair -> generator(pair.getLeft(), pair.getRight())));

            // FIXME: concatMap と reduce 合わせたみたいな方法があれば、x.apply を toBlocking しなくてすむのに…？
            writeStream.reduce((String)null, (hashID, x) -> x.apply(hashID).toBlocking().first()).subscribe(hashID -> {
                jedis.set("user", hashID);
            }, err -> {
                err.printStackTrace();
            });
        }
        pool.destroy();
    }

}
