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
import org.erukiti.pasco1.model.HashID;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rx.Observable;
import rx.subjects.ReplaySubject;

import java.util.*;

public class FixmeCreateDocument {
    final private JedisPool pool;
    final private ReadAndWriterGenerator readAndWriterGenerator;

    @Inject
    public FixmeCreateDocument(JedisPool pool, ReadAndWriterGenerator readAndWriterGenerator) {
        this.pool = pool;
        this.readAndWriterGenerator = readAndWriterGenerator;
    }

    public void createDocument(String team, String path, String text) {
        // Won(*3*)Chu FixMe!: path の正規化

        try (Jedis jedis = pool.getResource()) {
            LinkedList<HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>>> list = new LinkedList<>();

            list.add(readAndWriterGenerator.redisReadAndWriterGenerator(team, jedis));
            list.add(readAndWriterGenerator.bucketReadAndWriterGenerator(team));
            list.add(readAndWriterGenerator.dirReadAndWriterGenerator(team, path));
            list.add(readAndWriterGenerator.metaReadAndWriterGenerator(team));
            list.add(readAndWriterGenerator.blobReadAndWriterGenerator(team, text));

            Observable<HashIDChainFunction<ReplaySubject<HashID>>> writerStream = readAndWriterGenerator.readAndWriterGenerate(list);
            readAndWriterGenerator.write(writerStream).match(err -> {
                err.printStackTrace();
                return null;
            }, hashID -> {
                System.out.println("done: " + hashID.toString());
                return null;
            });
        }
        pool.destroy();
    }

}
