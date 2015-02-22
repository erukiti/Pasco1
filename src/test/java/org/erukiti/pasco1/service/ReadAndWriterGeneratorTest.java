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

import org.erukiti.pasco1.model.HashID;
import org.junit.Test;
import rx.subjects.ReplaySubject;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class ReadAndWriterGeneratorTest {
    private HashID hashID0 = new HashID("0000000000000000000000000000000000000000000000000000000000000000");
    private HashID hashID1 = new HashID("0000000000000000000000000000000000000000000000000000000000000001");
    private HashID hashID2 = new HashID("0000000000000000000000000000000000000000000000000000000000000002");
    private HashID hashID3 = new HashID("0000000000000000000000000000000000000000000000000000000000000003");
    private HashID hashID4 = new HashID("0000000000000000000000000000000000000000000000000000000000000004");
    private HashID hashID5 = new HashID("0000000000000000000000000000000000000000000000000000000000000005");
    private HashID hashID6 = new HashID("0000000000000000000000000000000000000000000000000000000000000006");

    // Won(*3*)Chu FixMe!: わかりやすい変数名にリライトもしくはエイリアスする

    private Throwable throwable0 = new Throwable("0");

    private HashIDChainFunction<ReplaySubject<HashID>> writeFunc1() {
        return (prevHashID, subject) -> {
            subject.onNext(hashID1);
            return hashID2;
        };
    }

    private HashIDChainFunction<ReplaySubject<HashID>> writeFunc2() {
        return (prevHashID, subject) -> {
            subject.onNext(hashID4);
            return hashID5;
        };
    }

    @Test
    public void readAndWriterGenerate() {
        LinkedList<HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>>> list = new LinkedList<>();
        list.add((prevHashID, subject) -> {
            assertNull(prevHashID);
            subject.onNext(writeFunc1());
            return hashID0;
        });
        list.add((prevHashID, subject) -> {
            assertEquals(prevHashID, hashID0);
            subject.onNext(writeFunc2());
            return hashID6;
        });
        ReadAndWriterGenerator readAndWriterGenerator = new ReadAndWriterGenerator(null, null);
        List<HashIDChainFunction<ReplaySubject<HashID>>> funcList = readAndWriterGenerator.readAndWriterGenerate(list).toList().toBlocking().first();
        assertEquals(funcList.size(), 2);
        ReplaySubject<HashID> sub = ReplaySubject.create();
        assertEquals(funcList.get(0).chain(hashID3, sub), hashID5);
        assertEquals(funcList.get(1).chain(hashID3, sub), hashID2);
        sub.onCompleted();
        assertEquals(sub.toList().toBlocking().first(), Arrays.asList(hashID4, hashID1));
    }

    @Test
    public void readAndWriterGenerateWithReadError() {
        LinkedList<HashIDChainFunction<ReplaySubject<HashIDChainFunction<ReplaySubject<HashID>>>>> list = new LinkedList<>();
        list.add((prevHashID, subject) -> {
            assertNull(prevHashID);
            subject.onNext(writeFunc1());
            return hashID0;
        });
        list.add((prevHashID, subject) -> {
            assertEquals(prevHashID, hashID0);
            subject.onError(throwable0);
            return hashID6;
        });
        ReadAndWriterGenerator readAndWriterGenerator = new ReadAndWriterGenerator(null, null);
        List<HashIDChainFunction<ReplaySubject<HashID>>> funcList = readAndWriterGenerator.readAndWriterGenerate(list).toList().toBlocking().first();
        assertEquals(funcList.size(), 2);
        ReplaySubject<HashID> sub = ReplaySubject.create();
        assertNull(funcList.get(0).chain(hashID3, sub));
        assertEquals(funcList.get(1).chain(hashID3, sub), hashID2);
        sub.onCompleted();
        sub.subscribe(hashID -> {
            assertFalse(true);
        }, err -> {
            assertEquals(err, throwable0);
        });
    }

}